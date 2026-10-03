use crate::config::Config;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::command_grants::CommandGrants;
use crate::mesh::keys::{self, RootSet};
use crate::mesh::row_sig::gateway_id_binds_key;
use crate::mesh::{gateway::notes_wire, mesh_root, scan_or_refuse, Note, NOTE_MAX_BYTES};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub const DIRECTIVE_KIND: &str = "directive";
pub(crate) const DIRECTIVE_SCHEMA_INVALID: &str = "DIRECTIVE_SCHEMA_INVALID";

const DIRECTIVE_REF: &str = "directive";
const PENDING_SUFFIX: &str = ".directive.json";
const COMMAND_AUDIT_FILE: &str = "command-audit.jsonl";
const DIRECTIVE_OUTCOME_REF: &str = "directive_outcome";
const DIRECTIVE_OUTCOME_PEER: &str = "user:mesh-command";
const DIRECTIVE_OUTCOME_NOTE_PREFIX: &str = "directive-outcome-";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DirectiveClass {
    Session,
    Harness,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directive {
    pub directive_id: String,
    pub from: String,
    pub target: String,
    pub ts: DateTime<Utc>,
    pub class: DirectiveClass,
    pub instruction: String,
    #[serde(default)]
    pub enroll_token: String,
    pub sig: String,
}

impl Directive {
    pub fn new(
        from: impl Into<String>,
        target: impl Into<String>,
        class: DirectiveClass,
        instruction: impl Into<String>,
        enroll_token: impl Into<String>,
    ) -> Self {
        let clock = keys::SystemClock;
        Self {
            directive_id: keys::mint_ulid(&clock),
            from: from.into(),
            target: target.into(),
            ts: Utc::now(),
            class,
            instruction: instruction.into(),
            enroll_token: enroll_token.into(),
            sig: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedReason {
    Ungranted,
    BadSignature,
    HarnessClass,
    Duplicate,
}

impl RefusedReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Ungranted => "DIRECTIVE_UNGRANTED",
            Self::BadSignature => "DIRECTIVE_BAD_SIGNATURE",
            Self::HarnessClass => "DIRECTIVE_HARNESS_CLASS",
            Self::Duplicate => "DIRECTIVE_DUPLICATE",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Imperative(Directive),
    Refused {
        directive_id: String,
        reason: RefusedReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutcomeVerdict {
    Acted,
    Failed,
    Refused,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveOutcome {
    pub directive_id: String,
    pub verdict: OutcomeVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl DirectiveOutcome {
    pub fn acted(directive_id: impl Into<String>) -> Self {
        Self {
            directive_id: directive_id.into(),
            verdict: OutcomeVerdict::Acted,
            reason: None,
            summary: Some("surfaced to the orchestrator".to_string()),
        }
    }

    pub fn refused(directive_id: impl Into<String>, reason: RefusedReason) -> Self {
        Self {
            directive_id: directive_id.into(),
            verdict: OutcomeVerdict::Refused,
            reason: Some(reason.code().to_string()),
            summary: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AuditVerdict {
    Imperative,
    Refused,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommandAuditEntry {
    ts: DateTime<Utc>,
    directive_id: String,
    from: String,
    verdict: AuditVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

pub fn evaluate(
    home: &A2aHome,
    grants: &CommandGrants,
    roots: &RootSet,
    self_id: &str,
    directives: Vec<Directive>,
) -> Result<Vec<Verdict>, AppError> {
    let mut acted = acted_directive_ids(home)?;
    let mut verdicts = Vec::with_capacity(directives.len());

    for directive in directives {
        let directive_id = directive.directive_id.clone();
        let from = directive.from.clone();
        let verdict = if acted.contains(&directive_id) {
            Verdict::Refused {
                directive_id: directive_id.clone(),
                reason: RefusedReason::Duplicate,
            }
        } else if verify(&directive, roots).is_err() {
            Verdict::Refused {
                directive_id: directive_id.clone(),
                reason: RefusedReason::BadSignature,
            }
        } else if !grants.is_granted(&from, self_id) {
            Verdict::Refused {
                directive_id: directive_id.clone(),
                reason: RefusedReason::Ungranted,
            }
        } else if directive.class != DirectiveClass::Session {
            Verdict::Refused {
                directive_id: directive_id.clone(),
                reason: RefusedReason::HarnessClass,
            }
        } else {
            Verdict::Imperative(directive)
        };

        append_verdict(home, &directive_id, &from, &verdict)?;
        if matches!(&verdict, Verdict::Imperative(_)) {
            acted.insert(directive_id);
        }
        verdicts.push(verdict);
    }

    Ok(verdicts)
}

fn acted_directive_ids(home: &A2aHome) -> Result<BTreeSet<String>, AppError> {
    let text = match fs::read_to_string(command_audit_path(home)) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeSet::new());
        }
        Err(error) => return Err(io_err("reading command audit", error)),
    };
    let mut acted = BTreeSet::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: CommandAuditEntry = serde_json::from_str(line).map_err(|error| {
            AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("command audit line {} is invalid: {error}", index + 1),
            )
            .remedy("repair or remove the invalid line in mesh/command-audit.jsonl")
        })?;
        if entry.verdict == AuditVerdict::Imperative {
            acted.insert(entry.directive_id);
        }
    }
    Ok(acted)
}

fn append_verdict(
    home: &A2aHome,
    directive_id: &str,
    from: &str,
    verdict: &Verdict,
) -> Result<(), AppError> {
    let (audit_verdict, reason) = match verdict {
        Verdict::Imperative(_) => (AuditVerdict::Imperative, None),
        Verdict::Refused { reason, .. } => {
            (AuditVerdict::Refused, Some(reason.code().to_string()))
        }
    };
    let entry = CommandAuditEntry {
        ts: Utc::now(),
        directive_id: directive_id.to_string(),
        from: from.to_string(),
        verdict: audit_verdict,
        reason,
    };
    let path = command_audit_path(home);
    let parent = path.parent().expect("command audit path has a parent");
    create_dir_0700(parent).map_err(|error| io_err("creating command audit directory", error))?;
    let mut line = serde_json::to_vec(&entry)
        .map_err(|error| data_err("serializing command audit entry", error))?;
    line.push(b'\n');
    let mut file = crate::exec::open_append_0600(&path)
        .map_err(|error| io_err("opening command audit", error))?;
    file.write_all(&line)
        .map_err(|error| io_err("appending command audit", error))?;
    file.sync_data()
        .map_err(|error| io_err("syncing command audit", error))?;
    Ok(())
}

fn command_audit_path(home: &A2aHome) -> PathBuf {
    home.root().join("mesh").join(COMMAND_AUDIT_FILE)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingStore {
    Stored,
    Duplicate,
}

pub fn spool(
    home: &A2aHome,
    cfg: &Config,
    directive: &Directive,
) -> Result<PathBuf, AppError> {
    validate_shape(directive)?;
    let local_gateway = keys::gateway_id(home)?;
    if directive.from != local_gateway {
        return Err(schema_invalid(format!(
            "directive sender {} does not match local gateway {local_gateway}",
            directive.from
        )));
    }
    let note = Note {
        note_id: Some(directive.directive_id.clone()),
        from: directive.from.clone(),
        to: gateway_address(&directive.target),
        ts: directive.ts,
        kind: DIRECTIVE_KIND.to_string(),
        text: directive.instruction.clone(),
        refs: json!({ "directive": directive }),
        extra: serde_json::Map::new(),
    };
    notes_wire::spool_note(
        home,
        cfg,
        &directive.from,
        &directive.target,
        note,
    )
}

pub fn outcome_note_body(outcome: &DirectiveOutcome) -> Result<String, AppError> {
    serde_json::to_string(outcome)
        .map_err(|error| data_err("serializing directive outcome", error))
}

pub fn post_outcome(
    home: &A2aHome,
    cfg: &Config,
    to_gateway: &str,
    outcome: &DirectiveOutcome,
) -> Result<PathBuf, AppError> {
    validate_gateway_id("outcome recipient", to_gateway)?;
    ulid::Ulid::from_string(&outcome.directive_id)
        .map_err(|_| schema_invalid("outcome directive_id must be a ULID"))?;
    let note_id = format!("{DIRECTIVE_OUTCOME_NOTE_PREFIX}{}", outcome.directive_id);
    if let Some(path) = existing_outcome_note(home, to_gateway, &note_id)? {
        return Ok(path);
    }

    let text = outcome_note_body(outcome)?;
    scan_or_refuse(&text, "SECRET_IN_NOTE")?;
    if text.len() > NOTE_MAX_BYTES {
        return Err(AppError::new(
            "NOTE_TOO_LARGE",
            ExitCode::Precondition,
            format!(
                "directive outcome is {} bytes, exceeding cap {NOTE_MAX_BYTES} bytes",
                text.len()
            ),
        ));
    }
    let note = Note {
        note_id: Some(note_id),
        from: DIRECTIVE_OUTCOME_PEER.to_string(),
        to: DIRECTIVE_OUTCOME_PEER.to_string(),
        ts: Utc::now(),
        kind: "note".to_string(),
        text,
        refs: json!({ (DIRECTIVE_OUTCOME_REF): outcome }),
        extra: serde_json::Map::new(),
    };
    let from_gateway = keys::gateway_id(home)?;
    notes_wire::spool_note(home, cfg, &from_gateway, to_gateway, note)
}

fn existing_outcome_note(
    home: &A2aHome,
    to_gateway: &str,
    note_id: &str,
) -> Result<Option<PathBuf>, AppError> {
    let outbound = mesh_root(home).join("outbound");
    for dir in [
        outbound.join(to_gateway),
        outbound.join("sent").join(to_gateway),
        outbound.join("rejected").join(to_gateway),
    ] {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_err("reading directive outcome spool", error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| io_err("reading directive outcome spool", error))?;
            if !entry
                .file_type()
                .map_err(|error| io_err("reading directive outcome spool", error))?
                .is_file()
                || entry.path().extension().and_then(|ext| ext.to_str()) != Some("json")
            {
                continue;
            }
            let spooled = notes_wire::read_spooled(&entry.path())?;
            if spooled.trace_id == note_id {
                return Ok(Some(entry.path()));
            }
        }
    }
    Ok(None)
}

pub fn pending(home: &A2aHome) -> Result<Vec<Directive>, AppError> {
    let entries = match fs::read_dir(pending_dir(home)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_err("reading pending directives", error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| io_err("reading pending directives", error))?;
        if !entry
            .file_type()
            .map_err(|error| io_err("reading pending directives", error))?
            .is_file()
        {
            continue;
        }
        if entry.file_name().to_string_lossy().ends_with(PENDING_SUFFIX) {
            paths.push(entry.path());
        }
    }
    paths.sort();

    let local_gateway = keys::gateway_id(home)?;
    let mut directives = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(&path).map_err(|error| io_err("reading pending directive", error))?;
        let directive: Directive = serde_json::from_slice(&bytes)
            .map_err(|error| data_err("decoding pending directive", error))?;
        validate_shape(&directive)?;
        if directive.target != local_gateway {
            return Err(schema_invalid(format!(
                "pending directive {} targets {}, not local gateway {local_gateway}",
                directive.directive_id, directive.target
            )));
        }
        directives.push(directive);
    }
    Ok(directives)
}

pub(crate) fn decode_transport_note(
    home: &A2aHome,
    from_gateway: &str,
    to_gateway: &str,
    to_peer: &str,
    note: &Note,
) -> Result<Directive, AppError> {
    if note.kind != DIRECTIVE_KIND {
        return Err(schema_invalid("transport note is not a directive"));
    }
    let raw = note
        .refs
        .get(DIRECTIVE_REF)
        .cloned()
        .ok_or_else(|| schema_invalid("directive transport is missing refs.directive"))?;
    let directive: Directive = serde_json::from_value(raw)
        .map_err(|error| schema_invalid(format!("decoding refs.directive: {error}")))?;
    validate_shape(&directive)?;

    let expected_peer = gateway_address(&directive.target);
    let local_gateway = keys::gateway_id(home)?;
    let valid = note.note_id.as_deref() == Some(directive.directive_id.as_str())
        && note.from == directive.from
        && note.to == expected_peer
        && note.ts == directive.ts
        && note.text == directive.instruction
        && from_gateway == directive.from
        && to_gateway == directive.target
        && to_peer == expected_peer
        && directive.target == local_gateway;
    if !valid {
        return Err(schema_invalid(
            "directive transport metadata does not match the signed payload or local gateway",
        ));
    }
    Ok(directive)
}

pub(crate) fn store_pending(
    home: &A2aHome,
    directive: &Directive,
) -> Result<PendingStore, AppError> {
    validate_shape(directive)?;
    let dir = pending_dir(home);
    create_dir_0700(&dir).map_err(|error| io_err("creating pending directive dir", error))?;
    let path = dir.join(format!("{}{}", directive.directive_id, PENDING_SUFFIX));
    let bytes = serde_json::to_vec_pretty(directive)
        .map_err(|error| data_err("serializing pending directive", error))?;
    let mut file = match write_new_0600(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(PendingStore::Duplicate);
        }
        Err(error) => return Err(io_err("writing pending directive", error)),
    };
    file.write_all(&bytes)
        .map_err(|error| io_err("writing pending directive", error))?;
    file.write_all(b"\n")
        .map_err(|error| io_err("writing pending directive", error))?;
    file.sync_all()
        .map_err(|error| io_err("syncing pending directive", error))?;
    Ok(PendingStore::Stored)
}

fn validate_shape(directive: &Directive) -> Result<(), AppError> {
    ulid::Ulid::from_string(&directive.directive_id)
        .map_err(|_| schema_invalid("directive_id must be a ULID"))?;
    validate_gateway_id("from", &directive.from)?;
    validate_gateway_id("target", &directive.target)?;
    let signature = keys::b64_decode(&directive.sig)
        .map_err(|_| schema_invalid("directive signature is not base64"))?;
    if signature.len() != 64 {
        return Err(schema_invalid("directive signature has invalid length"));
    }
    Ok(())
}

fn validate_gateway_id(field: &str, gateway_id: &str) -> Result<(), AppError> {
    let valid = !gateway_id.is_empty()
        && gateway_id.len() <= 64
        && gateway_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(schema_invalid(format!(
            "directive {field} must match ^[A-Za-z0-9_-]{{1,64}}$"
        )))
    }
}

fn gateway_address(gateway_id: &str) -> String {
    format!("gateway:{gateway_id}")
}

fn pending_dir(home: &A2aHome) -> PathBuf {
    mesh_root(home).join("directives").join("pending")
}

fn schema_invalid(message: impl Into<String>) -> AppError {
    AppError::new(DIRECTIVE_SCHEMA_INVALID, ExitCode::Precondition, message)
}

fn io_err(context: &'static str, error: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {error}"))
}

fn data_err(context: &'static str, error: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {error}"))
}

pub fn canonical_bytes(directive: &Directive) -> Result<String, AppError> {
    let enroll_token_sha256 = sha256_hex(directive.enroll_token.as_bytes());
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("class", json!(&directive.class)),
        ("directive_id", json!(&directive.directive_id)),
        ("enroll_token_sha256", json!(enroll_token_sha256)),
        ("from", json!(&directive.from)),
        ("instruction", json!(&directive.instruction)),
        ("target", json!(&directive.target)),
        ("ts", json!(&directive.ts)),
    ]);
    String::from_utf8(keys::canonical_json_fields(fields)?).map_err(|error| {
        AppError::new(
            "SERIALIZE",
            ExitCode::Internal,
            format!("canonical directive was not UTF-8: {error}"),
        )
    })
}

pub fn sign(directive: &mut Directive, key: &SigningKey) -> Result<(), AppError> {
    if directive.enroll_token.is_empty() {
        return Err(signature_invalid("directive enrollment token is missing"));
    }
    let canonical = canonical_bytes(directive)?;
    let signature = key.sign(canonical.as_bytes());
    directive.sig = keys::b64_encode(&signature.to_bytes());
    Ok(())
}

pub fn verify(directive: &Directive, roots: &RootSet) -> Result<String, AppError> {
    let (claims, issuer) = keys::verify_token_no_clock(&directive.enroll_token, roots)
        .map_err(|_| signature_invalid("directive enrollment token is invalid"))?;
    let issuer_fleet_matches = roots.entries().iter().any(|root| {
        root.fpr == issuer && root.fleet_id == claims.fleet_id
    });
    if !issuer_fleet_matches {
        return Err(signature_invalid(
            "directive enrollment token fleet does not match its trusted issuer",
        ));
    }
    if claims.gateway_id != directive.from {
        return Err(signature_invalid(
            "directive sender does not match its enrollment token",
        ));
    }
    if !gateway_id_binds_key(&directive.from, &claims) {
        return Err(signature_invalid(
            "directive sender is not bound to its enrolled gateway key",
        ));
    }
    let key = keys::verifying_key_from_b64(&claims.pubkey_b64)
        .map_err(|_| signature_invalid("enrolled directive public key is invalid"))?;
    let signature_bytes = keys::b64_decode(&directive.sig)
        .map_err(|_| signature_invalid("directive signature is not base64"))?;
    let signature_bytes: [u8; 64] = signature_bytes
        .try_into()
        .map_err(|_| signature_invalid("directive signature has invalid length"))?;
    let signature = Signature::from_bytes(&signature_bytes);
    let canonical = canonical_bytes(directive)?;
    key.verify(canonical.as_bytes(), &signature)
        .map_err(|_| {
            signature_invalid("directive signature does not verify with the enrolled gateway key")
        })?;
    Ok(directive.from.clone())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn signature_invalid(message: &'static str) -> AppError {
    AppError::new(
        "DIRECTIVE_SIGNATURE_INVALID",
        ExitCode::Precondition,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::A2aHome;
    use crate::mesh::command_grants::CommandGrants;
    use crate::mesh::gateway::notes_wire::{
        self, deliver_spooled_once, NoteAck, NoteDeliveryTarget,
    };
    use crate::mesh::keys::{
        self, enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
        install_gateway_token, RootSet, SystemClock, TestClock,
    };
    use crate::mesh::{notes, PeerId};
    use chrono::{TimeZone, Utc};
    use ed25519_dalek::SigningKey;
    use rand::{rngs::StdRng, SeedableRng};
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    struct EnrolledSigner {
        _tmp: tempfile::TempDir,
        root: A2aHome,
        sender: A2aHome,
        key: SigningKey,
        roots: RootSet,
        from: String,
        token: String,
    }

    fn enrolled_peer() -> EnrolledSigner {
        let tmp = tempfile::tempdir().unwrap();
        let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
        let sender = A2aHome::resolve(Some(&tmp.path().join("sender"))).unwrap();
        fleet_init(&root).unwrap();
        copy_root_pub(&root, &sender);
        let from = enroll_joiner(&root, &sender);
        let key = keys::gateway_signing_key(&sender).unwrap();
        let token = keys::gateway_token(&sender).unwrap();
        let roots = RootSet::load(&sender).unwrap();
        assert_ne!(
            gateway_public_key_b64(&sender).unwrap().as_str(),
            roots.entries()[0].pubkey_b64.as_str(),
            "the enrolled sender's gateway key must not be the fleet root key"
        );
        EnrolledSigner {
            _tmp: tmp,
            root,
            sender,
            key,
            roots,
            from,
            token,
        }
    }

    fn fixture(from: String, enroll_token: String) -> Directive {
        let ts = Utc
            .with_ymd_and_hms(2026, 9, 25, 16, 21, 0)
            .single()
            .unwrap();
        Directive {
            directive_id: keys::mint_ulid(&TestClock::new(ts)),
            from,
            target: "gw-target-0123456789abcdef".to_string(),
            ts,
            class: DirectiveClass::Session,
            instruction: "start a brainstorm on signed directives".to_string(),
            enroll_token,
            sig: String::new(),
        }
    }

    fn copy_root_pub(root: &A2aHome, joiner: &A2aHome) {
        let fleet_dir = joiner.root().join("mesh/fleet");
        fs::create_dir_all(&fleet_dir).unwrap();
        fs::copy(
            root.root().join("mesh/fleet/root.pub"),
            fleet_dir.join("root.pub"),
        )
        .unwrap();
    }

    fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) -> String {
        let identity = gateway_identity(joiner, false).unwrap();
        let token = enroll_gateway_token(
            root,
            &identity.gateway_id,
            &gateway_public_key_b64(joiner).unwrap(),
            &SystemClock,
        )
        .unwrap();
        install_gateway_token(joiner, &token, &SystemClock).unwrap();
        identity.gateway_id
    }

    fn two_home_fleet() -> (tempfile::TempDir, A2aHome, A2aHome, String, String) {
        let tmp = tempfile::tempdir().unwrap();
        let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
        let sender = A2aHome::resolve(Some(&tmp.path().join("sender"))).unwrap();
        let receiver = A2aHome::resolve(Some(&tmp.path().join("receiver"))).unwrap();
        fleet_init(&root).unwrap();
        copy_root_pub(&root, &sender);
        copy_root_pub(&root, &receiver);
        let sender_gateway = enroll_joiner(&root, &sender);
        let receiver_gateway = enroll_joiner(&root, &receiver);
        (
            tmp,
            sender,
            receiver,
            sender_gateway,
            receiver_gateway,
        )
    }

    fn two_home_command_fleet() -> (tempfile::TempDir, A2aHome, A2aHome, String, String) {
        let fleet = two_home_fleet();
        let roots = RootSet::load(&fleet.1).unwrap();
        assert_ne!(
            gateway_public_key_b64(&fleet.1).unwrap().as_str(),
            roots.entries()[0].pubkey_b64.as_str(),
            "the command sender must be an enrolled peer, not the root key holder"
        );
        fleet
    }

    fn signed_evaluation_fixture(
        class: DirectiveClass,
    ) -> (
        tempfile::TempDir,
        A2aHome,
        RootSet,
        String,
        String,
        Directive,
    ) {
        let enrolled = enrolled_peer();
        let mut directive = fixture(enrolled.from.clone(), enrolled.token.clone());
        directive.class = class;
        sign(&mut directive, &enrolled.key).unwrap();
        let self_id = directive.target.clone();
        let EnrolledSigner {
            _tmp,
            sender,
            roots,
            from,
            ..
        } = enrolled;
        (_tmp, sender, roots, from, self_id, directive)
    }

    fn granted_to(from: &str) -> CommandGrants {
        let mut grants = CommandGrants::default();
        grants.grant(from);
        grants
    }

    fn audit_rows(home: &A2aHome) -> Vec<Value> {
        fs::read_to_string(home.root().join("mesh/command-audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn assert_refused_only(verdicts: &[Verdict]) {
        assert!(
            verdicts
                .iter()
                .all(|verdict| !matches!(verdict, Verdict::Imperative(_))),
            "a directive that fails a gate must never become imperative"
        );
    }

    fn assert_audit_row(
        row: &Value,
        directive: &Directive,
        verdict: &str,
        reason: Option<RefusedReason>,
    ) {
        assert!(row["ts"].as_str().is_some_and(|ts| !ts.is_empty()));
        assert_eq!(row["directive_id"], directive.directive_id);
        assert_eq!(row["from"], directive.from);
        assert_eq!(row["verdict"], verdict);
        match reason {
            Some(reason) => assert_eq!(row["reason"], reason.code()),
            None => assert!(row.get("reason").is_none()),
        }
    }

    fn route_directive_and_outcome(
        granted: bool,
    ) -> (Directive, Option<String>, Note, DirectiveOutcome, String) {
        let (_tmp, sender, receiver, sender_gateway, receiver_gateway) =
            two_home_command_fleet();
        let sender_cfg = crate::config::load(&sender).unwrap().0;
        let receiver_cfg = crate::config::load(&receiver).unwrap().0;
        let mut directive = Directive::new(
            sender_gateway.clone(),
            receiver_gateway.clone(),
            DirectiveClass::Session,
            "start the routed outcome test",
            keys::gateway_token(&sender).unwrap(),
        );
        sign(
            &mut directive,
            &keys::gateway_signing_key(&sender).unwrap(),
        )
        .unwrap();
        spool(&sender, &sender_cfg, &directive).unwrap();

        let mut rng = StdRng::seed_from_u64(19);
        let delivered = deliver_spooled_once(
            &sender_cfg,
            &sender,
            NoteDeliveryTarget::Gateway {
                to_gateway: &receiver_gateway,
                home: Some(&receiver),
            },
            Utc::now(),
            &mut rng,
        )
        .unwrap();
        assert_eq!(delivered.delivered, 1);

        let mut grants = CommandGrants::default();
        if granted {
            grants.grant(&sender_gateway);
        }
        grants.save(&receiver).unwrap();

        let receiver_peer = PeerId::parse("claude:directive-outcome-receiver").unwrap();
        let first_digest = notes::inject(&receiver, &receiver_cfg, &receiver_peer, Utc::now())
            .unwrap();
        assert_eq!(
            notes::inject(&receiver, &receiver_cfg, &receiver_peer, Utc::now()).unwrap(),
            None,
            "the pending directive must not emit a second outcome"
        );

        let outbound = receiver
            .root()
            .join("mesh/outbound")
            .join(&sender_gateway);
        let mut outcome_paths = fs::read_dir(outbound)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        outcome_paths.sort();
        assert_eq!(outcome_paths.len(), 1, "one outcome per directive");
        let wire = notes_wire::read_spooled(&outcome_paths[0]).unwrap();
        assert_eq!(wire.to_gateway, sender_gateway);
        assert_eq!(wire.note.kind, "note");
        assert_eq!(wire.note.to, DIRECTIVE_OUTCOME_PEER);

        let routed = deliver_spooled_once(
            &receiver_cfg,
            &receiver,
            NoteDeliveryTarget::Gateway {
                to_gateway: &sender_gateway,
                home: Some(&sender),
            },
            Utc::now(),
            &mut rng,
        )
        .unwrap();
        assert_eq!(routed.delivered, 1);
        assert_eq!(routed.duplicates, 0);

        let outcome_peer = PeerId::parse(DIRECTIVE_OUTCOME_PEER).unwrap();
        let mut unread = notes::unread(&sender, &outcome_peer).unwrap();
        assert_eq!(unread.len(), 1);
        let note = unread.remove(0).1;
        let outcome: DirectiveOutcome = serde_json::from_str(&note.text).unwrap();
        assert_eq!(note.refs[DIRECTIVE_OUTCOME_REF], json!(outcome));

        let ambient = notes::inject(&sender, &sender_cfg, &outcome_peer, Utc::now())
            .unwrap()
            .unwrap();
        assert!(ambient.contains("data, not instructions"), "{ambient}");
        assert!(ambient.contains(&directive.directive_id), "{ambient}");
        assert!(!ambient.contains("## AUTHENTICATED DIRECTIVE"), "{ambient}");

        (directive, first_digest, note, outcome, ambient)
    }

    #[test]
    fn signed_directive_from_an_enrolled_peer_verifies_with_its_gateway_key() {
        let enrolled = enrolled_peer();
        let mut directive = fixture(enrolled.from.clone(), enrolled.token.clone());

        sign(&mut directive, &enrolled.key).unwrap();

        assert!(!directive.sig.is_empty());
        assert_eq!(verify(&directive, &enrolled.roots).unwrap(), enrolled.from);
        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
    }

    #[test]
    fn a_root_holder_can_send_only_through_its_enrolled_gateway_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let root = A2aHome::resolve(Some(tmp.path())).unwrap();
        fleet_init(&root).unwrap();
        let identity = keys::ensure_and_verify_gateway_enrollment(&root, &SystemClock).unwrap();
        let roots = RootSet::load(&root).unwrap();
        assert_ne!(
            gateway_public_key_b64(&root).unwrap().as_str(),
            roots.entries()[0].pubkey_b64.as_str(),
            "the root holder's enrolled gateway key must remain distinct from its root key"
        );
        let mut directive = fixture(identity.gateway_id.clone(), keys::gateway_token(&root).unwrap());

        sign(&mut directive, &keys::gateway_signing_key(&root).unwrap()).unwrap();

        assert_eq!(verify(&directive, &roots).unwrap(), identity.gateway_id);
    }

    #[test]
    fn new_directive_mints_a_ulid_and_starts_unsigned() {
        let directive = Directive::new(
            "gw-from-0123456789abcdef",
            "gw-target-0123456789abcdef",
            DirectiveClass::Session,
            "start the requested work",
            "enrollment-token",
        );

        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
        assert_eq!(directive.enroll_token, "enrollment-token");
        assert!(directive.sig.is_empty());
    }

    #[test]
    fn tampered_instruction_is_rejected() {
        let enrolled = enrolled_peer();
        let mut directive = fixture(enrolled.from.clone(), enrolled.token.clone());
        sign(&mut directive, &enrolled.key).unwrap();

        directive.instruction.push('!');

        assert!(verify(&directive, &enrolled.roots).is_err());
    }

    #[test]
    fn tampered_sender_is_rejected() {
        let enrolled = enrolled_peer();
        let mut directive = fixture(enrolled.from.clone(), enrolled.token.clone());
        sign(&mut directive, &enrolled.key).unwrap();

        directive.from.push_str("-tampered");

        assert!(verify(&directive, &enrolled.roots).is_err());
    }

    #[test]
    fn a_valid_signature_from_an_unbound_key_is_rejected() {
        let enrolled = enrolled_peer();
        let mut directive = fixture(
            "gw-directive-test-deadbeef".to_string(),
            enrolled.token.clone(),
        );

        sign(&mut directive, &enrolled.key).unwrap();

        assert!(verify(&directive, &enrolled.roots).is_err());
    }

    #[test]
    fn swapping_a_valid_enrollment_token_after_signing_is_rejected() {
        let enrolled = enrolled_peer();
        let mut directive = fixture(enrolled.from.clone(), enrolled.token.clone());
        sign(&mut directive, &enrolled.key).unwrap();
        let replacement_clock = TestClock::new(
            Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0)
                .single()
                .unwrap(),
        );
        let replacement = enroll_gateway_token(
            &enrolled.root,
            &enrolled.from,
            &gateway_public_key_b64(&enrolled.sender).unwrap(),
            &replacement_clock,
        )
        .unwrap();
        assert_ne!(replacement, directive.enroll_token);

        directive.enroll_token = replacement;

        assert!(verify(&directive, &enrolled.roots).is_err());
    }

    #[test]
    fn canonical_bytes_are_stable_across_struct_field_order() {
        let enrolled = enrolled_peer();
        let first = fixture(enrolled.from.clone(), enrolled.token.clone());
        let second = Directive {
            sig: "ignored-signature".to_string(),
            enroll_token: first.enroll_token.clone(),
            instruction: first.instruction.clone(),
            class: DirectiveClass::Session,
            ts: first.ts,
            target: first.target.clone(),
            from: enrolled.from,
            directive_id: first.directive_id.clone(),
        };

        let canonical = canonical_bytes(&first).unwrap();
        assert_eq!(canonical, canonical_bytes(&second).unwrap());
        assert!(canonical.contains(&sha256_hex(first.enroll_token.as_bytes())));
        assert!(!canonical.contains(&first.enroll_token));
    }

    #[test]
    fn unknown_directive_classes_deserialize_forward_compatibly() {
        assert_eq!(
            serde_json::from_str::<DirectiveClass>("\"future\"").unwrap(),
            DirectiveClass::Unknown
        );
    }

    #[test]
    fn directive_routes_over_the_note_spool_and_redelivery_is_idempotent() {
        let (_tmp, sender, receiver, sender_gateway, receiver_gateway) = two_home_fleet();
        let cfg = crate::config::load(&sender).unwrap().0;
        let mut directive = Directive::new(
            sender_gateway,
            receiver_gateway.clone(),
            DirectiveClass::Session,
            "start the routed work",
            keys::gateway_token(&sender).unwrap(),
        );
        sign(
            &mut directive,
            &keys::gateway_signing_key(&sender).unwrap(),
        )
        .unwrap();

        let spool_path = spool(&sender, &cfg, &directive).unwrap();
        let wire = notes_wire::read_spooled(&spool_path).unwrap();
        assert_eq!(wire.note.kind, DIRECTIVE_KIND);
        assert_eq!(
            wire.note.note_id.as_deref(),
            Some(directive.directive_id.as_str())
        );

        let mut rng = StdRng::seed_from_u64(3);
        let report = deliver_spooled_once(
            &cfg,
            &sender,
            NoteDeliveryTarget::Gateway {
                to_gateway: &receiver_gateway,
                home: Some(&receiver),
            },
            Utc::now(),
            &mut rng,
        )
        .unwrap();
        assert_eq!(report.delivered, 1);
        assert_eq!(report.duplicates, 0);

        let first = pending(&receiver).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].directive_id, directive.directive_id);

        assert_eq!(
            notes_wire::receive_note(&receiver, &cfg, &wire).unwrap(),
            NoteAck::Duplicate
        );
        let after_redelivery = pending(&receiver).unwrap();
        assert_eq!(after_redelivery.len(), 1);
        assert_eq!(after_redelivery[0].directive_id, directive.directive_id);
    }

    #[test]
    fn granted_verified_session_directive_is_imperative_and_audited() {
        let (_tmp, home, roots, from, self_id, directive) =
            signed_evaluation_fixture(DirectiveClass::Session);
        let grants = granted_to(&from);

        let verdicts = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(verdicts, vec![Verdict::Imperative(directive.clone())]);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 1);
        assert_audit_row(&rows[0], &directive, "imperative", None);
        let mode = fs::metadata(home.root().join("mesh/command-audit.jsonl"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn ungranted_directive_is_refused_and_audited() {
        let (_tmp, home, roots, _from, self_id, directive) =
            signed_evaluation_fixture(DirectiveClass::Session);

        let verdicts = evaluate(
            &home,
            &CommandGrants::default(),
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(
            verdicts,
            vec![Verdict::Refused {
                directive_id: directive.directive_id.clone(),
                reason: RefusedReason::Ungranted,
            }]
        );
        assert_refused_only(&verdicts);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 1);
        assert_audit_row(
            &rows[0],
            &directive,
            "refused",
            Some(RefusedReason::Ungranted),
        );
    }

    #[test]
    fn tampered_signature_directive_is_refused_and_audited() {
        let (_tmp, home, roots, from, self_id, mut directive) =
            signed_evaluation_fixture(DirectiveClass::Session);
        let grants = granted_to(&from);
        directive.instruction.push('!');

        let verdicts = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(
            verdicts,
            vec![Verdict::Refused {
                directive_id: directive.directive_id.clone(),
                reason: RefusedReason::BadSignature,
            }]
        );
        assert_refused_only(&verdicts);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 1);
        assert_audit_row(
            &rows[0],
            &directive,
            "refused",
            Some(RefusedReason::BadSignature),
        );
    }

    #[test]
    fn harness_directive_is_refused_and_audited() {
        let (_tmp, home, roots, from, self_id, directive) =
            signed_evaluation_fixture(DirectiveClass::Harness);
        let grants = granted_to(&from);

        let verdicts = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(
            verdicts,
            vec![Verdict::Refused {
                directive_id: directive.directive_id.clone(),
                reason: RefusedReason::HarnessClass,
            }]
        );
        assert_refused_only(&verdicts);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 1);
        assert_audit_row(
            &rows[0],
            &directive,
            "refused",
            Some(RefusedReason::HarnessClass),
        );
    }

    #[test]
    fn unknown_class_directive_is_refused_and_audited() {
        let (_tmp, home, roots, from, self_id, directive) =
            signed_evaluation_fixture(DirectiveClass::Unknown);
        let grants = granted_to(&from);

        let verdicts = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(
            verdicts,
            vec![Verdict::Refused {
                directive_id: directive.directive_id.clone(),
                reason: RefusedReason::HarnessClass,
            }]
        );
        assert_refused_only(&verdicts);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 1);
        assert_audit_row(
            &rows[0],
            &directive,
            "refused",
            Some(RefusedReason::HarnessClass),
        );
    }

    #[test]
    fn already_imperative_directive_is_duplicate_and_audited() {
        let (_tmp, home, roots, from, self_id, directive) =
            signed_evaluation_fixture(DirectiveClass::Session);
        let grants = granted_to(&from);

        let first = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();
        let second = evaluate(
            &home,
            &grants,
            &roots,
            &self_id,
            vec![directive.clone()],
        )
        .unwrap();

        assert_eq!(first, vec![Verdict::Imperative(directive.clone())]);
        assert_eq!(
            second,
            vec![Verdict::Refused {
                directive_id: directive.directive_id.clone(),
                reason: RefusedReason::Duplicate,
            }]
        );
        assert_refused_only(&second);
        let rows = audit_rows(&home);
        assert_eq!(rows.len(), 2);
        assert_audit_row(&rows[0], &directive, "imperative", None);
        assert_audit_row(
            &rows[1],
            &directive,
            "refused",
            Some(RefusedReason::Duplicate),
        );
    }

    #[test]
    fn surfaced_directive_routes_one_acted_outcome_back_to_sender() {
        let (directive, digest, note, outcome, ambient) = route_directive_and_outcome(true);

        assert!(
            digest
                .as_deref()
                .is_some_and(|digest| digest.contains("## AUTHENTICATED DIRECTIVE")),
            "digest={digest:?}, outcome={outcome:?}"
        );
        assert_eq!(note.from, DIRECTIVE_OUTCOME_PEER);
        assert_eq!(outcome.directive_id, directive.directive_id);
        assert_eq!(outcome.verdict, OutcomeVerdict::Acted);
        assert_eq!(outcome.reason, None);
        assert_eq!(
            outcome.summary.as_deref(),
            Some("surfaced to the orchestrator")
        );
        assert!(ambient.contains("\"verdict\":\"acted\""), "{ambient}");
    }

    #[test]
    fn ungranted_directive_routes_one_refused_outcome_back_to_sender() {
        let (directive, digest, note, outcome, ambient) = route_directive_and_outcome(false);

        assert_eq!(digest, None);
        assert_eq!(note.from, DIRECTIVE_OUTCOME_PEER);
        assert_eq!(outcome.directive_id, directive.directive_id);
        assert_eq!(outcome.verdict, OutcomeVerdict::Refused);
        assert_eq!(outcome.reason.as_deref(), Some("DIRECTIVE_UNGRANTED"));
        assert_eq!(outcome.summary, None);
        assert!(ambient.contains("\"verdict\":\"refused\""), "{ambient}");
    }

    #[test]
    fn outcome_body_supports_failed_and_omits_absent_optional_fields() {
        let outcome = DirectiveOutcome {
            directive_id: "01K60G9DB0WQQ7Y9AT0Y1VJB4Q".to_string(),
            verdict: OutcomeVerdict::Failed,
            reason: None,
            summary: None,
        };

        let body: Value = serde_json::from_str(&outcome_note_body(&outcome).unwrap()).unwrap();

        assert_eq!(body["directive_id"], outcome.directive_id);
        assert_eq!(body["verdict"], "failed");
        assert!(body.get("reason").is_none());
        assert!(body.get("summary").is_none());
    }
}
