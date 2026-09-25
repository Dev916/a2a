use crate::config::Config;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::command_grants::CommandGrants;
use crate::mesh::keys::{self, RootSet, TokenClaims};
use crate::mesh::row_sig::gateway_id_binds_key;
use crate::mesh::{gateway::notes_wire, mesh_root, Note};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub const DIRECTIVE_KIND: &str = "directive";
pub(crate) const DIRECTIVE_SCHEMA_INVALID: &str = "DIRECTIVE_SCHEMA_INVALID";

const DIRECTIVE_REF: &str = "directive";
const PENDING_SUFFIX: &str = ".directive.json";
const COMMAND_AUDIT_FILE: &str = "command-audit.jsonl";

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
    pub sig: String,
}

impl Directive {
    pub fn new(
        from: impl Into<String>,
        target: impl Into<String>,
        class: DirectiveClass,
        instruction: impl Into<String>,
    ) -> Self {
        let clock = keys::SystemClock;
        Self {
            directive_id: keys::mint_ulid(&clock),
            from: from.into(),
            target: target.into(),
            ts: Utc::now(),
            class,
            instruction: instruction.into(),
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
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("class", json!(&directive.class)),
        ("directive_id", json!(&directive.directive_id)),
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
    let canonical = canonical_bytes(directive)?;
    let signature = key.sign(canonical.as_bytes());
    directive.sig = keys::b64_encode(&signature.to_bytes());
    Ok(())
}

pub fn verify(directive: &Directive, roots: &RootSet) -> Result<String, AppError> {
    let signature_bytes = keys::b64_decode(&directive.sig)
        .map_err(|_| signature_invalid("directive signature is not base64"))?;
    let signature_bytes: [u8; 64] = signature_bytes
        .try_into()
        .map_err(|_| signature_invalid("directive signature has invalid length"))?;
    let signature = Signature::from_bytes(&signature_bytes);
    let canonical = canonical_bytes(directive)?;
    let mut sender_is_bound = false;

    for root in roots.entries() {
        let claims = TokenClaims {
            fleet_id: root.fleet_id.clone(),
            gateway_id: directive.from.clone(),
            pubkey_b64: root.pubkey_b64.clone(),
            key_epoch: 0,
            issued_at: 0,
            exp: 0,
        };
        if !gateway_id_binds_key(&directive.from, &claims) {
            continue;
        }
        sender_is_bound = true;
        let key = keys::verifying_key_from_b64(&claims.pubkey_b64)
            .map_err(|_| signature_invalid("trusted directive public key is invalid"))?;
        if key.verify(canonical.as_bytes(), &signature).is_ok() {
            return Ok(directive.from.clone());
        }
    }

    if sender_is_bound {
        Err(signature_invalid(
            "directive signature does not verify against the bound trusted key",
        ))
    } else {
        Err(signature_invalid(
            "directive sender is not bound to a trusted key",
        ))
    }
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
    use chrono::{TimeZone, Utc};
    use ed25519_dalek::SigningKey;
    use rand::{rngs::StdRng, SeedableRng};
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn trusted_key() -> (tempfile::TempDir, SigningKey, RootSet, String) {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let key = SigningKey::from_bytes(&[7_u8; 32]);
        let pubkey_b64 = keys::b64_encode(&key.verifying_key().to_bytes());
        let fingerprint = keys::fpr_of_pubkey_b64(&pubkey_b64).unwrap();
        let root_dir = home.root().join("mesh/fleet");
        fs::create_dir_all(&root_dir).unwrap();
        fs::write(
            root_dir.join("root.pub"),
            serde_json::to_vec(&json!({
                "fleet_id": "fleet-directive-test",
                "pubkey_b64": pubkey_b64,
            }))
            .unwrap(),
        )
        .unwrap();
        let roots = RootSet::load(&home).unwrap();
        let from = format!("gw-directive-test-{}", &fingerprint[..16]);
        (tmp, key, roots, from)
    }

    fn fixture(from: String) -> Directive {
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
        let (tmp, key, roots, from) = trusted_key();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let mut directive = fixture(from.clone());
        directive.class = class;
        sign(&mut directive, &key).unwrap();
        let self_id = directive.target.clone();
        (tmp, home, roots, from, self_id, directive)
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

    #[test]
    fn signed_directive_verifies_against_a_bound_trusted_key() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from.clone());

        sign(&mut directive, &key).unwrap();

        assert!(!directive.sig.is_empty());
        assert_eq!(verify(&directive, &roots).unwrap(), from);
        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
    }

    #[test]
    fn new_directive_mints_a_ulid_and_starts_unsigned() {
        let directive = Directive::new(
            "gw-from-0123456789abcdef",
            "gw-target-0123456789abcdef",
            DirectiveClass::Session,
            "start the requested work",
        );

        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
        assert!(directive.sig.is_empty());
    }

    #[test]
    fn tampered_instruction_is_rejected() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from);
        sign(&mut directive, &key).unwrap();

        directive.instruction.push('!');

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn tampered_sender_is_rejected() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from);
        sign(&mut directive, &key).unwrap();

        directive.from.push_str("-tampered");

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn a_valid_signature_from_an_unbound_key_is_rejected() {
        let (_tmp, key, roots, _from) = trusted_key();
        let mut directive = fixture("gw-directive-test-deadbeef".to_string());

        sign(&mut directive, &key).unwrap();

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn canonical_bytes_are_stable_across_struct_field_order() {
        let (_tmp, _key, _roots, from) = trusted_key();
        let first = fixture(from.clone());
        let second = Directive {
            sig: "ignored-signature".to_string(),
            instruction: first.instruction.clone(),
            class: DirectiveClass::Session,
            ts: first.ts,
            target: first.target.clone(),
            from,
            directive_id: first.directive_id.clone(),
        };

        assert_eq!(canonical_bytes(&first).unwrap(), canonical_bytes(&second).unwrap());
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
}
