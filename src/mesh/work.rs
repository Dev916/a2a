use crate::home::A2aHome;
use crate::mesh::keys::{
    self, b64_decode, b64_encode, canonical_json_fields, Clock, RootSet, TokenClaims,
};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, Signer, Verifier};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const WORK_ENVELOPE_VERSION: u8 = 1;
const WORK_ENVELOPE_DOMAIN: &[u8] = b"a2a/work-envelope/v1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkEnvelope {
    pub v: u8,
    pub work_id: String,
    pub fleet_id: String,
    pub sender: WorkSender,
    pub addressing: WorkAddressing,
    pub repo: WorkRepo,
    pub title: String,
    pub brief_text: String,
    pub acceptance: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub sig_b64: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkSender {
    pub gateway_id: String,
    pub session: String,
    pub pubkey_b64: String,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkAddressing {
    Directed(String),
    Pool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkRepo {
    pub remote_url: String,
    pub base_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsignedWorkEnvelope {
    pub work_id: String,
    pub session: String,
    pub addressing: WorkAddressing,
    pub repo: WorkRepo,
    pub title: String,
    pub brief_text: String,
    pub acceptance: String,
    pub ttl_days: i64,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedEnvelope {
    pub envelope: WorkEnvelope,
    pub sender_claims: TokenClaims,
}

pub fn sign_envelope(
    home: &A2aHome,
    unsigned: UnsignedWorkEnvelope,
    clock: &dyn Clock,
) -> Result<WorkEnvelope, AppError> {
    validate_repo_url(&unsigned.repo.remote_url)?;
    validate_remote_text("title", &unsigned.title, TextShape::OneLine)?;
    validate_remote_text("brief_text", &unsigned.brief_text, TextShape::MultiLine)?;
    validate_remote_text("acceptance", &unsigned.acceptance, TextShape::MultiLine)?;

    let gateway = keys::gateway_identity(home, false)?;
    let pubkey_b64 = keys::gateway_public_key_b64(home)?;
    let token = keys::gateway_token(home)?;
    let created_at = clock.now();
    let expires_at = created_at + Duration::days(unsigned.ttl_days);

    let mut env = WorkEnvelope {
        v: WORK_ENVELOPE_VERSION,
        work_id: unsigned.work_id,
        fleet_id: keys::fleet_id(home)?,
        sender: WorkSender {
            gateway_id: gateway.gateway_id,
            session: unsigned.session,
            pubkey_b64,
            token,
        },
        addressing: unsigned.addressing,
        repo: unsigned.repo,
        title: unsigned.title,
        brief_text: unsigned.brief_text,
        acceptance: unsigned.acceptance,
        created_at,
        expires_at,
        sig_b64: String::new(),
    };

    let digest = envelope_signature_digest(&env)?;
    let signing = keys::gateway_signing_key(home)?;
    env.sig_b64 = b64_encode(&signing.sign(&digest).to_bytes());
    validate_envelope_hygiene(&env, unsigned.max_bytes)?;
    Ok(env)
}

pub fn verify_envelope(
    home: &A2aHome,
    env: &WorkEnvelope,
    clock: &dyn Clock,
) -> Result<VerifiedEnvelope, AppError> {
    let claims =
        keys::verify_token(env.sender.token.trim(), &RootSet::load(home)?, clock).map_err(|err| {
            work_sender_unknown(format!(
                "sender token cannot be verified against local fleet roots: {}",
                err.message
            ))
        })?;

    let home_fleet_id = keys::fleet_id(home)?;
    if claims.fleet_id != home_fleet_id {
        return Err(work_wrong_fleet(format!(
            "sender token belongs to {}, local fleet is {}",
            claims.fleet_id, home_fleet_id
        )));
    }
    if claims.gateway_id != env.sender.gateway_id {
        return Err(work_token_mismatch(
            "gateway_id",
            &claims.gateway_id,
            &env.sender.gateway_id,
        ));
    }
    if claims.pubkey_b64 != env.sender.pubkey_b64 {
        return Err(work_token_mismatch(
            "pubkey_b64",
            &claims.pubkey_b64,
            &env.sender.pubkey_b64,
        ));
    }

    verify_signature(env)?;
    if env.v != WORK_ENVELOPE_VERSION {
        return Err(work_signature_invalid("work envelope version is unsupported"));
    }

    if env.fleet_id != home_fleet_id {
        return Err(work_wrong_fleet(format!(
            "work envelope belongs to {}, local fleet is {}",
            env.fleet_id, home_fleet_id
        )));
    }
    if clock.now() >= env.expires_at {
        return Err(AppError::new(
            "WORK_EXPIRED",
            ExitCode::Precondition,
            format!("work envelope expired at {}", env.expires_at),
        )
        .remedy("ask the sender to issue a fresh work envelope"));
    }
    if let WorkAddressing::Directed(gateway_id) = &env.addressing {
        let local_gateway_id = keys::gateway_id(home)?;
        if gateway_id != &local_gateway_id {
            return Err(AppError::new(
                "WORK_TOKEN_MISMATCH",
                ExitCode::Precondition,
                format!(
                    "directed envelope targets gateway {gateway_id}, local gateway is {local_gateway_id}"
                ),
            )
            .remedy("only approve work addressed to this gateway, or use a pool offer"));
        }
    }

    Ok(VerifiedEnvelope {
        envelope: env.clone(),
        sender_claims: claims,
    })
}

pub fn validate_envelope_hygiene(env: &WorkEnvelope, max_bytes: usize) -> Result<(), AppError> {
    validate_repo_url(&env.repo.remote_url)?;
    validate_remote_text("title", &env.title, TextShape::OneLine)?;
    validate_remote_text("brief_text", &env.brief_text, TextShape::MultiLine)?;
    validate_remote_text("acceptance", &env.acceptance, TextShape::MultiLine)?;

    let bytes = serde_json::to_vec(env)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
    if bytes.len() > max_bytes {
        return Err(AppError::new(
            "WORK_TOO_LARGE",
            ExitCode::Precondition,
            format!(
                "work envelope is {} bytes, exceeding cap {max_bytes} bytes",
                bytes.len()
            ),
        )
        .remedy("shorten the brief or use the configured directed/pool size cap"));
    }
    Ok(())
}

pub fn canonical_envelope_without_sig(env: &WorkEnvelope) -> Result<Vec<u8>, AppError> {
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("acceptance", json!(env.acceptance)),
        ("addressing", serde_json::to_value(&env.addressing).map_err(serialize_err)?),
        ("brief_text", json!(env.brief_text)),
        ("created_at", json!(env.created_at)),
        ("expires_at", json!(env.expires_at)),
        ("fleet_id", json!(env.fleet_id)),
        ("repo", serde_json::to_value(&env.repo).map_err(serialize_err)?),
        ("sender", serde_json::to_value(&env.sender).map_err(serialize_err)?),
        ("title", json!(env.title)),
        ("v", json!(env.v)),
        ("work_id", json!(env.work_id)),
    ]);
    canonical_json_fields(fields)
}

impl Serialize for WorkAddressing {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WorkAddressing::Directed(gateway_id) => {
                serializer.serialize_str(&format!("directed:{gateway_id}"))
            }
            WorkAddressing::Pool => serializer.serialize_str("pool"),
        }
    }
}

impl<'de> Deserialize<'de> for WorkAddressing {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "pool" {
            return Ok(WorkAddressing::Pool);
        }
        let Some(gateway_id) = s.strip_prefix("directed:") else {
            return Err(serde::de::Error::custom(
                "addressing must be `pool` or `directed:<gateway_id>`",
            ));
        };
        if gateway_id.is_empty()
            || gateway_id
                .bytes()
                .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        {
            return Err(serde::de::Error::custom(
                "directed addressing requires a non-empty gateway id",
            ));
        }
        Ok(WorkAddressing::Directed(gateway_id.to_string()))
    }
}

fn verify_signature(env: &WorkEnvelope) -> Result<(), AppError> {
    let sig_bytes = b64_decode(&env.sig_b64)
        .map_err(|_| work_signature_invalid("work envelope signature is not base64"))?;
    let sig: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| work_signature_invalid("work envelope signature has invalid length"))?;
    let signature = Signature::from_bytes(&sig);
    let key = keys::verifying_key_from_b64(&env.sender.pubkey_b64)
        .map_err(|_| work_signature_invalid("sender public key is invalid"))?;
    let digest = envelope_signature_digest(env)?;
    key.verify(&digest, &signature)
        .map_err(|_| work_signature_invalid("work envelope signature does not verify"))
}

fn envelope_signature_digest(env: &WorkEnvelope) -> Result<[u8; 32], AppError> {
    let canonical = canonical_envelope_without_sig(env)?;
    let digest = Sha256::new()
        .chain_update(WORK_ENVELOPE_DOMAIN)
        .chain_update(canonical)
        .finalize();
    Ok(digest.into())
}

#[derive(Clone, Copy)]
enum TextShape {
    OneLine,
    MultiLine,
}

fn validate_remote_text(field: &'static str, text: &str, shape: TextShape) -> Result<(), AppError> {
    for c in text.chars() {
        let allowed_layout = matches!(shape, TextShape::MultiLine) && matches!(c, '\n' | '\r' | '\t');
        if c.is_control() && !allowed_layout {
            return Err(AppError::new(
                "WORK_CONTROL_CHARS",
                ExitCode::Precondition,
                format!("work envelope field {field} contains a control character"),
            )
            .remedy("remove terminal control characters before sending work"));
        }
    }
    Ok(())
}

fn validate_repo_url(url: &str) -> Result<(), AppError> {
    let valid = !url.is_empty()
        && !url.starts_with('-')
        && !url.bytes().any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        && (valid_scheme_url(url, "ssh://")
            || valid_scheme_url(url, "https://")
            || valid_scp_like_url(url));
    if valid {
        Ok(())
    } else {
        Err(AppError::new(
            "WORK_BAD_REPO_URL",
            ExitCode::Precondition,
            "work envelope repo remote_url must be ssh://, https://, or git@host:path and must not begin with '-'",
        )
        .remedy("use a canonical git remote URL; git invocation paths pass it after `--`"))
    }
}

fn valid_scheme_url(url: &str, scheme: &str) -> bool {
    let Some(rest) = url.strip_prefix(scheme) else {
        return false;
    };
    let Some((host, path)) = rest.split_once('/') else {
        return false;
    };
    !host.is_empty() && !path.is_empty()
}

fn valid_scp_like_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("git@") else {
        return false;
    };
    let Some((host, path)) = rest.split_once(':') else {
        return false;
    };
    !host.is_empty() && !path.is_empty()
}

fn work_signature_invalid(message: impl Into<String>) -> AppError {
    AppError::new("WORK_SIGNATURE_INVALID", ExitCode::Precondition, message)
        .remedy("discard the envelope and ask the sender to re-send it")
}

fn work_sender_unknown(message: impl Into<String>) -> AppError {
    AppError::new("WORK_SENDER_UNKNOWN", ExitCode::Precondition, message)
        .remedy("install the sender fleet root, or ask the fleet root holder to re-enroll the sender")
}

fn work_token_mismatch(field: &'static str, expected: &str, actual: &str) -> AppError {
    AppError::new(
        "WORK_TOKEN_MISMATCH",
        ExitCode::Precondition,
        format!("sender token {field} mismatch: expected {expected}, got {actual}"),
    )
    .remedy("re-enroll the sender gateway and retry with a fresh envelope")
}

fn work_wrong_fleet(message: impl Into<String>) -> AppError {
    AppError::new("WORK_WRONG_FLEET", ExitCode::Precondition, message)
        .remedy("only ingest work from the local fleet")
}

fn serialize_err(e: serde_json::Error) -> AppError {
    AppError::new("SERIALIZE", ExitCode::Internal, e.to_string())
}
