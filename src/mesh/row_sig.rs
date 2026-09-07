use crate::mesh::keys;
use crate::mesh::registry::GatewayRow;
use crate::mesh::row_mark::RowMark;
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::de::MapAccess;
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

pub const ROW_MSG_VER: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMsg {
    pub msg_ver: u64,
    pub fleet_id: String,
    pub gateway_id: String,
    pub endpoint_url: Option<String>,
    pub pubkey_fpr: String,
    pub cert_fpr: Option<String>,
    pub version: String,
    pub owner: String,
    pub created_at: DateTime<Utc>,
    pub schema_version: i32,
    pub key_epoch: u64,
    pub roots_held: String,
    pub row_token_sha256: String,
    pub row_resign_secs: u64,
    pub registry_heartbeat_secs: u64,
    pub enforce_require_signed_rows: bool,
    pub enforce_auto_push: bool,
    pub signed_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowSigFault {
    UnknownVersion(u64),
    Malformed(&'static str),
    SignatureMismatch,
}

impl fmt::Display for RowSigFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion(version) => write!(f, "unknown row_msg version {version}"),
            Self::Malformed(message) => f.write_str(message),
            Self::SignatureMismatch => f.write_str("row signature does not verify"),
        }
    }
}

impl std::error::Error for RowSigFault {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowVerdict {
    ValidSigned,
    Unsigned { stripped: bool },
    UnsignedStale,
    Unverified,
    BadSig,
    RowSigIncomplete,
    RowSigUnknownVersion,
    RowKeySuperseded,
    RowReplayed,
    RowClockAhead,
    Revoked,
    RowRevocationMisplaced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    Usable,
    UsableWarn,
    Refused,
}

impl RowVerdict {
    pub fn label(&self) -> &'static str {
        match self {
            Self::ValidSigned => "valid-signed",
            Self::Unsigned { stripped: false } => "UNSIGNED",
            Self::Unsigned { stripped: true } => "UNSIGNED (stripped)",
            Self::UnsignedStale => "UNSIGNED-STALE",
            Self::Unverified => "UNVERIFIED",
            Self::BadSig => "BAD_SIG",
            Self::RowSigIncomplete => "ROW_SIG_INCOMPLETE",
            Self::RowSigUnknownVersion => "ROW_SIG_UNKNOWN_VERSION",
            Self::RowKeySuperseded => "ROW_KEY_SUPERSEDED",
            Self::RowReplayed => "ROW_REPLAYED",
            Self::RowClockAhead => "ROW_CLOCK_AHEAD",
            Self::Revoked => "REVOKED",
            Self::RowRevocationMisplaced => "ROW_REVOCATION_MISPLACED",
        }
    }

    pub fn disposition(&self, require_signed_rows: bool) -> Disposition {
        match self {
            Self::ValidSigned => Disposition::Usable,
            Self::Unsigned { stripped: false } | Self::UnsignedStale | Self::Unverified => {
                if require_signed_rows {
                    Disposition::Refused
                } else {
                    Disposition::UsableWarn
                }
            }
            _ => Disposition::Refused,
        }
    }

    pub fn remedy(&self) -> &'static str {
        match self {
            Self::ValidSigned => "no action is required",
            Self::Unsigned { stripped: false } => {
                "enable signed rows on the peer, then confirm a valid signature before requiring them"
            }
            Self::Unsigned { stripped: true } => {
                "capture the row for evidence, then restore the peer's signed publisher"
            }
            Self::UnsignedStale => {
                "capture the row for evidence and have the peer publish a freshly signed row"
            }
            Self::Unverified => {
                "install the issuing fleet root only after verifying its fingerprint out of band"
            }
            Self::BadSig => {
                "capture the row for evidence first; only then, if deliberate, run `a2a mesh fleet clear-signature <gw>`"
            }
            Self::RowSigIncomplete => {
                "capture the row for evidence and have the peer republish all signature columns atomically"
            }
            Self::RowSigUnknownVersion => "upgrade this reader before trusting or using this row",
            Self::RowKeySuperseded => {
                "install the peer's current enrollment token and have it publish with the latest key epoch"
            }
            Self::RowReplayed => {
                "capture the row for evidence first; use `a2a mesh fleet forget-signed <gw>` only for a deliberate replay reset"
            }
            Self::RowClockAhead => "check the local and peer clocks before accepting a newer row",
            Self::Revoked => "keep the row as evidence and re-enroll the peer only through the fleet root",
            Self::RowRevocationMisplaced => {
                "capture the carrying row and revocation for evidence; do not move or copy the record"
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRowColumns {
    pub fleet_id: String,
    pub gateway_id: String,
    pub endpoint_url: Option<String>,
    pub pubkey_fpr: String,
    pub cert_fpr: Option<String>,
    pub version: String,
    pub owner: String,
    pub created_at: DateTime<Utc>,
    pub schema_version: i32,
    pub row_msg: Option<String>,
    pub row_token: Option<String>,
    pub row_sig: Option<String>,
    pub signed_at: Option<DateTime<Utc>>,
    pub revocation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowVerifyCfg {
    pub now: DateTime<Utc>,
    pub max_accepted_row_resign_secs: u64,
    pub skew_tolerance_secs: i64,
}

impl RowVerifyCfg {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now,
            max_accepted_row_resign_secs:
                crate::config::DEFAULT_MESH_GATEWAY_MAX_ACCEPTED_ROW_RESIGN_SECS,
            skew_tolerance_secs: keys::SKEW_TOLERANCE_SECS,
        }
    }

    pub fn from_config(now: DateTime<Utc>, cfg: &crate::config::Config) -> Self {
        Self {
            max_accepted_row_resign_secs: cfg
                .mesh
                .gateway
                .max_accepted_row_resign_secs,
            ..Self::new(now)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowFlag {
    StaleRow,
    FirstTrust,
    KeyRebound,
    TokenExpired,
    TokenNotYetValid,
    EndpointConflict,
}

impl RowFlag {
    pub fn label(&self) -> &'static str {
        match self {
            Self::StaleRow => "stale-row",
            Self::FirstTrust => "first-trust",
            Self::KeyRebound => "row_key_rebound",
            Self::TokenExpired => "TOKEN_EXPIRED",
            Self::TokenNotYetValid => "TOKEN_NOT_YET_VALID",
            Self::EndpointConflict => "ROW_ENDPOINT_CONFLICT",
        }
    }

    pub fn remedy(&self) -> &'static str {
        match self {
            Self::StaleRow => {
                "check the peer gateway and run `a2a mesh gateway resign` on that machine"
            }
            Self::FirstTrust => {
                "verify the gateway identity and key fingerprint out of band before relying on this first trust"
            }
            Self::KeyRebound => {
                "confirm the higher key epoch matches an intentional owner-approved gateway rebind"
            }
            Self::TokenExpired => {
                "renew the enrollment token on the fleet root and install it on the gateway"
            }
            Self::TokenNotYetValid => {
                "check local and fleet-root clocks before installing or renewing this token"
            }
            Self::EndpointConflict => {
                "keep the incumbent endpoint and investigate the later gateway row before dialing it"
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RevocationState {
    Absent,
    Invalid,
    Misplaced(keys::RevocationClaims),
    Revoked(keys::RevocationClaims),
    Superseded {
        record: keys::RevocationClaims,
        token_epoch: u64,
    },
}

impl From<&GatewayRow> for StoredRowColumns {
    fn from(row: &GatewayRow) -> Self {
        Self {
            fleet_id: row.fleet_id.clone(),
            gateway_id: row.gateway_id.clone(),
            endpoint_url: row.endpoint_url.clone(),
            pubkey_fpr: row.pubkey_fpr.clone(),
            cert_fpr: row.cert_fpr.clone(),
            version: row.version.clone(),
            owner: row.owner.clone(),
            created_at: row.created_at,
            schema_version: row.schema_version,
            row_msg: row.row_msg.clone(),
            row_token: row.row_token.clone(),
            row_sig: row.row_sig.clone(),
            signed_at: row.signed_at,
            revocation: row.revocation.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    AllNull,
    Partial,
    Present,
}

fn shape(stored: &StoredRowColumns) -> Shape {
    let present = [
        stored.row_msg.is_some(),
        stored.row_token.is_some(),
        stored.row_sig.is_some(),
        stored.signed_at.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    match present {
        0 => Shape::AllNull,
        4 => Shape::Present,
        _ => Shape::Partial,
    }
}

// REVOKED > ROW_REVOCATION_MISPLACED > ROW_SIG_UNKNOWN_VERSION > ROW_SIG_INCOMPLETE
//   > malformation-BAD_SIG > ROW_KEY_SUPERSEDED > ROW_REPLAYED / ROW_CLOCK_AHEAD
//   > disagreement (BAD_SIG | UNSIGNED-STALE, by the bit) > UNVERIFIED > UNSIGNED > valid-signed
pub fn verify_row(
    stored: &StoredRowColumns,
    roots: &keys::RootSet,
    mark: Option<&RowMark>,
    cfg: &RowVerifyCfg,
) -> RowVerdict {
    if let Some(verdict) = revocation_verdict(stored, roots) {
        return verdict;
    }
    if let Some(text) = stored.row_msg.as_deref() {
        match peek_msg_ver(text) {
            Err(RowSigFault::UnknownVersion(_)) => return RowVerdict::RowSigUnknownVersion,
            Err(_) => return RowVerdict::BadSig,
            Ok(_) => {}
        }
    }
    let bit = mark.is_some_and(|row_mark| row_mark.first_signature);
    match shape(stored) {
        Shape::Partial => return RowVerdict::RowSigIncomplete,
        Shape::AllNull => return RowVerdict::Unsigned { stripped: bit },
        Shape::Present => {}
    }
    let msg = match parse_row_msg_strict(stored.row_msg.as_deref().expect("present shape")) {
        Ok(msg) => msg,
        Err(_) => return RowVerdict::BadSig,
    };
    let token = stored.row_token.as_deref().expect("present shape");
    let (claims, token_verified) = match keys::verify_token_no_clock(token, roots) {
        Ok((claims, _issuer)) => (claims, true),
        Err(_) => match keys::unverified_claims(token) {
            Ok(claims) => (claims, false),
            Err(_) => return RowVerdict::BadSig,
        },
    };
    let public_key_matches = keys::fpr_of_pubkey_b64(&claims.pubkey_b64)
        .ok()
        .as_deref()
        == Some(stored.pubkey_fpr.as_str());
    if sha256_hex(token.as_bytes()) != msg.row_token_sha256
        || claims.gateway_id != stored.gateway_id
        || claims.fleet_id != stored.fleet_id
        || !public_key_matches
        || claims.key_epoch != msg.key_epoch
        || !gateway_id_binds_key(&stored.gateway_id, &claims)
    {
        return RowVerdict::BadSig;
    }
    if let Some(row_mark) = mark {
        if claims.key_epoch < row_mark.highest_key_epoch {
            return RowVerdict::RowKeySuperseded;
        }
        if row_mark
            .pinned_pubkey_b64
            .as_deref()
            .is_some_and(|pinned| pinned != claims.pubkey_b64)
            && claims.key_epoch <= row_mark.highest_key_epoch
        {
            return RowVerdict::BadSig;
        }
        if let Some(high_water) = row_mark.high_water {
            if msg.signed_at < high_water {
                return RowVerdict::RowReplayed;
            }
            let stored_digest = sha256_hex(
                stored
                    .row_msg
                    .as_deref()
                    .expect("present shape")
                    .as_bytes(),
            );
            if msg.signed_at == high_water
                && row_mark.msg_digest.as_deref() != Some(stored_digest.as_str())
            {
                return RowVerdict::RowReplayed;
            }
        }
    }
    if msg.signed_at > cfg.now + chrono::Duration::seconds(cfg.skew_tolerance_secs) {
        return RowVerdict::RowClockAhead;
    }
    let key = mark
        .filter(|row_mark| claims.key_epoch <= row_mark.highest_key_epoch)
        .and_then(|row_mark| row_mark.pinned_pubkey_b64.clone())
        .unwrap_or_else(|| claims.pubkey_b64.clone());
    let verifies = verify_row_bytes(
        stored.row_msg.as_deref().expect("present shape"),
        stored.row_sig.as_deref().expect("present shape"),
        &key,
    )
    .is_ok()
        && columns_bind(&msg, stored);
    if !verifies {
        return if bit {
            RowVerdict::BadSig
        } else {
            RowVerdict::UnsignedStale
        };
    }
    if !token_verified {
        return RowVerdict::Unverified;
    }
    RowVerdict::ValidSigned
}

pub fn row_flags(
    stored: &StoredRowColumns,
    mark: Option<&RowMark>,
    cfg: &RowVerifyCfg,
) -> Vec<RowFlag> {
    let mut flags = Vec::new();
    if let Some(msg) = stored
        .row_msg
        .as_deref()
        .and_then(|text| parse_row_msg_strict(text).ok())
    {
        let declared = msg
            .row_resign_secs
            .min(cfg.max_accepted_row_resign_secs);
        let stale_after = declared.saturating_mul(2).min(i64::MAX as u64) as i64;
        if cfg.now.signed_duration_since(msg.signed_at)
            > chrono::Duration::seconds(stale_after)
        {
            flags.push(RowFlag::StaleRow);
        }
    }
    if let Some(claims) = stored
        .row_token
        .as_deref()
        .and_then(|token| keys::unverified_claims(token).ok())
    {
        if claims.key_epoch > 0 {
            flags.push(RowFlag::KeyRebound);
        }
        if cfg.now.timestamp() > claims.exp {
            flags.push(RowFlag::TokenExpired);
        }
        if cfg.now.timestamp() < claims.issued_at {
            flags.push(RowFlag::TokenNotYetValid);
        }
    }
    if mark.is_none() {
        flags.push(RowFlag::FirstTrust);
    }
    flags
}

pub fn endpoint_conflicts(rows: &[(GatewayRow, RowVerdict)]) -> BTreeMap<String, String> {
    let mut groups = BTreeMap::<(String, String), Vec<&GatewayRow>>::new();
    for (row, verdict) in rows {
        if *verdict != RowVerdict::ValidSigned {
            continue;
        }
        let (Some(endpoint), Some(cert_fpr)) = (&row.endpoint_url, &row.cert_fpr) else {
            continue;
        };
        groups
            .entry((endpoint.clone(), cert_fpr.clone()))
            .or_default()
            .push(row);
    }
    let mut conflicts = BTreeMap::new();
    for group in groups.values_mut() {
        group.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.gateway_id.cmp(&right.gateway_id))
        });
        let Some(incumbent) = group.first() else {
            continue;
        };
        for later in group.iter().skip(1) {
            if later.gateway_id != incumbent.gateway_id {
                conflicts.insert(later.gateway_id.clone(), incumbent.gateway_id.clone());
            }
        }
    }
    conflicts
}

fn columns_bind(msg: &RowMsg, stored: &StoredRowColumns) -> bool {
    msg.fleet_id == stored.fleet_id
        && msg.gateway_id == stored.gateway_id
        && msg.endpoint_url == stored.endpoint_url
        && msg.pubkey_fpr == stored.pubkey_fpr
        && msg.cert_fpr == stored.cert_fpr
        && msg.version == stored.version
        && msg.owner == stored.owner
        && trunc_us(msg.created_at) == trunc_us(stored.created_at)
        && msg.schema_version == stored.schema_version
        && stored
            .signed_at
            .is_some_and(|signed_at| trunc_us(msg.signed_at) == trunc_us(signed_at))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn gateway_id_binds_key(gateway_id: &str, claims: &keys::TokenClaims) -> bool {
    if claims.key_epoch > 0 {
        return true;
    }
    let Some((_prefix, suffix)) = gateway_id.rsplit_once('-') else {
        return false;
    };
    if suffix.len() < 6
        || suffix.len() > 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return false;
    }
    keys::fpr_of_pubkey_b64(&claims.pubkey_b64)
        .is_ok_and(|fingerprint| fingerprint.starts_with(suffix))
}

fn revocation_verdict(
    stored: &StoredRowColumns,
    roots: &keys::RootSet,
) -> Option<RowVerdict> {
    match revocation_state(stored, roots) {
        RevocationState::Absent | RevocationState::Superseded { .. } => None,
        RevocationState::Invalid => Some(RowVerdict::BadSig),
        RevocationState::Misplaced(_) => Some(RowVerdict::RowRevocationMisplaced),
        RevocationState::Revoked(_) => Some(RowVerdict::Revoked),
    }
}

pub fn revocation_state(
    stored: &StoredRowColumns,
    roots: &keys::RootSet,
) -> RevocationState {
    let Some(text) = stored.revocation.as_deref() else {
        return RevocationState::Absent;
    };
    let record = match keys::verify_revocation(text, roots) {
        Ok(record) => record,
        Err(_) => return RevocationState::Invalid,
    };
    if record.fleet_id != stored.fleet_id || record.gateway_id != stored.gateway_id {
        return RevocationState::Misplaced(record);
    }
    let token_epoch = stored
        .row_token
        .as_deref()
        .and_then(|token| keys::verify_token_no_clock(token, roots).ok())
        .map(|(claims, _issuer)| claims)
        .filter(|claims| {
            claims.fleet_id == stored.fleet_id && claims.gateway_id == stored.gateway_id
        })
        .map(|claims| claims.key_epoch);
    if let Some(token_epoch) = token_epoch.filter(|epoch| *epoch > record.key_epoch) {
        return RevocationState::Superseded {
            record,
            token_epoch,
        };
    }
    RevocationState::Revoked(record)
}

pub fn canonical_row_msg(msg: &RowMsg) -> Result<String, AppError> {
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("cert_fpr", json!(&msg.cert_fpr)),
        ("created_at", Value::String(rfc3339_us(msg.created_at))),
        ("endpoint_url", json!(&msg.endpoint_url)),
        ("enforce_auto_push", json!(msg.enforce_auto_push)),
        (
            "enforce_require_signed_rows",
            json!(msg.enforce_require_signed_rows),
        ),
        ("fleet_id", json!(&msg.fleet_id)),
        ("gateway_id", json!(&msg.gateway_id)),
        ("key_epoch", json!(msg.key_epoch)),
        ("msg_ver", json!(msg.msg_ver)),
        ("owner", json!(&msg.owner)),
        ("pubkey_fpr", json!(&msg.pubkey_fpr)),
        (
            "registry_heartbeat_secs",
            json!(msg.registry_heartbeat_secs),
        ),
        ("roots_held", json!(&msg.roots_held)),
        ("row_resign_secs", json!(msg.row_resign_secs)),
        ("row_token_sha256", json!(&msg.row_token_sha256)),
        ("schema_version", json!(msg.schema_version)),
        ("signed_at", Value::String(rfc3339_us(msg.signed_at))),
        ("version", json!(&msg.version)),
    ]);
    String::from_utf8(keys::canonical_json_fields(fields)?).map_err(|error| {
        AppError::new(
            "SERIALIZE",
            ExitCode::Internal,
            format!("canonical row message was not UTF-8: {error}"),
        )
    })
}

pub fn row_signature_prefix(msg_ver: u64) -> Vec<u8> {
    format!("a2a/registry-row/v{msg_ver}\0").into_bytes()
}

pub fn row_signature_digest(msg_ver: u64, row_msg: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(row_signature_prefix(msg_ver))
        .chain_update(row_msg)
        .finalize()
        .into()
}

pub fn sign_row(msg: &RowMsg, key: &SigningKey) -> Result<(String, String), AppError> {
    let row_msg = canonical_row_msg(msg)?;
    let signature = key.sign(&row_signature_digest(msg.msg_ver, row_msg.as_bytes()));
    Ok((row_msg, keys::b64_encode(&signature.to_bytes())))
}

struct StrictObject(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for StrictObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrictObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for StrictObjectVisitor {
            type Value = StrictObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a flat JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    if out.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate key {key}"
                        )));
                    }
                }
                Ok(StrictObject(out))
            }
        }

        deserializer.deserialize_map(StrictObjectVisitor)
    }
}

pub fn peek_msg_ver(text: &str) -> Result<u64, RowSigFault> {
    let fields = strict_fields(text)?;
    let msg_ver = required_u64(&fields, "msg_ver")?;
    if msg_ver != ROW_MSG_VER {
        return Err(RowSigFault::UnknownVersion(msg_ver));
    }
    Ok(msg_ver)
}

pub fn parse_row_msg_strict(text: &str) -> Result<RowMsg, RowSigFault> {
    const FIELD_NAMES: [&str; 18] = [
        "cert_fpr",
        "created_at",
        "endpoint_url",
        "enforce_auto_push",
        "enforce_require_signed_rows",
        "fleet_id",
        "gateway_id",
        "key_epoch",
        "msg_ver",
        "owner",
        "pubkey_fpr",
        "registry_heartbeat_secs",
        "roots_held",
        "row_resign_secs",
        "row_token_sha256",
        "schema_version",
        "signed_at",
        "version",
    ];

    let fields = strict_fields(text)?;
    let msg_ver = required_u64(&fields, "msg_ver")?;
    if msg_ver != ROW_MSG_VER {
        return Err(RowSigFault::UnknownVersion(msg_ver));
    }
    if fields
        .keys()
        .any(|field| !FIELD_NAMES.contains(&field.as_str()))
    {
        return Err(RowSigFault::Malformed("unknown field"));
    }
    if FIELD_NAMES.iter().any(|field| !fields.contains_key(*field)) {
        return Err(RowSigFault::Malformed("missing field"));
    }

    Ok(RowMsg {
        msg_ver,
        fleet_id: required_string(&fields, "fleet_id")?,
        gateway_id: required_string(&fields, "gateway_id")?,
        endpoint_url: nullable_string(&fields, "endpoint_url")?,
        pubkey_fpr: required_string(&fields, "pubkey_fpr")?,
        cert_fpr: nullable_string(&fields, "cert_fpr")?,
        version: required_string(&fields, "version")?,
        owner: required_string(&fields, "owner")?,
        created_at: required_timestamp(&fields, "created_at")?,
        schema_version: required_i32(&fields, "schema_version")?,
        key_epoch: required_u64(&fields, "key_epoch")?,
        roots_held: required_string(&fields, "roots_held")?,
        row_token_sha256: required_string(&fields, "row_token_sha256")?,
        row_resign_secs: required_u64(&fields, "row_resign_secs")?,
        registry_heartbeat_secs: required_u64(&fields, "registry_heartbeat_secs")?,
        enforce_require_signed_rows: required_bool(&fields, "enforce_require_signed_rows")?,
        enforce_auto_push: required_bool(&fields, "enforce_auto_push")?,
        signed_at: required_timestamp(&fields, "signed_at")?,
    })
}

pub fn verify_row_bytes(
    row_msg: &str,
    row_sig_b64: &str,
    pubkey_b64: &str,
) -> Result<RowMsg, RowSigFault> {
    let msg_ver = peek_msg_ver(row_msg)?;
    if msg_ver != ROW_MSG_VER {
        return Err(RowSigFault::UnknownVersion(msg_ver));
    }
    let msg = parse_row_msg_strict(row_msg)?;
    let signature_bytes = keys::b64_decode(row_sig_b64).map_err(|_| {
        RowSigFault::Malformed("row_sig is not a 64-byte base64 signature")
    })?;
    let signature_bytes: [u8; 64] = signature_bytes.try_into().map_err(|_| {
        RowSigFault::Malformed("row_sig is not a 64-byte base64 signature")
    })?;
    let signature = Signature::from_bytes(&signature_bytes);
    let key = keys::verifying_key_from_b64(pubkey_b64)
        .map_err(|_| RowSigFault::Malformed("pubkey is invalid"))?;
    key.verify(
        &row_signature_digest(msg.msg_ver, row_msg.as_bytes()),
        &signature,
    )
    .map_err(|_| RowSigFault::SignatureMismatch)?;
    Ok(msg)
}

fn strict_fields(text: &str) -> Result<BTreeMap<String, Value>, RowSigFault> {
    serde_json::from_str::<StrictObject>(text)
        .map(|strict| strict.0)
        .map_err(|error| {
            if error.to_string().contains("duplicate key") {
                RowSigFault::Malformed("duplicate key")
            } else {
                RowSigFault::Malformed("row_msg is not a flat JSON object")
            }
        })
}

fn field<'a>(
    fields: &'a BTreeMap<String, Value>,
    name: &str,
) -> Result<&'a Value, RowSigFault> {
    fields
        .get(name)
        .ok_or(RowSigFault::Malformed("missing field"))
}

fn required_string(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<String, RowSigFault> {
    field(fields, name)?
        .as_str()
        .map(str::to_string)
        .ok_or(RowSigFault::Malformed("field has invalid type"))
}

fn nullable_string(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<Option<String>, RowSigFault> {
    match field(fields, name)? {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        _ => Err(RowSigFault::Malformed("field has invalid type")),
    }
}

fn required_u64(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<u64, RowSigFault> {
    field(fields, name)?
        .as_u64()
        .ok_or(RowSigFault::Malformed("field has invalid type"))
}

fn required_i32(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<i32, RowSigFault> {
    field(fields, name)?
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or(RowSigFault::Malformed("field has invalid type"))
}

fn required_bool(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<bool, RowSigFault> {
    field(fields, name)?
        .as_bool()
        .ok_or(RowSigFault::Malformed("field has invalid type"))
}

fn required_timestamp(
    fields: &BTreeMap<String, Value>,
    name: &str,
) -> Result<DateTime<Utc>, RowSigFault> {
    let text = field(fields, name)?
        .as_str()
        .ok_or(RowSigFault::Malformed("field has invalid type"))?;
    DateTime::parse_from_rfc3339(text)
        .map(|value| trunc_us(value.with_timezone(&Utc)))
        .map_err(|_| RowSigFault::Malformed("timestamp is invalid"))
}

fn rfc3339_us(dt: DateTime<Utc>) -> String {
    trunc_us(dt).to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn trunc_us(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(dt.timestamp_subsec_micros() * 1_000)
        .expect("micros fit")
}
