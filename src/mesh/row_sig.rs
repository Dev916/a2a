use crate::mesh::keys;
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
            max_accepted_row_resign_secs: 604_800,
            skew_tolerance_secs: keys::SKEW_TOLERANCE_SECS,
        }
    }
}

// R4's verify_row decision procedure consumes these parsing, signature, verdict, and config
// primitives; keeping that policy out of this module lets this layer remain byte-focused.

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
    required_u64(&fields, "msg_ver")
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
