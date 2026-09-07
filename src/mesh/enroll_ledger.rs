use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::keys::{self, RootSet, TokenClaims};
use crate::mesh::render::escape_untrusted;
use crate::mesh::registry::GatewayRow;
use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::PathBuf;

const LEDGER_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerEntry {
    pub gateway_id: String,
    pub pubkey_b64: String,
    pub key_epoch: u64,
    pub issuing_root_fpr: String,
    pub issued_at: i64,
    pub exp: i64,
}

impl LedgerEntry {
    pub fn from_claims(claims: &TokenClaims, issuing_root_fpr: String) -> Self {
        Self {
            gateway_id: claims.gateway_id.clone(),
            pubkey_b64: claims.pubkey_b64.clone(),
            key_epoch: claims.key_epoch,
            issuing_root_fpr,
            issued_at: claims.issued_at,
            exp: claims.exp,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EnrollLedger {
    entries: Vec<LedgerEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerFile {
    v: u8,
    entries: Vec<LedgerEntry>,
}

impl EnrollLedger {
    pub fn load(home: &A2aHome) -> Result<Self, AppError> {
        let path = ledger_path(home);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(io_error("reading enrollment ledger", error)),
        };
        let file: LedgerFile = serde_json::from_slice(&bytes).map_err(|error| {
            AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("enrollment ledger is invalid: {error}"),
            )
            .remedy("restore mesh/fleet/enrollments.json from trusted owner-side state")
        })?;
        if file.v != LEDGER_VERSION {
            return Err(AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("enrollment ledger version {} is unsupported", file.v),
            )
            .remedy("upgrade a2a before changing fleet enrollment state"));
        }
        Ok(Self {
            entries: file.entries,
        })
    }

    pub fn entries(&self) -> &[LedgerEntry] {
        &self.entries
    }

    pub fn latest_for(&self, gateway_id: &str) -> Option<&LedgerEntry> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.gateway_id == gateway_id)
    }

    pub fn record(&mut self, home: &A2aHome, entry: LedgerEntry) -> Result<(), AppError> {
        validate_gateway_id(&entry.gateway_id)?;
        let previous_len = self.entries.len();
        self.entries.push(entry);
        if let Err(error) = self.persist(home) {
            self.entries.truncate(previous_len);
            return Err(error);
        }
        Ok(())
    }

    fn persist(&self, home: &A2aHome) -> Result<(), AppError> {
        let path = ledger_path(home);
        let parent = path.parent().expect("ledger path has a parent");
        create_dir_0700(parent).map_err(|error| io_error("creating enrollment ledger", error))?;
        let mut bytes = serde_json::to_vec_pretty(&LedgerFile {
            v: LEDGER_VERSION,
            entries: self.entries.clone(),
        })
        .map_err(|error| {
            AppError::new("SERIALIZE", ExitCode::Internal, error.to_string())
        })?;
        bytes.push(b'\n');
        crate::store::write_atomic(&path, &bytes)
            .map_err(|error| io_error("writing enrollment ledger", error))
    }
}

pub fn enroll_conflict(
    gateway_id: &str,
    requested_pubkey_b64: &str,
    ledger: &EnrollLedger,
    live_row: Option<&GatewayRow>,
) -> Result<(), AppError> {
    let requested_fpr = keys::fpr_of_pubkey_b64(requested_pubkey_b64)?;
    let mut conflicts = Vec::<(&str, String)>::new();
    if let Some(entry) = ledger.latest_for(gateway_id) {
        let ledger_fpr = keys::fpr_of_pubkey_b64(&entry.pubkey_b64)?;
        if ledger_fpr != requested_fpr {
            conflicts.push(("ledger", ledger_fpr));
        }
    }
    if let Some(row) = live_row {
        let live_fpr = escape_untrusted(&row.pubkey_fpr, 128).text;
        if live_fpr != requested_fpr
            && !conflicts
                .iter()
                .any(|(_source, fingerprint)| fingerprint == &live_fpr)
        {
            conflicts.push(("live row", live_fpr));
        }
    }
    if conflicts.is_empty() {
        return Ok(());
    }
    let existing = conflicts
        .iter()
        .map(|(source, fingerprint)| format!("{source}={fingerprint}"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(AppError::new(
        "ENROLLMENT_REBIND_REQUIRED",
        ExitCode::Precondition,
        format!(
            "gateway {gateway_id} is bound to {existing}; requested fingerprint={requested_fpr}"
        ),
    )
    .remedy("verify both fingerprints out of band, then rerun enrollment with --rebind")
    .data(json!({
        "gateway_id": gateway_id,
        "requested_fingerprint": requested_fpr,
        "existing": conflicts
            .into_iter()
            .map(|(source, fingerprint)| json!({"source": source, "fingerprint": fingerprint}))
            .collect::<Vec<_>>(),
    })))
}

pub fn enrollment_epoch(
    gateway_id: &str,
    rebind: bool,
    ledger: &EnrollLedger,
    live_row: Option<&GatewayRow>,
    roots: &RootSet,
) -> Result<u64, AppError> {
    let ledger_epoch = ledger
        .entries()
        .iter()
        .filter(|entry| entry.gateway_id == gateway_id)
        .map(|entry| entry.key_epoch)
        .max()
        .unwrap_or(0);
    let row_epoch = live_row
        .and_then(|row| {
            let (claims, _issuer) =
                keys::verify_token_no_clock(row.row_token.as_deref()?, roots).ok()?;
            (claims.gateway_id == gateway_id && claims.fleet_id == row.fleet_id)
                .then_some(claims.key_epoch)
        })
        .unwrap_or(0);
    let epoch = ledger_epoch.max(row_epoch);
    if rebind {
        epoch.checked_add(1).ok_or_else(|| {
            AppError::new(
                "ENROLLMENT_REBIND_REQUIRED",
                ExitCode::Precondition,
                format!("gateway {gateway_id} key epoch cannot be incremented past {epoch}"),
            )
            .remedy("inspect the owner enrollment ledger before retrying the rebind")
        })
    } else {
        Ok(epoch)
    }
}

fn ledger_path(home: &A2aHome) -> PathBuf {
    home.root()
        .join("mesh")
        .join("fleet")
        .join("enrollments.json")
}

fn validate_gateway_id(gateway_id: &str) -> Result<(), AppError> {
    let valid = !gateway_id.is_empty()
        && gateway_id.len() <= 64
        && gateway_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(AppError::new(
            "ENROLLMENT_REBIND_REQUIRED",
            ExitCode::Precondition,
            "gateway_id must match ^[A-Za-z0-9_-]{1,64}$ before ledger storage",
        )
        .remedy("use the exact gateway id printed by `a2a mesh gateway identity`"))
    }
}

fn io_error(context: &str, error: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {error}"))
}
