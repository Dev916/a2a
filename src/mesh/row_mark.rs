use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::keys::{RevocationClaims, TokenClaims};
use crate::mesh::row_sig::{canonical_row_msg, RowMsg};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowMark {
    pub high_water: Option<DateTime<Utc>>,
    pub msg_digest: Option<String>,
    pub first_signature: bool,
    pub pinned_pubkey_b64: Option<String>,
    pub highest_key_epoch: u64,
}

/// Reader-local replay and first-signature state.
///
/// Opening the store never creates a directory or file. Only an explicit state mutation writes,
/// which keeps read-only commands such as `mesh fleet ls` read-only on a cold home.
#[derive(Clone, Debug)]
pub struct RowMarkStore {
    root: Option<PathBuf>,
    marks: BTreeMap<String, RowMark>,
}

impl Default for RowMarkStore {
    fn default() -> Self {
        Self::empty()
    }
}

impl RowMarkStore {
    pub fn open(home: &A2aHome) -> Result<Self, AppError> {
        let root = home.root().join("mesh").join("row-marks");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    root: Some(root),
                    marks: BTreeMap::new(),
                });
            }
            Err(error) => return Err(io_error("reading row marks", &root, error)),
        };
        let mut marks = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|error| io_error("reading row marks", &root, error))?;
            if !entry
                .file_type()
                .map_err(|error| io_error("reading row mark metadata", &entry.path(), error))?
                .is_file()
                || entry.path().extension().and_then(|part| part.to_str()) != Some("json")
            {
                continue;
            }
            let gateway_id = entry
                .path()
                .file_stem()
                .and_then(|part| part.to_str())
                .ok_or_else(|| invalid_mark("row-mark filename is not valid UTF-8"))?
                .to_string();
            validate_gateway_id(&gateway_id)?;
            let bytes = fs::read(entry.path())
                .map_err(|error| io_error("reading row mark", &entry.path(), error))?;
            let mark: RowMark = serde_json::from_slice(&bytes).map_err(|error| {
                invalid_mark(format!(
                    "row mark for {gateway_id} is not valid JSON: {error}"
                ))
            })?;
            marks.insert(gateway_id, mark);
        }
        Ok(Self {
            root: Some(root),
            marks,
        })
    }

    pub(crate) fn empty() -> Self {
        Self {
            root: None,
            marks: BTreeMap::new(),
        }
    }

    pub fn load_all(&self) -> &BTreeMap<String, RowMark> {
        &self.marks
    }

    pub fn get(&self, gateway_id: &str) -> Option<&RowMark> {
        self.marks.get(gateway_id)
    }

    pub fn apply_verified(
        &mut self,
        gateway_id: &str,
        msg: &RowMsg,
        claims: &TokenClaims,
    ) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        if gateway_id != msg.gateway_id || gateway_id != claims.gateway_id {
            return Err(invalid_mark(format!(
                "row mark gateway {gateway_id} does not match the verified row and token"
            )));
        }
        let digest = sha256_hex(canonical_row_msg(msg)?.as_bytes());
        let previous = self.marks.get(gateway_id).cloned();
        let is_first_mark = previous.is_none();
        let mark = self.marks.entry(gateway_id.to_string()).or_default();

        if !is_first_mark && claims.key_epoch < mark.highest_key_epoch {
            return Err(invalid_mark(format!(
                "refusing to lower row-mark key epoch from {} to {}",
                mark.highest_key_epoch, claims.key_epoch
            )));
        }
        if let Some(pinned) = mark.pinned_pubkey_b64.as_deref() {
            if claims.key_epoch == mark.highest_key_epoch && pinned != claims.pubkey_b64 {
                return Err(invalid_mark(
                    "refusing to replace a row-mark public key without a higher key epoch",
                ));
            }
        }

        mark.first_signature = true;
        if is_first_mark || mark.pinned_pubkey_b64.is_none() {
            mark.pinned_pubkey_b64 = Some(claims.pubkey_b64.clone());
            mark.highest_key_epoch = claims.key_epoch;
            mark.high_water = Some(msg.signed_at);
            mark.msg_digest = Some(digest);
        } else if claims.key_epoch > mark.highest_key_epoch {
            mark.pinned_pubkey_b64 = Some(claims.pubkey_b64.clone());
            mark.highest_key_epoch = claims.key_epoch;
            mark.high_water = None;
            mark.msg_digest = None;
        } else {
            mark.high_water = Some(msg.signed_at);
            mark.msg_digest = Some(digest);
        }

        if let Err(error) = self.persist_gateway(gateway_id) {
            match previous {
                Some(mark) => {
                    self.marks.insert(gateway_id.to_string(), mark);
                }
                None => {
                    self.marks.remove(gateway_id);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn forget_signed(&mut self, gateway_id: &str) {
        if let Err(error) = self.try_forget_signed(gateway_id) {
            eprintln!(
                "a2a: warning: failed to persist forget-signed for {gateway_id}: {}: {}",
                error.code, error.message
            );
        }
    }

    pub fn clear_signature(&mut self, gateway_id: &str) {
        if let Err(error) = self.try_clear_signature(gateway_id) {
            eprintln!(
                "a2a: warning: failed to persist clear-signature for {gateway_id}: {}: {}",
                error.code, error.message
            );
        }
    }

    pub fn try_forget_signed(&mut self, gateway_id: &str) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        let Some(mark) = self.marks.get_mut(gateway_id) else {
            return Ok(());
        };
        let previous = mark.clone();
        mark.high_water = None;
        mark.msg_digest = None;
        if let Err(error) = self.persist_gateway(gateway_id) {
            self.marks.insert(gateway_id.to_string(), previous);
            return Err(error);
        }
        Ok(())
    }

    pub fn try_clear_signature(&mut self, gateway_id: &str) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        let previous = self.marks.get(gateway_id).cloned();
        self.marks
            .entry(gateway_id.to_string())
            .or_default()
            .first_signature = false;
        if let Err(error) = self.persist_gateway(gateway_id) {
            match previous {
                Some(mark) => {
                    self.marks.insert(gateway_id.to_string(), mark);
                }
                None => {
                    self.marks.remove(gateway_id);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn persist_gateway(&self, gateway_id: &str) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        let root = self.root.as_deref().ok_or_else(|| {
            invalid_mark("this row-mark store is read-only and has no on-disk home")
        })?;
        create_dir_0700(root)
            .map_err(|error| io_error("creating row-mark directory", root, error))?;
        let mark = self
            .marks
            .get(gateway_id)
            .ok_or_else(|| invalid_mark("row mark disappeared before it could be written"))?;
        let bytes = serde_json::to_vec_pretty(mark)
            .map_err(|error| invalid_mark(format!("serializing row mark: {error}")))?;
        let path = root.join(format!("{gateway_id}.json"));
        crate::store::write_atomic(&path, &bytes)
            .map_err(|error| io_error("writing row mark", &path, error))
    }
}

/// Reader-local cache of root-verified revocation tombstones.
///
/// `open` is read-only until `observe` is called. Gateway daemon registry reads call `observe`;
/// CLI and approval paths only consult `is_revoked` so a cold read never creates cache state.
#[derive(Clone, Debug)]
pub struct RevocationCache {
    root: Option<PathBuf>,
    records: BTreeMap<String, RevocationClaims>,
}

impl Default for RevocationCache {
    fn default() -> Self {
        Self::empty()
    }
}

impl RevocationCache {
    pub fn open(home: &A2aHome) -> Result<Self, AppError> {
        let root = home.root().join("mesh").join("revocations");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    root: Some(root),
                    records: BTreeMap::new(),
                });
            }
            Err(error) => return Err(io_error("reading revocation cache", &root, error)),
        };
        let mut records = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|error| io_error("reading revocation cache", &root, error))?;
            if !entry
                .file_type()
                .map_err(|error| {
                    io_error("reading revocation cache metadata", &entry.path(), error)
                })?
                .is_file()
                || entry.path().extension().and_then(|part| part.to_str()) != Some("json")
            {
                continue;
            }
            let gateway_id = entry
                .path()
                .file_stem()
                .and_then(|part| part.to_str())
                .ok_or_else(|| invalid_revocation("revocation filename is not valid UTF-8"))?
                .to_string();
            validate_gateway_id(&gateway_id)?;
            let bytes = fs::read(entry.path())
                .map_err(|error| io_error("reading revocation cache", &entry.path(), error))?;
            let claims: RevocationClaims = serde_json::from_slice(&bytes).map_err(|error| {
                invalid_revocation(format!(
                    "revocation cache for {gateway_id} is not valid JSON: {error}"
                ))
            })?;
            if claims.gateway_id != gateway_id {
                return Err(invalid_revocation(format!(
                    "revocation cache filename {gateway_id} does not match record {}",
                    claims.gateway_id
                )));
            }
            records.insert(gateway_id, claims);
        }
        Ok(Self {
            root: Some(root),
            records,
        })
    }

    pub(crate) fn empty() -> Self {
        Self {
            root: None,
            records: BTreeMap::new(),
        }
    }

    pub fn observe(
        &mut self,
        gateway_id: &str,
        claims: &RevocationClaims,
    ) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        if claims.gateway_id != gateway_id {
            return Err(invalid_revocation(format!(
                "refusing to cache revocation for {} under {gateway_id}",
                claims.gateway_id
            )));
        }
        let previous = self.records.get(gateway_id).cloned();
        if previous
            .as_ref()
            .is_some_and(|record| record.key_epoch > claims.key_epoch)
        {
            return Ok(());
        }
        self.records
            .insert(gateway_id.to_string(), claims.clone());
        if let Err(error) = self.persist_gateway(gateway_id) {
            match previous {
                Some(record) => {
                    self.records.insert(gateway_id.to_string(), record);
                }
                None => {
                    self.records.remove(gateway_id);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn is_revoked(&self, gateway_id: &str, token_epoch: u64) -> bool {
        self.records
            .get(gateway_id)
            .is_some_and(|record| token_epoch <= record.key_epoch)
    }

    pub(crate) fn supersede(
        &mut self,
        gateway_id: &str,
        token_epoch: u64,
    ) -> Result<bool, AppError> {
        validate_gateway_id(gateway_id)?;
        let Some(record) = self.records.get(gateway_id) else {
            return Ok(false);
        };
        if token_epoch <= record.key_epoch {
            return Ok(false);
        }
        let previous = record.clone();
        self.records.remove(gateway_id);
        let Some(root) = self.root.as_deref() else {
            return Ok(true);
        };
        let path = root.join(format!("{gateway_id}.json"));
        if let Err(error) = fs::remove_file(&path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                self.records.insert(gateway_id.to_string(), previous);
                return Err(io_error("clearing superseded revocation", &path, error));
            }
        }
        Ok(true)
    }

    fn persist_gateway(&self, gateway_id: &str) -> Result<(), AppError> {
        validate_gateway_id(gateway_id)?;
        let root = self.root.as_deref().ok_or_else(|| {
            invalid_revocation("this revocation cache is read-only and has no on-disk home")
        })?;
        create_dir_0700(root)
            .map_err(|error| io_error("creating revocation cache directory", root, error))?;
        let record = self.records.get(gateway_id).ok_or_else(|| {
            invalid_revocation("revocation disappeared before it could be written")
        })?;
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|error| invalid_revocation(format!("serializing revocation: {error}")))?;
        let path = root.join(format!("{gateway_id}.json"));
        crate::store::write_atomic(&path, &bytes)
            .map_err(|error| io_error("writing revocation cache", &path, error))
    }
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
        Err(invalid_mark(
            "gateway_id must match ^[A-Za-z0-9_-]{1,64}$ before row-mark storage",
        ))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn invalid_mark(message: impl Into<String>) -> AppError {
    AppError::new("DATA", ExitCode::Precondition, message)
}

fn invalid_revocation(message: impl Into<String>) -> AppError {
    AppError::new("REVOCATION_INVALID", ExitCode::Precondition, message)
        .remedy("discard the local cache entry and refresh it from a root-verified registry row")
}

fn io_error(context: &str, path: &Path, error: std::io::Error) -> AppError {
    AppError::new(
        "IO",
        ExitCode::Internal,
        format!("{context} {}: {error}", path.display()),
    )
}
