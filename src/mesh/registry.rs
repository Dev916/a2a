use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::keys::RootSet;
use crate::mesh::row_mark::{RevocationCache, RowMarkStore};
use crate::mesh::row_sig::{
    revocation_state, verify_row, RevocationState, RowVerdict, RowVerifyCfg, StoredRowColumns,
};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{Acquire, PgPool, Row};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

pub const REGISTRY_SCHEMA_VERSION: i32 = 1;
pub const DEFAULT_REGISTRY_HEARTBEAT_SECS: u64 = 180;
pub const DEFAULT_REGISTRY_TIMEOUT_SECS: u64 = 10;

const FILE_REGISTRY_ROWS: &str = "mesh_gateways.json";
const FILE_WORK_ROWS: &str = "mesh_work_items.json";
const FILE_WORK_LOCK: &str = "mesh_work_items.lock";
const SET_LOCAL_STATEMENT_TIMEOUT: &str = concat!("SET", " LOCAL statement_timeout = ");
pub const UPSERT_GATEWAY_SQL: &str = concat!(
    "INSERT INTO mesh_gateways ",
    "(fleet_id, gateway_id, owner, endpoint_url, pubkey_fpr, cert_fpr, version, ",
    "created_at, last_heartbeat, revoked_at, schema_version, row_msg, row_token, ",
    "row_sig, signed_at, revocation) ",
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ",
    "ON CONFLICT (fleet_id, gateway_id) DO UPDATE SET ",
    "owner = EXCLUDED.owner, endpoint_url = EXCLUDED.endpoint_url, ",
    "pubkey_fpr = EXCLUDED.pubkey_fpr, cert_fpr = EXCLUDED.cert_fpr, ",
    "version = EXCLUDED.version, last_heartbeat = EXCLUDED.last_heartbeat, ",
    "schema_version = EXCLUDED.schema_version, row_msg = EXCLUDED.row_msg, ",
    "row_token = EXCLUDED.row_token, row_sig = EXCLUDED.row_sig, ",
    "signed_at = EXCLUDED.signed_at"
);
pub const HEARTBEAT_GATEWAY_SQL: &str = concat!(
    "UPDATE mesh_gateways SET last_heartbeat = $3 ",
    "WHERE fleet_id = $1 AND gateway_id = $2"
);
const WORK_SELECT_BY_ID_SQL: &str = concat!(
    "SELECT ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version ",
    "FROM mesh_work_items WHERE fleet_id = $1 AND work_id = $2"
);
const WORK_CLAIM_SQL: &str = concat!(
    "UPDATE mesh_work_items SET status = 'approved', claimed_by = $3, updated_at = now() ",
    "WHERE fleet_id = $1 AND work_id = $2 AND status = 'offered' ",
    "AND (addressing = 'pool' OR addressing = $4) RETURNING ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version"
);
const WORK_TRANSITION_SQL: &str = concat!(
    "UPDATE mesh_work_items SET status = $4, ",
    "result_ref = CASE WHEN $5 THEN $6 ELSE result_ref END, ",
    "result_delivery = CASE WHEN $7 THEN $8 ELSE result_delivery END, ",
    "swept_by = CASE WHEN $9 THEN $10 ELSE swept_by END, ",
    "updated_at = COALESCE($11, now()) ",
    "WHERE fleet_id = $1 AND work_id = $2 AND status = $3 ",
    "AND ($12 = 'any' OR ($12 = 'sender' AND sender_gw = $13) ",
    "OR ($12 = 'claimer' AND claimed_by = $13)) RETURNING ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version"
);
const WORK_LIST_SQL: &str = concat!(
    "SELECT ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version ",
    "FROM mesh_work_items WHERE fleet_id = $1 ",
    "AND ($2::text IS NULL OR status = $2) ",
    "AND ($3::text IS NULL OR claimed_by = $3) ",
    "ORDER BY created_at, work_id"
);
// `awaiting_release` is deliberately excluded: reviewer holds have their own long horizon.
const WORK_SWEEP_ABANDONED_SQL: &str = concat!(
    "UPDATE mesh_work_items SET status = 'failed', swept_by = $2, updated_at = now() ",
    "WHERE fleet_id = $1 AND claimed_by = $2 AND status IN ('approved', 'running') ",
    "AND updated_at < now() - ($3::bigint * interval '1 second') RETURNING ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version"
);
const WORK_SWEEP_RELEASE_STALLED_SQL: &str = concat!(
    "UPDATE mesh_work_items SET status = 'failed', swept_by = $2, ",
    "result_delivery = 'release_stalled', updated_at = now() ",
    "WHERE fleet_id = $1 AND claimed_by = $2 AND status = 'awaiting_release' ",
    "AND updated_at < now() - ($3::bigint * interval '1 second') RETURNING ",
    "fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, result_delivery, ",
    "swept_by, created_at, updated_at, expires_at, schema_version"
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayRow {
    pub fleet_id: String,
    pub gateway_id: String,
    pub owner: String,
    pub endpoint_url: Option<String>,
    pub pubkey_fpr: String,
    pub cert_fpr: Option<String>,
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub schema_version: i32,
    #[serde(default)]
    pub row_msg: Option<String>,
    #[serde(default)]
    pub row_token: Option<String>,
    #[serde(default)]
    pub row_sig: Option<String>,
    #[serde(default)]
    pub signed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub revocation: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkStatus {
    Offered,
    Approved,
    Running,
    AwaitingRelease,
    Done,
    Failed,
    Rejected,
    Acked,
    Declined,
    Cancelled,
    Expired,
}

impl WorkStatus {
    fn as_sql(self) -> &'static str {
        match self {
            WorkStatus::Offered => "offered",
            WorkStatus::Approved => "approved",
            WorkStatus::Running => "running",
            WorkStatus::AwaitingRelease => "awaiting_release",
            WorkStatus::Done => "done",
            WorkStatus::Failed => "failed",
            WorkStatus::Rejected => "rejected",
            WorkStatus::Acked => "acked",
            WorkStatus::Declined => "declined",
            WorkStatus::Cancelled => "cancelled",
            WorkStatus::Expired => "expired",
        }
    }

    fn from_sql(value: &str) -> Result<Self, AppError> {
        match value {
            "offered" => Ok(WorkStatus::Offered),
            "approved" => Ok(WorkStatus::Approved),
            "running" => Ok(WorkStatus::Running),
            "awaiting_release" => Ok(WorkStatus::AwaitingRelease),
            "done" => Ok(WorkStatus::Done),
            "failed" => Ok(WorkStatus::Failed),
            "rejected" => Ok(WorkStatus::Rejected),
            "acked" => Ok(WorkStatus::Acked),
            "declined" => Ok(WorkStatus::Declined),
            "cancelled" => Ok(WorkStatus::Cancelled),
            "expired" => Ok(WorkStatus::Expired),
            _ => Err(AppError::new(
                "WORK_STATUS_INVALID",
                ExitCode::Internal,
                format!("registry returned unknown work status {value:?}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRow {
    pub fleet_id: String,
    pub work_id: String,
    pub sender_gw: String,
    pub addressing: String,
    pub status: WorkStatus,
    pub claimed_by: Option<String>,
    pub result_ref: Option<String>,
    pub result_delivery: Option<String>,
    pub swept_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub schema_version: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ClaimOutcome {
    Won,
    Lost(WorkRow),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum TransitionOutcome {
    Applied,
    Lost(WorkRow),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkOwner {
    Sender(String),
    Claimer(String),
    Any,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkPatch {
    pub result_ref: Option<Option<String>>,
    pub result_delivery: Option<Option<String>>,
    pub swept_by: Option<Option<String>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkListFilter {
    pub status: Option<WorkStatus>,
    pub claimed_by: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RegistryStaleness {
    Active,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryPlan {
    pub ssl_verify_full: bool,
    pub statement_cache_capacity: Option<usize>,
    pub max_connections: u32,
    pub min_connections: u32,
    pub idle_timeout_secs: u64,
}

#[allow(async_fn_in_trait)]
pub trait Registry {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError>;
    async fn heartbeat(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError>;
    /// Raw gateway-row read. Public consumers use [`RegistryHandle::discover`], which applies the
    /// row verifier; verification helpers in this module are the only callers of this method.
    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError>;
    /// Fleet ids visible to this credential. Adapters that cannot enumerate
    /// them retain the empty default; concrete built-in adapters override it.
    async fn fleet_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(Vec::new())
    }
    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        revocation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError>;
    async fn clear_legacy_revoked_at(
        &self,
        fleet_id: &str,
        gateway_id: &str,
    ) -> Result<(), AppError> {
        let _ = (fleet_id, gateway_id);
        Err(AppError::new(
            "INTERNAL",
            ExitCode::Internal,
            "this registry adapter cannot clear the legacy revocation mirror",
        ))
    }
    async fn schema_version(&self) -> Result<i32, AppError>;
    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError>;
    async fn work_claim(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer_gw: &str,
    ) -> Result<ClaimOutcome, AppError>;
    async fn work_transition(
        &self,
        fleet_id: &str,
        work_id: &str,
        from: WorkStatus,
        to: WorkStatus,
        owner: WorkOwner,
        patch: WorkPatch,
    ) -> Result<TransitionOutcome, AppError>;
    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError>;
    async fn work_list(
        &self,
        fleet_id: &str,
        filter: WorkListFilter,
    ) -> Result<Vec<WorkRow>, AppError>;
    async fn work_touch_running(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer: &str,
    ) -> Result<(), AppError>;
    async fn work_sweep_expired(
        &self,
        fleet_id: &str,
        swept_by: &str,
    ) -> Result<Vec<String>, AppError>;
    async fn work_sweep_abandoned(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        stall: Duration,
    ) -> Result<Vec<WorkRow>, AppError>;
    async fn work_sweep_release_stalled(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        horizon: Duration,
    ) -> Result<Vec<WorkRow>, AppError>;
}

#[derive(Clone, Debug)]
pub struct FileRegistry {
    root: PathBuf,
}

impl FileRegistry {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn init_schema(&self) -> Result<(), AppError> {
        self.ensure_root()?;
        if !self.rows_path().exists() {
            self.write_rows(&[])?;
        }
        if !self.work_rows_path().exists() {
            self.write_work_rows_locked(&[])?;
        }
        Ok(())
    }

    pub fn apply_migration(&self, sql: &str) -> Result<(), AppError> {
        self.ensure_root()?;
        if sql.contains("mesh_work_items") {
            if !self.work_rows_path().exists() {
                self.write_work_rows_locked(&[])?;
            }
        } else if sql.contains("mesh_gateways") && !self.rows_path().exists() {
            self.write_rows(&[])?;
        }
        Ok(())
    }

    fn rows_path(&self) -> PathBuf {
        self.root.join(FILE_REGISTRY_ROWS)
    }

    fn work_rows_path(&self) -> PathBuf {
        self.root.join(FILE_WORK_ROWS)
    }

    fn work_lock_path(&self) -> PathBuf {
        self.root.join(FILE_WORK_LOCK)
    }

    fn ensure_root(&self) -> Result<(), AppError> {
        crate::home::create_dir_0700(&self.root).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("creating file registry: {e}"),
            )
        })
    }

    fn read_rows(&self) -> Result<Vec<GatewayRow>, AppError> {
        self.ensure_root()?;
        let path = self.rows_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&path).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading file registry: {e}"),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|e| {
            AppError::new(
                "REGISTRY_FILE_INVALID",
                ExitCode::Precondition,
                format!("file registry is invalid: {e}"),
            )
        })
    }

    fn write_rows(&self, rows: &[GatewayRow]) -> Result<(), AppError> {
        self.ensure_root()?;
        let bytes = serde_json::to_vec_pretty(rows)
            .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
        crate::store::write_atomic(&self.rows_path(), &bytes).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("writing file registry: {e}"),
            )
        })
    }

    fn lock_work_rows(&self) -> Result<Flock<File>, AppError> {
        self.ensure_root()?;
        let path = self.work_lock_path();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|e| {
                AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("opening file registry work lock: {e}"),
                )
            })?;
        Flock::lock(file, FlockArg::LockExclusive).map_err(|(_file, errno)| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("locking file registry work rows: {errno}"),
            )
        })
    }

    fn read_work_rows_unlocked(&self) -> Result<Vec<WorkRow>, AppError> {
        let path = self.work_rows_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&path).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading file registry work rows: {e}"),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|e| {
            AppError::new(
                "REGISTRY_FILE_INVALID",
                ExitCode::Precondition,
                format!("file registry work rows are invalid: {e}"),
            )
        })
    }

    fn write_work_rows_unlocked(&self, rows: &[WorkRow]) -> Result<(), AppError> {
        let bytes = serde_json::to_vec_pretty(rows)
            .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
        crate::store::write_atomic(&self.work_rows_path(), &bytes).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("writing file registry work rows: {e}"),
            )
        })
    }

    fn write_work_rows_locked(&self, rows: &[WorkRow]) -> Result<(), AppError> {
        let _lock = self.lock_work_rows()?;
        self.write_work_rows_unlocked(rows)
    }

    fn with_work_rows<T>(
        &self,
        f: impl FnOnce(&mut Vec<WorkRow>) -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        let _lock = self.lock_work_rows()?;
        let mut rows = self.read_work_rows_unlocked()?;
        let out = f(&mut rows)?;
        rows.sort_by(|a, b| {
            a.fleet_id
                .cmp(&b.fleet_id)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.work_id.cmp(&b.work_id))
        });
        self.write_work_rows_unlocked(&rows)?;
        Ok(out)
    }
}

impl Registry for FileRegistry {
    async fn upsert_self(&self, mut row: GatewayRow) -> Result<(), AppError> {
        let mut rows = self.read_rows()?;
        if let Some(existing) = rows
            .iter_mut()
            .find(|existing| {
                existing.fleet_id == row.fleet_id && existing.gateway_id == row.gateway_id
            })
        {
            row.created_at = existing.created_at;
            row.revoked_at = existing.revoked_at;
            row.revocation.clone_from(&existing.revocation);
            *existing = row;
        } else {
            rows.push(row);
        }
        rows.sort_by(|a, b| a.gateway_id.cmp(&b.gateway_id));
        self.write_rows(&rows)
    }

    async fn heartbeat(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let mut rows = self.read_rows()?;
        if let Some(row) = rows
            .iter_mut()
            .find(|r| r.fleet_id == fleet_id && r.gateway_id == gateway_id)
        {
            row.last_heartbeat = now;
        }
        self.write_rows(&rows)
    }

    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        Ok(self
            .read_rows()?
            .into_iter()
            .filter(|r| r.fleet_id == fleet_id)
            .collect())
    }

    async fn fleet_ids(&self) -> Result<Vec<String>, AppError> {
        let mut fleet_ids = self
            .read_rows()?
            .into_iter()
            .map(|row| row.fleet_id)
            .collect::<Vec<_>>();
        fleet_ids.sort();
        fleet_ids.dedup();
        Ok(fleet_ids)
    }

    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        revocation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let mut rows = self.read_rows()?;
        if let Some(row) = rows
            .iter_mut()
            .find(|r| r.fleet_id == fleet_id && r.gateway_id == gateway_id)
        {
            row.revocation = Some(revocation.to_string());
            row.revoked_at = Some(now);
        }
        self.write_rows(&rows)
    }

    async fn clear_legacy_revoked_at(
        &self,
        fleet_id: &str,
        gateway_id: &str,
    ) -> Result<(), AppError> {
        let mut rows = self.read_rows()?;
        if let Some(row) = rows
            .iter_mut()
            .find(|r| r.fleet_id == fleet_id && r.gateway_id == gateway_id)
        {
            row.revoked_at = None;
        }
        self.write_rows(&rows)
    }

    async fn schema_version(&self) -> Result<i32, AppError> {
        Ok(REGISTRY_SCHEMA_VERSION)
    }

    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        self.with_work_rows(|rows| {
            rows.retain(|r| !(r.fleet_id == row.fleet_id && r.work_id == row.work_id));
            rows.push(row);
            Ok(())
        })
    }

    async fn work_claim(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer_gw: &str,
    ) -> Result<ClaimOutcome, AppError> {
        self.with_work_rows(|rows| {
            let row = work_row_mut(rows, fleet_id, work_id)?;
            let directed_to_me = format!("directed:{claimer_gw}");
            if row.status == WorkStatus::Offered
                && (row.addressing == "pool" || row.addressing == directed_to_me)
            {
                row.status = WorkStatus::Approved;
                row.claimed_by = Some(claimer_gw.to_string());
                row.updated_at = Utc::now();
                Ok(ClaimOutcome::Won)
            } else {
                Ok(ClaimOutcome::Lost(row.clone()))
            }
        })
    }

    async fn work_transition(
        &self,
        fleet_id: &str,
        work_id: &str,
        from: WorkStatus,
        to: WorkStatus,
        owner: WorkOwner,
        patch: WorkPatch,
    ) -> Result<TransitionOutcome, AppError> {
        self.with_work_rows(|rows| {
            let row = work_row_mut(rows, fleet_id, work_id)?;
            if row.status == from && owner_matches(row, &owner) {
                row.status = to;
                apply_work_patch(row, patch);
                Ok(TransitionOutcome::Applied)
            } else {
                Ok(TransitionOutcome::Lost(row.clone()))
            }
        })
    }

    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError> {
        let _lock = self.lock_work_rows()?;
        Ok(self
            .read_work_rows_unlocked()?
            .into_iter()
            .find(|r| r.fleet_id == fleet_id && r.work_id == work_id))
    }

    async fn work_list(
        &self,
        fleet_id: &str,
        filter: WorkListFilter,
    ) -> Result<Vec<WorkRow>, AppError> {
        let _lock = self.lock_work_rows()?;
        let mut rows = self
            .read_work_rows_unlocked()?
            .into_iter()
            .filter(|r| r.fleet_id == fleet_id)
            .filter(|r| filter.status.is_none_or(|status| r.status == status))
            .filter(|r| {
                filter
                    .claimed_by
                    .as_deref()
                    .is_none_or(|claimer| r.claimed_by.as_deref() == Some(claimer))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.work_id.cmp(&b.work_id)));
        Ok(rows)
    }

    async fn work_touch_running(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer: &str,
    ) -> Result<(), AppError> {
        self.with_work_rows(|rows| {
            let row = work_row_mut(rows, fleet_id, work_id)?;
            if row.status == WorkStatus::Running && row.claimed_by.as_deref() == Some(claimer) {
                row.updated_at = Utc::now();
            }
            Ok(())
        })
    }

    async fn work_sweep_expired(
        &self,
        fleet_id: &str,
        swept_by: &str,
    ) -> Result<Vec<String>, AppError> {
        self.with_work_rows(|rows| {
            let now = Utc::now();
            let mut swept = Vec::new();
            for row in rows {
                if row.fleet_id == fleet_id
                    && row.status == WorkStatus::Offered
                    && row.expires_at < now
                {
                    row.status = WorkStatus::Expired;
                    row.swept_by = Some(swept_by.to_string());
                    row.updated_at = now;
                    swept.push(row.work_id.clone());
                }
            }
            swept.sort();
            Ok(swept)
        })
    }

    async fn work_sweep_abandoned(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        stall: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        let stall = chrono::Duration::from_std(stall).map_err(|e| {
            AppError::new(
                "DURATION_INVALID",
                ExitCode::Precondition,
                format!("claim stall duration is invalid: {e}"),
            )
        })?;
        self.with_work_rows(|rows| {
            let now = Utc::now();
            let cutoff = now - stall;
            let mut swept = Vec::new();
            for row in rows {
                if row.fleet_id == fleet_id
                    && matches!(row.status, WorkStatus::Approved | WorkStatus::Running)
                    && row.claimed_by.as_deref() == Some(claimer_gw)
                    && row.updated_at < cutoff
                {
                    row.status = WorkStatus::Failed;
                    row.swept_by = Some(claimer_gw.to_string());
                    row.updated_at = now;
                    swept.push(row.clone());
                }
            }
            swept.sort_by(|a, b| a.work_id.cmp(&b.work_id));
            Ok(swept)
        })
    }

    async fn work_sweep_release_stalled(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        horizon: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        let horizon = chrono::Duration::from_std(horizon).map_err(|e| {
            AppError::new(
                "DURATION_INVALID",
                ExitCode::Precondition,
                format!("release stall duration is invalid: {e}"),
            )
        })?;
        self.with_work_rows(|rows| {
            let now = Utc::now();
            let cutoff = now - horizon;
            let mut swept = Vec::new();
            for row in rows {
                if row.fleet_id == fleet_id
                    && row.status == WorkStatus::AwaitingRelease
                    && row.claimed_by.as_deref() == Some(claimer_gw)
                    && row.updated_at < cutoff
                {
                    row.status = WorkStatus::Failed;
                    row.swept_by = Some(claimer_gw.to_string());
                    row.result_delivery = Some("release_stalled".to_string());
                    row.updated_at = now;
                    swept.push(row.clone());
                }
            }
            swept.sort_by(|a, b| a.work_id.cmp(&b.work_id));
            Ok(swept)
        })
    }
}

fn work_row_mut<'a>(
    rows: &'a mut [WorkRow],
    fleet_id: &str,
    work_id: &str,
) -> Result<&'a mut WorkRow, AppError> {
    rows.iter_mut()
        .find(|r| r.fleet_id == fleet_id && r.work_id == work_id)
        .ok_or_else(|| work_not_found(fleet_id, work_id))
}

fn owner_matches(row: &WorkRow, owner: &WorkOwner) -> bool {
    match owner {
        WorkOwner::Sender(gw) => row.sender_gw == *gw,
        WorkOwner::Claimer(gw) => row.claimed_by.as_deref() == Some(gw.as_str()),
        WorkOwner::Any => true,
    }
}

fn apply_work_patch(row: &mut WorkRow, patch: WorkPatch) {
    if let Some(result_ref) = patch.result_ref {
        row.result_ref = result_ref;
    }
    if let Some(result_delivery) = patch.result_delivery {
        row.result_delivery = result_delivery;
    }
    if let Some(swept_by) = patch.swept_by {
        row.swept_by = swept_by;
    }
    row.updated_at = patch.updated_at.unwrap_or_else(Utc::now);
}

fn work_not_found(fleet_id: &str, work_id: &str) -> AppError {
    AppError::new(
        "WORK_NOT_FOUND",
        ExitCode::Precondition,
        format!("work item {fleet_id}/{work_id} was not found"),
    )
    .remedy("check `a2a mesh work list` or ask the sender to re-send the work")
}

pub struct PgRegistry {
    pool: PgPool,
    timeout: Duration,
    first_acquire: AtomicBool,
}

pub(crate) enum RegistryBackend {
    File(FileRegistry),
    Pg(PgRegistry),
}

pub struct RegistryHandle {
    backend: RegistryBackend,
    roots: RootSet,
    fleet_id: String,
    marks: RowMarkStore,
    verify_cfg: RowVerifyCfg,
    revocations: Option<Mutex<RevocationCache>>,
}

impl RegistryHandle {
    pub async fn apply_migration(&self, sql: &str) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.apply_migration(sql),
            RegistryBackend::Pg(registry) => registry.apply_migration(sql).await,
        }
    }

    pub fn with_reader_state(mut self, marks: RowMarkStore, cfg: RowVerifyCfg) -> Self {
        self.marks = marks;
        self.verify_cfg = cfg;
        self
    }

    pub fn with_revocation_cache(mut self, cache: RevocationCache) -> Self {
        self.revocations = Some(Mutex::new(cache));
        self
    }

    pub async fn discover(&self) -> Result<Vec<(GatewayRow, RowVerdict)>, AppError> {
        let rows = discover_verified(
            self,
            &self.fleet_id,
            &self.roots,
            &self.marks,
            &self.verify_cfg,
        )
        .await?;
        if let Some(cache) = &self.revocations {
            let mut cache = cache.lock().map_err(|_| {
                AppError::new(
                    "REVOCATION_INVALID",
                    ExitCode::Internal,
                    "revocation cache lock is poisoned",
                )
                .remedy("restart the gateway and inspect the local mesh/revocations cache")
            })?;
            for (row, _verdict) in &rows {
                observe_row_revocation(&mut cache, row, &self.roots)?;
            }
        }
        Ok(rows)
    }

    pub async fn fleet_ids(&self) -> Result<Vec<String>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.fleet_ids().await,
            RegistryBackend::Pg(registry) => registry.fleet_ids().await,
        }
    }

    pub fn is_file_backend(&self) -> bool {
        matches!(self.backend, RegistryBackend::File(_))
    }
}

pub(crate) async fn discover_verified<R: Registry + ?Sized>(
    registry: &R,
    fleet_id: &str,
    roots: &RootSet,
    marks: &RowMarkStore,
    verify_cfg: &RowVerifyCfg,
) -> Result<Vec<(GatewayRow, RowVerdict)>, AppError> {
    let rows = registry.discover_unverified(fleet_id).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let stored = StoredRowColumns::from(&row);
            let verdict = verify_row(&stored, roots, marks.get(&row.gateway_id), verify_cfg);
            (row, verdict)
        })
        .collect())
}

impl Registry for RegistryHandle {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.upsert_self(row).await,
            RegistryBackend::Pg(registry) => registry.upsert_self(row).await,
        }
    }

    async fn heartbeat(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.heartbeat(fleet_id, gateway_id, now).await,
            RegistryBackend::Pg(registry) => registry.heartbeat(fleet_id, gateway_id, now).await,
        }
    }

    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.discover_unverified(fleet_id).await,
            RegistryBackend::Pg(registry) => registry.discover_unverified(fleet_id).await,
        }
    }

    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        revocation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry
                    .revoke(fleet_id, gateway_id, revocation, now)
                    .await
            }
            RegistryBackend::Pg(registry) => {
                registry
                    .revoke(fleet_id, gateway_id, revocation, now)
                    .await
            }
        }
    }

    async fn clear_legacy_revoked_at(
        &self,
        fleet_id: &str,
        gateway_id: &str,
    ) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry
                    .clear_legacy_revoked_at(fleet_id, gateway_id)
                    .await
            }
            RegistryBackend::Pg(registry) => {
                registry
                    .clear_legacy_revoked_at(fleet_id, gateway_id)
                    .await
            }
        }
    }

    async fn schema_version(&self) -> Result<i32, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.schema_version().await,
            RegistryBackend::Pg(registry) => registry.schema_version().await,
        }
    }

    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.work_insert(row).await,
            RegistryBackend::Pg(registry) => registry.work_insert(row).await,
        }
    }

    async fn work_claim(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer_gw: &str,
    ) -> Result<ClaimOutcome, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.work_claim(fleet_id, work_id, claimer_gw).await,
            RegistryBackend::Pg(registry) => registry.work_claim(fleet_id, work_id, claimer_gw).await,
        }
    }

    async fn work_transition(
        &self,
        fleet_id: &str,
        work_id: &str,
        from: WorkStatus,
        to: WorkStatus,
        owner: WorkOwner,
        patch: WorkPatch,
    ) -> Result<TransitionOutcome, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry
                    .work_transition(fleet_id, work_id, from, to, owner, patch)
                    .await
            }
            RegistryBackend::Pg(registry) => {
                registry
                    .work_transition(fleet_id, work_id, from, to, owner, patch)
                    .await
            }
        }
    }

    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.work_get(fleet_id, work_id).await,
            RegistryBackend::Pg(registry) => registry.work_get(fleet_id, work_id).await,
        }
    }

    async fn work_list(
        &self,
        fleet_id: &str,
        filter: WorkListFilter,
    ) -> Result<Vec<WorkRow>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.work_list(fleet_id, filter).await,
            RegistryBackend::Pg(registry) => registry.work_list(fleet_id, filter).await,
        }
    }

    async fn work_touch_running(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer: &str,
    ) -> Result<(), AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry.work_touch_running(fleet_id, work_id, claimer).await
            }
            RegistryBackend::Pg(registry) => {
                registry.work_touch_running(fleet_id, work_id, claimer).await
            }
        }
    }

    async fn work_sweep_expired(
        &self,
        fleet_id: &str,
        swept_by: &str,
    ) -> Result<Vec<String>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => registry.work_sweep_expired(fleet_id, swept_by).await,
            RegistryBackend::Pg(registry) => registry.work_sweep_expired(fleet_id, swept_by).await,
        }
    }

    async fn work_sweep_abandoned(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        stall: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry
                    .work_sweep_abandoned(fleet_id, claimer_gw, stall)
                    .await
            }
            RegistryBackend::Pg(registry) => {
                registry
                    .work_sweep_abandoned(fleet_id, claimer_gw, stall)
                    .await
            }
        }
    }

    async fn work_sweep_release_stalled(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        horizon: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        match &self.backend {
            RegistryBackend::File(registry) => {
                registry
                    .work_sweep_release_stalled(fleet_id, claimer_gw, horizon)
                    .await
            }
            RegistryBackend::Pg(registry) => {
                registry
                    .work_sweep_release_stalled(fleet_id, claimer_gw, horizon)
                    .await
            }
        }
    }
}

pub fn open_registry(
    url: &str,
    roots: RootSet,
    fleet_id: String,
) -> Result<RegistryHandle, AppError> {
    let backend = if let Some(path) = url.strip_prefix("file://") {
        RegistryBackend::File(FileRegistry::new(Path::new(path)))
    } else if let Some(path) = url.strip_prefix("file:") {
        RegistryBackend::File(FileRegistry::new(Path::new(path)))
    } else {
        RegistryBackend::Pg(PgRegistry::connect_lazy(url, DEFAULT_REGISTRY_TIMEOUT_SECS)?)
    };
    Ok(RegistryHandle {
        backend,
        roots,
        fleet_id,
        marks: RowMarkStore::empty(),
        verify_cfg: RowVerifyCfg::new(Utc::now()),
        revocations: None,
    })
}

fn observe_row_revocation(
    cache: &mut RevocationCache,
    row: &GatewayRow,
    roots: &RootSet,
) -> Result<(), AppError> {
    let stored = StoredRowColumns::from(row);
    match revocation_state(&stored, roots) {
        RevocationState::Revoked(record) => cache.observe(&row.gateway_id, &record),
        RevocationState::Superseded { token_epoch, .. } => {
            cache.supersede(&row.gateway_id, token_epoch).map(|_| ())
        }
        RevocationState::Absent => {
            if let Some(token_epoch) = verified_row_token_epoch(&stored, roots) {
                cache.supersede(&row.gateway_id, token_epoch)?;
            }
            Ok(())
        }
        RevocationState::Invalid | RevocationState::Misplaced(_) => Ok(()),
    }
}

fn verified_row_token_epoch(stored: &StoredRowColumns, roots: &RootSet) -> Option<u64> {
    let (claims, _issuer) =
        crate::mesh::keys::verify_token_no_clock(stored.row_token.as_deref()?, roots).ok()?;
    (claims.fleet_id == stored.fleet_id && claims.gateway_id == stored.gateway_id)
        .then_some(claims.key_epoch)
}

pub fn reader_roots(home: &A2aHome) -> Result<RootSet, AppError> {
    RootSet::load(home).map_err(|error| {
        let fleet_dir = home.root().join("mesh").join("fleet");
        AppError::new(
            "ROOTS_UNREADABLE",
            ExitCode::Precondition,
            format!(
                "local root files {}/root.pub and {}/root.pub.next are unreadable: {}: {}",
                fleet_dir.display(),
                fleet_dir.display(),
                error.code,
                error.message
            ),
        )
        .remedy("repair the LOCAL root.pub/root.pub.next files and permissions, then retry")
    })
}

pub fn registry_env_path(home: &A2aHome) -> PathBuf {
    home.root().join("mesh").join("fleet").join("registry.env")
}

pub fn write_registry_env(home: &A2aHome, url: &str) -> Result<(), AppError> {
    let path = registry_env_path(home);
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| {
            AppError::new("IO", ExitCode::Internal, format!("creating fleet dir: {e}"))
        })?;
    }
    crate::store::write_atomic(&path, format!("{url}\n").as_bytes()).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("writing registry env: {e}"),
        )
    })
}

pub fn read_registry_env(home: &A2aHome) -> Result<String, AppError> {
    let path = registry_env_path(home);
    let text = fs::read_to_string(&path).map_err(|e| {
        AppError::new(
            "REGISTRY_URL_REQUIRED",
            ExitCode::Precondition,
            format!("reading registry env: {e}"),
        )
        .remedy("run `a2a mesh fleet init --registry -`")
    })?;
    let url = text.trim();
    if url.is_empty() {
        return Err(AppError::new(
            "REGISTRY_URL_REQUIRED",
            ExitCode::Precondition,
            "registry env is empty",
        )
        .remedy("run `a2a mesh fleet init --registry -`"));
    }
    Ok(url.to_string())
}

impl PgRegistry {
    pub fn connect_lazy(url: &str, timeout_secs: u64) -> Result<Self, AppError> {
        let plan = registry_plan(url)?;
        let mut options = PgConnectOptions::from_str(url).map_err(|_| {
            AppError::new(
                "REGISTRY_URL_INVALID",
                ExitCode::Precondition,
                "registry connection string is not a valid Postgres URL",
            )
        })?;
        options = options.ssl_mode(PgSslMode::VerifyFull);
        if plan.statement_cache_capacity.is_none() {
            options = options.statement_cache_capacity(0);
        }
        let pool = PgPoolOptions::new()
            .max_connections(plan.max_connections)
            .min_connections(plan.min_connections)
            .idle_timeout(Duration::from_secs(plan.idle_timeout_secs))
            .connect_lazy_with(options);
        Ok(Self {
            pool,
            timeout: Duration::from_secs(timeout_secs),
            first_acquire: AtomicBool::new(false),
        })
    }

    pub async fn apply_migration(&self, sql: &str) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(sql)
                .execute(&mut *tx)
                .await
                .map_err(registry_unreachable)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    pub async fn prepared_statement_probe(&self) -> Result<i32, AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut last = 0;
            for _ in 0..2 {
                let row = sqlx::query("SELECT $1::int4 AS value")
                    .bind(17_i32)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(registry_unreachable)?;
                last = row.try_get("value").map_err(registry_unreachable)?;
            }
            Ok(last)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn acquire(&self) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, AppError> {
        let first = !self.first_acquire.swap(true, Ordering::SeqCst);
        let timeout = if first {
            self.timeout.saturating_mul(2)
        } else {
            self.timeout
        };
        tokio::time::timeout(timeout, self.pool.acquire())
            .await
            .map_err(|_| registry_unreachable_timeout())?
            .map_err(registry_unreachable)
    }
}

impl Registry for PgRegistry {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(UPSERT_GATEWAY_SQL)
            .bind(row.fleet_id)
            .bind(row.gateway_id)
            .bind(row.owner)
            .bind(row.endpoint_url)
            .bind(row.pubkey_fpr)
            .bind(row.cert_fpr)
            .bind(row.version)
            .bind(row.created_at)
            .bind(row.last_heartbeat)
            .bind(row.revoked_at)
            .bind(row.schema_version)
            .bind(row.row_msg)
            .bind(row.row_token)
            .bind(row.row_sig)
            .bind(row.signed_at)
            .bind(row.revocation)
            .execute(&mut *tx)
            .await
            .map_err(registry_gateway_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn heartbeat(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let gateway_id = gateway_id.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(HEARTBEAT_GATEWAY_SQL)
            .bind(fleet_id)
            .bind(gateway_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(registry_gateway_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let rows = sqlx::query(
                "SELECT fleet_id, gateway_id, owner, endpoint_url, pubkey_fpr, cert_fpr, \
                 version, created_at, last_heartbeat, revoked_at, schema_version, row_msg, \
                 row_token, row_sig, signed_at, revocation \
                 FROM mesh_gateways \
                 WHERE fleet_id = $1 \
                 ORDER BY gateway_id",
            )
            .bind(fleet_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(registry_gateway_error)?
            .into_iter()
            .map(pg_row)
            .collect::<Result<Vec<_>, _>>()?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(rows)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn fleet_ids(&self) -> Result<Vec<String>, AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let fleet_ids = sqlx::query("SELECT DISTINCT fleet_id FROM mesh_gateways ORDER BY fleet_id")
                .fetch_all(&mut *tx)
                .await
                .map_err(registry_gateway_error)?
                .into_iter()
                .map(|row| row.try_get::<String, _>("fleet_id").map_err(registry_gateway_error))
                .collect::<Result<Vec<_>, _>>()?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(fleet_ids)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        revocation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let gateway_id = gateway_id.to_string();
        let revocation = revocation.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(
                "UPDATE mesh_gateways SET revocation = $3, revoked_at = $4 \
                 WHERE fleet_id = $1 AND gateway_id = $2",
            )
            .bind(fleet_id)
            .bind(gateway_id)
            .bind(revocation)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(registry_gateway_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn clear_legacy_revoked_at(
        &self,
        fleet_id: &str,
        gateway_id: &str,
    ) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let gateway_id = gateway_id.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(
                "UPDATE mesh_gateways SET revoked_at = NULL \
                 WHERE fleet_id = $1 AND gateway_id = $2",
            )
            .bind(fleet_id)
            .bind(gateway_id)
            .execute(&mut *tx)
            .await
            .map_err(registry_gateway_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn schema_version(&self) -> Result<i32, AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let row = sqlx::query(
                "SELECT COALESCE(MAX(schema_version), 0) AS schema_version FROM mesh_gateways",
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(registry_gateway_error)?;
            let version = row
                .try_get("schema_version")
                .map_err(registry_gateway_error)?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(version)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(
                "INSERT INTO mesh_work_items \
                 (fleet_id, work_id, sender_gw, addressing, status, claimed_by, result_ref, \
                  result_delivery, swept_by, created_at, updated_at, expires_at, schema_version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            )
            .bind(row.fleet_id)
            .bind(row.work_id)
            .bind(row.sender_gw)
            .bind(row.addressing)
            .bind(row.status.as_sql())
            .bind(row.claimed_by)
            .bind(row.result_ref)
            .bind(row.result_delivery)
            .bind(row.swept_by)
            .bind(row.created_at)
            .bind(row.updated_at)
            .bind(row.expires_at)
            .bind(row.schema_version)
            .execute(&mut *tx)
            .await
            .map_err(registry_work_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_claim(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer_gw: &str,
    ) -> Result<ClaimOutcome, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let work_id = work_id.to_string();
        let claimer_gw = claimer_gw.to_string();
        let directed_to_me = format!("directed:{claimer_gw}");
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let updated = sqlx::query(WORK_CLAIM_SQL)
                .bind(&fleet_id)
                .bind(&work_id)
                .bind(&claimer_gw)
                .bind(&directed_to_me)
                .fetch_optional(&mut *tx)
                .await
                .map_err(registry_work_error)?;
            if updated.is_some() {
                tx.commit().await.map_err(registry_unreachable)?;
                return Ok(ClaimOutcome::Won);
            }
            let row = pg_work_get_tx(&mut tx, &fleet_id, &work_id).await?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(ClaimOutcome::Lost(row))
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_transition(
        &self,
        fleet_id: &str,
        work_id: &str,
        from: WorkStatus,
        to: WorkStatus,
        owner: WorkOwner,
        patch: WorkPatch,
    ) -> Result<TransitionOutcome, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let work_id = work_id.to_string();
        let (owner_kind, owner_value) = match owner {
            WorkOwner::Sender(gw) => ("sender", gw),
            WorkOwner::Claimer(gw) => ("claimer", gw),
            WorkOwner::Any => ("any", String::new()),
        };
        let (set_result_ref, result_ref) = patch_value(patch.result_ref);
        let (set_result_delivery, result_delivery) = patch_value(patch.result_delivery);
        let (set_swept_by, swept_by) = patch_value(patch.swept_by);
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let updated = sqlx::query(WORK_TRANSITION_SQL)
                .bind(&fleet_id)
                .bind(&work_id)
                .bind(from.as_sql())
                .bind(to.as_sql())
                .bind(set_result_ref)
                .bind(result_ref)
                .bind(set_result_delivery)
                .bind(result_delivery)
                .bind(set_swept_by)
                .bind(swept_by)
                .bind(patch.updated_at)
                .bind(owner_kind)
                .bind(owner_value)
                .fetch_optional(&mut *tx)
                .await
                .map_err(registry_work_error)?;
            if updated.is_some() {
                tx.commit().await.map_err(registry_unreachable)?;
                return Ok(TransitionOutcome::Applied);
            }
            let row = pg_work_get_tx(&mut tx, &fleet_id, &work_id).await?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(TransitionOutcome::Lost(row))
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let work_id = work_id.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let row = sqlx::query(WORK_SELECT_BY_ID_SQL)
                .bind(&fleet_id)
                .bind(&work_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(registry_work_error)?
                .map(pg_work_row)
                .transpose()?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(row)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_list(
        &self,
        fleet_id: &str,
        filter: WorkListFilter,
    ) -> Result<Vec<WorkRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let status = filter.status.map(|s| s.as_sql().to_string());
        let claimed_by = filter.claimed_by;
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let rows = sqlx::query(WORK_LIST_SQL)
                .bind(&fleet_id)
                .bind(status)
                .bind(claimed_by)
                .fetch_all(&mut *tx)
                .await
                .map_err(registry_work_error)?
                .into_iter()
                .map(pg_work_row)
                .collect::<Result<Vec<_>, _>>()?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(rows)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_touch_running(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer: &str,
    ) -> Result<(), AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let work_id = work_id.to_string();
        let claimer = claimer.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            sqlx::query(
                "UPDATE mesh_work_items SET updated_at = now() \
                 WHERE fleet_id = $1 AND work_id = $2 AND status = 'running' AND claimed_by = $3",
            )
            .bind(fleet_id)
            .bind(work_id)
            .bind(claimer)
            .execute(&mut *tx)
            .await
            .map_err(registry_work_error)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_sweep_expired(
        &self,
        fleet_id: &str,
        swept_by: &str,
    ) -> Result<Vec<String>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let swept_by = swept_by.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let mut ids = sqlx::query(
                "UPDATE mesh_work_items \
                 SET status = 'expired', swept_by = $2, updated_at = now() \
                 WHERE fleet_id = $1 AND status = 'offered' AND expires_at < now() \
                 RETURNING work_id",
            )
            .bind(fleet_id)
            .bind(swept_by)
            .fetch_all(&mut *tx)
            .await
            .map_err(registry_work_error)?
            .into_iter()
            .map(|row| row.try_get("work_id").map_err(registry_work_error))
            .collect::<Result<Vec<String>, _>>()?;
            ids.sort();
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(ids)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_sweep_abandoned(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        stall: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let claimer_gw = claimer_gw.to_string();
        let stall_secs = i64::try_from(stall.as_secs()).unwrap_or(i64::MAX);
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let mut rows = sqlx::query(WORK_SWEEP_ABANDONED_SQL)
                .bind(fleet_id)
                .bind(claimer_gw)
                .bind(stall_secs)
                .fetch_all(&mut *tx)
                .await
                .map_err(registry_work_error)?
                .into_iter()
                .map(pg_work_row)
                .collect::<Result<Vec<_>, _>>()?;
            rows.sort_by(|a, b| a.work_id.cmp(&b.work_id));
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(rows)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn work_sweep_release_stalled(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        horizon: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let claimer_gw = claimer_gw.to_string();
        let horizon_secs = i64::try_from(horizon.as_secs()).unwrap_or(i64::MAX);
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let mut rows = sqlx::query(WORK_SWEEP_RELEASE_STALLED_SQL)
                .bind(fleet_id)
                .bind(claimer_gw)
                .bind(horizon_secs)
                .fetch_all(&mut *tx)
                .await
                .map_err(registry_work_error)?
                .into_iter()
                .map(pg_work_row)
                .collect::<Result<Vec<_>, _>>()?;
            rows.sort_by(|a, b| a.work_id.cmp(&b.work_id));
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(rows)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }
}

pub fn registry_plan(url: &str) -> Result<RegistryPlan, AppError> {
    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        return Err(AppError::new(
            "REGISTRY_URL_INVALID",
            ExitCode::Precondition,
            "registry connection string must be a Postgres URL",
        ));
    }
    if let Some(mode) = query_param(url, "sslmode").or_else(|| query_param(url, "ssl-mode")) {
        if !mode.eq_ignore_ascii_case("verify-full") {
            return Err(AppError::new(
                "REGISTRY_TLS_REQUIRED",
                ExitCode::Precondition,
                "registry requires sslmode=verify-full",
            )
            .remedy("remove the sslmode parameter or set sslmode=verify-full"));
        }
    }
    let host = query_param(url, "host")
        .or_else(|| query_param(url, "hostaddr"))
        .unwrap_or_else(|| authority_host(url).unwrap_or_default());
    let statement_cache_capacity = if host.contains("-pooler") {
        None
    } else {
        Some(100)
    };
    Ok(RegistryPlan {
        ssl_verify_full: true,
        statement_cache_capacity,
        max_connections: 1,
        min_connections: 0,
        idle_timeout_secs: 60,
    })
}

pub fn staleness(
    last_heartbeat: DateTime<Utc>,
    now: DateTime<Utc>,
    heartbeat_secs: u64,
) -> RegistryStaleness {
    let active_window = chrono::Duration::seconds((heartbeat_secs.saturating_mul(3)) as i64);
    if now.signed_duration_since(last_heartbeat) < active_window {
        RegistryStaleness::Active
    } else {
        RegistryStaleness::Stale
    }
}

async fn set_local_statement_timeout<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Postgres>,
    timeout: Duration,
) -> Result<(), AppError> {
    let millis = timeout.as_millis().max(1);
    let sql = format!("{SET_LOCAL_STATEMENT_TIMEOUT}'{millis}ms'");
    sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map(|_| ())
        .map_err(registry_unreachable)
}

fn pg_row(row: sqlx::postgres::PgRow) -> Result<GatewayRow, AppError> {
    Ok(GatewayRow {
        fleet_id: row.try_get("fleet_id").map_err(registry_gateway_error)?,
        gateway_id: row
            .try_get("gateway_id")
            .map_err(registry_gateway_error)?,
        owner: row.try_get("owner").map_err(registry_gateway_error)?,
        endpoint_url: row
            .try_get("endpoint_url")
            .map_err(registry_gateway_error)?,
        pubkey_fpr: row
            .try_get("pubkey_fpr")
            .map_err(registry_gateway_error)?,
        cert_fpr: row
            .try_get("cert_fpr")
            .map_err(registry_gateway_error)?,
        version: row.try_get("version").map_err(registry_gateway_error)?,
        created_at: row
            .try_get("created_at")
            .map_err(registry_gateway_error)?,
        last_heartbeat: row
            .try_get("last_heartbeat")
            .map_err(registry_gateway_error)?,
        revoked_at: row
            .try_get("revoked_at")
            .map_err(registry_gateway_error)?,
        schema_version: row
            .try_get("schema_version")
            .map_err(registry_gateway_error)?,
        row_msg: row.try_get("row_msg").map_err(registry_gateway_error)?,
        row_token: row
            .try_get("row_token")
            .map_err(registry_gateway_error)?,
        row_sig: row.try_get("row_sig").map_err(registry_gateway_error)?,
        signed_at: row
            .try_get("signed_at")
            .map_err(registry_gateway_error)?,
        revocation: row
            .try_get("revocation")
            .map_err(registry_gateway_error)?,
    })
}

fn pg_work_row(row: sqlx::postgres::PgRow) -> Result<WorkRow, AppError> {
    let status: String = row.try_get("status").map_err(registry_work_error)?;
    Ok(WorkRow {
        fleet_id: row.try_get("fleet_id").map_err(registry_work_error)?,
        work_id: row.try_get("work_id").map_err(registry_work_error)?,
        sender_gw: row.try_get("sender_gw").map_err(registry_work_error)?,
        addressing: row.try_get("addressing").map_err(registry_work_error)?,
        status: WorkStatus::from_sql(&status)?,
        claimed_by: row.try_get("claimed_by").map_err(registry_work_error)?,
        result_ref: row.try_get("result_ref").map_err(registry_work_error)?,
        result_delivery: row
            .try_get("result_delivery")
            .map_err(registry_work_error)?,
        swept_by: row.try_get("swept_by").map_err(registry_work_error)?,
        created_at: row.try_get("created_at").map_err(registry_work_error)?,
        updated_at: row.try_get("updated_at").map_err(registry_work_error)?,
        expires_at: row.try_get("expires_at").map_err(registry_work_error)?,
        schema_version: row
            .try_get("schema_version")
            .map_err(registry_work_error)?,
    })
}

async fn pg_work_get_tx<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Postgres>,
    fleet_id: &str,
    work_id: &str,
) -> Result<WorkRow, AppError> {
    sqlx::query(WORK_SELECT_BY_ID_SQL)
        .bind(fleet_id)
        .bind(work_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(registry_work_error)?
        .map(pg_work_row)
        .transpose()?
        .ok_or_else(|| work_not_found(fleet_id, work_id))
}

fn patch_value(value: Option<Option<String>>) -> (bool, Option<String>) {
    match value {
        Some(value) => (true, value),
        None => (false, None),
    }
}

fn query_param(url: &str, name: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    let query = query.split('#').next().unwrap_or(query);
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

fn authority_host(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    if host_port.starts_with('[') {
        return host_port
            .split_once(']')
            .map(|(host, _)| host.trim_start_matches('[').to_string());
    }
    Some(host_port.split(':').next().unwrap_or(host_port).to_string())
}

fn registry_unreachable(e: sqlx::Error) -> AppError {
    AppError::new(
        "REGISTRY_UNREACHABLE",
        ExitCode::BrokerUnavailable,
        format!("registry is unreachable: {e}"),
    )
    .remedy("check the registry URL, network path, and database availability")
}

pub(crate) fn map_gateway_sqlstate(code: Option<&str>) -> Option<AppError> {
    (code == Some("42703")).then(|| {
        AppError::new(
            "REGISTRY_SCHEMA_STALE",
            ExitCode::Precondition,
            "mesh gateway registry schema is stale",
        )
        .remedy("the fleet root machine must run `a2a mesh fleet migrate`")
    })
}

fn registry_gateway_error(e: sqlx::Error) -> AppError {
    if let sqlx::Error::Database(db) = &e {
        if let Some(err) = map_gateway_sqlstate(db.code().as_deref()) {
            return err;
        }
    }
    registry_unreachable(e)
}

fn registry_work_error(e: sqlx::Error) -> AppError {
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("42P01") {
            return AppError::new(
                "WORK_SCHEMA_MISSING",
                ExitCode::Precondition,
                "mesh work registry schema is missing",
            )
            .remedy("the fleet root machine must run `a2a mesh fleet migrate`");
        }
    }
    registry_unreachable(e)
}

fn registry_unreachable_timeout() -> AppError {
    AppError::new(
        "REGISTRY_UNREACHABLE",
        ExitCode::BrokerUnavailable,
        "registry operation timed out",
    )
    .remedy("check the registry URL, network path, and database availability")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_42703_on_mesh_gateways_maps_to_registry_schema_stale() {
        let err = map_gateway_sqlstate(Some("42703")).expect("42703 is mapped");
        assert_eq!(err.code, "REGISTRY_SCHEMA_STALE");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert!(
            err.remedy
                .expect("schema-stale errors carry a remedy")
                .contains("a2a mesh fleet migrate")
        );
        assert!(map_gateway_sqlstate(Some("42P01")).is_none());
        assert!(map_gateway_sqlstate(None).is_none());
    }
}
