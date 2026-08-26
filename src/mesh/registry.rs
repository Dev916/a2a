use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{Acquire, PgPool, Row};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub const REGISTRY_SCHEMA_VERSION: i32 = 1;
pub const DEFAULT_REGISTRY_HEARTBEAT_SECS: u64 = 180;
pub const DEFAULT_REGISTRY_TIMEOUT_SECS: u64 = 10;

const FILE_REGISTRY_ROWS: &str = "mesh_gateways.json";
const SET_LOCAL_STATEMENT_TIMEOUT: &str = concat!("SET", " LOCAL statement_timeout = ");

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
    async fn discover(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError>;
    async fn retire_self(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError>;
    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError>;
    async fn schema_version(&self) -> Result<i32, AppError>;
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
        Ok(())
    }

    fn rows_path(&self) -> PathBuf {
        self.root.join(FILE_REGISTRY_ROWS)
    }

    fn ensure_root(&self) -> Result<(), AppError> {
        crate::home::create_dir_0700(&self.root)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("creating file registry: {e}")))
    }

    fn read_rows(&self) -> Result<Vec<GatewayRow>, AppError> {
        self.ensure_root()?;
        let path = self.rows_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&path).map_err(|e| {
            AppError::new("IO", ExitCode::Internal, format!("reading file registry: {e}"))
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
        crate::store::write_atomic(&self.rows_path(), &bytes)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing file registry: {e}")))
    }
}

impl Registry for FileRegistry {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError> {
        let mut rows = self.read_rows()?;
        rows.retain(|r| !(r.fleet_id == row.fleet_id && r.gateway_id == row.gateway_id));
        rows.push(row);
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

    async fn discover(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        Ok(self
            .read_rows()?
            .into_iter()
            .filter(|r| r.fleet_id == fleet_id && r.revoked_at.is_none())
            .collect())
    }

    async fn retire_self(
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
            row.endpoint_url = None;
            row.last_heartbeat = now;
        }
        self.write_rows(&rows)
    }

    async fn revoke(
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
            row.revoked_at = Some(now);
        }
        self.write_rows(&rows)
    }

    async fn schema_version(&self) -> Result<i32, AppError> {
        Ok(REGISTRY_SCHEMA_VERSION)
    }
}

pub struct PgRegistry {
    pool: PgPool,
    timeout: Duration,
    first_acquire: AtomicBool,
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
            sqlx::query(sql).execute(&mut *tx).await.map_err(registry_unreachable)?;
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
            sqlx::query(
                "INSERT INTO mesh_gateways \
                 (fleet_id, gateway_id, owner, endpoint_url, pubkey_fpr, cert_fpr, version, \
                  created_at, last_heartbeat, revoked_at, schema_version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
                 ON CONFLICT (fleet_id, gateway_id) DO UPDATE SET \
                 owner = EXCLUDED.owner, endpoint_url = EXCLUDED.endpoint_url, \
                 pubkey_fpr = EXCLUDED.pubkey_fpr, cert_fpr = EXCLUDED.cert_fpr, \
                 version = EXCLUDED.version, last_heartbeat = EXCLUDED.last_heartbeat, \
                 revoked_at = EXCLUDED.revoked_at, schema_version = EXCLUDED.schema_version",
            )
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
            .execute(&mut *tx)
            .await
            .map_err(registry_unreachable)?;
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
            sqlx::query(
                "UPDATE mesh_gateways SET last_heartbeat = $3 \
                 WHERE fleet_id = $1 AND gateway_id = $2",
            )
            .bind(fleet_id)
            .bind(gateway_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(registry_unreachable)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn discover(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        let mut conn = self.acquire().await?;
        let fleet_id = fleet_id.to_string();
        let timeout = self.timeout;
        tokio::time::timeout(timeout, async move {
            let mut tx = conn.begin().await.map_err(registry_unreachable)?;
            set_local_statement_timeout(&mut tx, timeout).await?;
            let rows = sqlx::query(
                "SELECT fleet_id, gateway_id, owner, endpoint_url, pubkey_fpr, cert_fpr, \
                 version, created_at, last_heartbeat, revoked_at, schema_version \
                 FROM mesh_gateways \
                 WHERE fleet_id = $1 AND revoked_at IS NULL \
                 ORDER BY gateway_id",
            )
            .bind(fleet_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(registry_unreachable)?
            .into_iter()
            .map(pg_row)
            .collect::<Result<Vec<_>, _>>()?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(rows)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn retire_self(
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
            sqlx::query(
                "UPDATE mesh_gateways SET endpoint_url = NULL, last_heartbeat = $3 \
                 WHERE fleet_id = $1 AND gateway_id = $2",
            )
            .bind(fleet_id)
            .bind(gateway_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(registry_unreachable)?;
            tx.commit().await.map_err(registry_unreachable)
        })
        .await
        .map_err(|_| registry_unreachable_timeout())?
    }

    async fn revoke(
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
            sqlx::query(
                "UPDATE mesh_gateways SET revoked_at = $3 \
                 WHERE fleet_id = $1 AND gateway_id = $2",
            )
            .bind(fleet_id)
            .bind(gateway_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(registry_unreachable)?;
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
            let row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) AS schema_version FROM mesh_gateways")
                .fetch_one(&mut *tx)
                .await
                .map_err(registry_unreachable)?;
            let version = row.try_get("schema_version").map_err(registry_unreachable)?;
            tx.commit().await.map_err(registry_unreachable)?;
            Ok(version)
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
        fleet_id: row.try_get("fleet_id").map_err(registry_unreachable)?,
        gateway_id: row.try_get("gateway_id").map_err(registry_unreachable)?,
        owner: row.try_get("owner").map_err(registry_unreachable)?,
        endpoint_url: row.try_get("endpoint_url").map_err(registry_unreachable)?,
        pubkey_fpr: row.try_get("pubkey_fpr").map_err(registry_unreachable)?,
        cert_fpr: row.try_get("cert_fpr").map_err(registry_unreachable)?,
        version: row.try_get("version").map_err(registry_unreachable)?,
        created_at: row.try_get("created_at").map_err(registry_unreachable)?,
        last_heartbeat: row.try_get("last_heartbeat").map_err(registry_unreachable)?,
        revoked_at: row.try_get("revoked_at").map_err(registry_unreachable)?,
        schema_version: row.try_get("schema_version").map_err(registry_unreachable)?,
    })
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
    let authority = after_scheme.split(['/', '?', '#']).next().unwrap_or(after_scheme);
    let host_port = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
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

fn registry_unreachable_timeout() -> AppError {
    AppError::new(
        "REGISTRY_UNREACHABLE",
        ExitCode::BrokerUnavailable,
        "registry operation timed out",
    )
    .remedy("check the registry URL, network path, and database availability")
}
