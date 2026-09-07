use super::RegistryErrorChange;
use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::keys::{self, Clock};
use crate::mesh::registry::{self, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use crate::mesh::row_mark::RowMarkStore;
use crate::mesh::row_sig::{self, RowMsg, RowVerdict, RowVerifyCfg, ROW_MSG_VER};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Timelike, Utc};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const DEFAULT_NGROK_API: &str = "http://127.0.0.1:4040";
const NGROK_API_TIMEOUT: Duration = Duration::from_secs(2);

#[allow(async_fn_in_trait)]
pub trait Exposure {
    async fn current_url(&self) -> Result<Option<String>, AppError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoneExposure;

impl Exposure for NoneExposure {
    async fn current_url(&self) -> Result<Option<String>, AppError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct ManualExposure {
    endpoint_url: Option<String>,
}

impl ManualExposure {
    pub fn new(endpoint_url: Option<String>) -> Self {
        Self { endpoint_url }
    }
}

impl Exposure for ManualExposure {
    async fn current_url(&self) -> Result<Option<String>, AppError> {
        Ok(self.endpoint_url.clone())
    }
}

#[derive(Clone, Debug)]
pub enum ExposureProvider {
    None(NoneExposure),
    Manual(ManualExposure),
    Ngrok(NgrokExposure),
}

impl ExposureProvider {
    pub fn from_config(cfg: &Config) -> Result<Self, AppError> {
        match cfg.mesh.gateway.expose.as_str() {
            "none" => Ok(Self::None(NoneExposure)),
            "manual" => Ok(Self::Manual(ManualExposure::new(
                cfg.mesh.gateway.endpoint_url.clone(),
            ))),
            "ngrok" => Ok(Self::Ngrok(NgrokExposure::new(
                cfg.mesh.gateway.listen_port,
            ))),
            other => Err(AppError::new(
                "MESH_GATEWAY_EXPOSE_INVALID",
                ExitCode::Precondition,
                format!("mesh.gateway.expose must be none, manual, or ngrok; got `{other}`"),
            )),
        }
    }
}

impl Exposure for ExposureProvider {
    async fn current_url(&self) -> Result<Option<String>, AppError> {
        match self {
            ExposureProvider::None(exposure) => exposure.current_url().await,
            ExposureProvider::Manual(exposure) => exposure.current_url().await,
            ExposureProvider::Ngrok(exposure) => exposure.current_url().await,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NgrokExposure<A = HttpNgrokApi> {
    listen_port: u64,
    api: A,
}

impl NgrokExposure<HttpNgrokApi> {
    pub fn new(listen_port: u64) -> Self {
        Self::with_api_base(listen_port, DEFAULT_NGROK_API.to_string())
    }

    pub fn with_api_base(listen_port: u64, api_base: String) -> Self {
        Self::with_api(listen_port, HttpNgrokApi::new(api_base))
    }
}

impl<A> NgrokExposure<A> {
    pub fn with_api(listen_port: u64, api: A) -> Self {
        Self { listen_port, api }
    }

    pub fn api(&self) -> &A {
        &self.api
    }
}

impl<A: NgrokApi> Exposure for NgrokExposure<A> {
    async fn current_url(&self) -> Result<Option<String>, AppError> {
        let tunnels = self.api.tunnels().await?;
        if let Some(tunnel) = tunnels
            .tunnels
            .into_iter()
            .find(|tunnel| tunnel.matches_port(self.listen_port))
        {
            return tunnel.to_wss_url().map(Some);
        }
        self.api
            .start_tunnel(self.listen_port)
            .await?
            .to_wss_url()
            .map(Some)
    }
}

#[derive(Clone, Debug)]
pub struct ExposureRow {
    pub fleet_id: String,
    pub gateway_id: String,
    pub owner: String,
    pub pubkey_fpr: String,
    pub cert_fpr: String,
    pub version: String,
}

#[derive(Clone)]
pub struct RowSigningContext {
    pub home: A2aHome,
    pub row_resign_secs: u64,
    pub registry_heartbeat_secs: u64,
    pub enforce_require_signed_rows: bool,
    pub enforce_auto_push: bool,
    pub max_accepted_row_resign_secs: u64,
}

impl std::fmt::Debug for RowSigningContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowSigningContext")
            .field("home", &self.home.root())
            .field("row_resign_secs", &self.row_resign_secs)
            .field("registry_heartbeat_secs", &self.registry_heartbeat_secs)
            .field(
                "enforce_require_signed_rows",
                &self.enforce_require_signed_rows,
            )
            .field("enforce_auto_push", &self.enforce_auto_push)
            .field(
                "max_accepted_row_resign_secs",
                &self.max_accepted_row_resign_secs,
            )
            .finish()
    }
}

impl RowSigningContext {
    pub fn from_config(home: &A2aHome, cfg: &Config) -> Self {
        // W3 adds mesh.work.auto_push. Reading it through serde keeps this wave compatible
        // with both the pre-W3 and post-W3 Config shapes, defaulting fail-closed to false.
        let enforce_auto_push = serde_json::to_value(&cfg.mesh.work)
            .ok()
            .and_then(|work| work.get("auto_push").and_then(serde_json::Value::as_bool))
            .unwrap_or(false);
        Self {
            home: home.clone(),
            row_resign_secs: cfg.mesh.gateway.row_resign_secs,
            registry_heartbeat_secs: cfg.mesh.gateway.registry_heartbeat_secs,
            enforce_require_signed_rows: cfg.mesh.gateway.require_signed_rows,
            enforce_auto_push,
            max_accepted_row_resign_secs: cfg
                .mesh
                .gateway
                .max_accepted_row_resign_secs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExposureTick {
    pub endpoint_url: Option<String>,
    pub upserted: bool,
    pub resigned: bool,
    pub self_tamper: Option<String>,
    pub exposure_error: Option<String>,
    pub exposure_error_since: Option<DateTime<Utc>>,
    pub exposure_error_change: Option<ExposureErrorChange>,
    pub registry_error: Option<String>,
    pub registry_error_since: Option<DateTime<Utc>>,
    pub registry_error_change: Option<RegistryErrorChange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExposureErrorChange {
    pub error: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub suppressed: u64,
}

#[derive(Clone, Debug)]
struct ExposureErrorState {
    message: String,
    since: DateTime<Utc>,
    suppressed: u64,
}

#[derive(Clone, Debug)]
pub struct ExposurePublisher {
    row: ExposureRow,
    signing: Option<RowSigningContext>,
    last_signed_msg: Option<RowMsg>,
    last_endpoint_url: Option<Option<String>>,
    created_at: Option<DateTime<Utc>>,
    exposure_error: Option<ExposureErrorState>,
    registry_error: Option<ExposureErrorState>,
    last_self_tamper: Option<String>,
}

impl ExposurePublisher {
    pub fn new(row: ExposureRow) -> Self {
        Self {
            row,
            signing: None,
            last_signed_msg: None,
            last_endpoint_url: None,
            created_at: None,
            exposure_error: None,
            registry_error: None,
            last_self_tamper: None,
        }
    }

    pub fn with_signing(mut self, ctx: RowSigningContext) -> Self {
        self.signing = Some(ctx);
        self
    }

    pub fn gateway_id(&self) -> &str {
        &self.row.gateway_id
    }

    pub async fn tick<R, E>(
        &mut self,
        registry: &R,
        exposure: &E,
        now: DateTime<Utc>,
    ) -> Result<ExposureTick, AppError>
    where
        R: Registry,
        E: Exposure,
    {
        if let Some(ctx) = self.signing.clone() {
            return self.signed_tick(registry, exposure, now, &ctx).await;
        }
        self.unsigned_tick(registry, exposure, now).await
    }

    async fn unsigned_tick<R, E>(
        &mut self,
        registry: &R,
        exposure: &E,
        now: DateTime<Utc>,
    ) -> Result<ExposureTick, AppError>
    where
        R: Registry,
        E: Exposure,
    {
        let endpoint_url = match exposure.current_url().await {
            Ok(endpoint_url) => endpoint_url,
            Err(err) => {
                let registry_error_change = match registry
                    .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                    .await
                {
                    Ok(()) => self.clear_registry_error(),
                    Err(err) => self.record_registry_error(&err, now),
                };
                let message = format!("{}: {}", err.code, err.message);
                let exposure_error_change = match &mut self.exposure_error {
                    Some(state) => {
                        state.suppressed = state.suppressed.saturating_add(1);
                        None
                    }
                    None => {
                        self.exposure_error = Some(ExposureErrorState {
                            message: message.clone(),
                            since: now,
                            suppressed: 0,
                        });
                        Some(ExposureErrorChange {
                            error: Some(message.clone()),
                            since: Some(now),
                            suppressed: 0,
                        })
                    }
                };
                let state = self.exposure_error.as_ref().expect("exposure error recorded");
                let (registry_error, registry_error_since) = self.registry_error_fields();
                return Ok(ExposureTick {
                    endpoint_url: self.last_endpoint_url.clone().flatten(),
                    upserted: false,
                    resigned: false,
                    self_tamper: None,
                    exposure_error: Some(state.message.clone()),
                    exposure_error_since: Some(state.since),
                    exposure_error_change,
                    registry_error,
                    registry_error_since,
                    registry_error_change,
                });
            }
        };
        let exposure_error_change = self.exposure_error.take().map(|state| ExposureErrorChange {
            error: None,
            since: Some(state.since),
            suppressed: state.suppressed,
        });
        let changed = self
            .last_endpoint_url
            .as_ref()
            .map(|last| last != &endpoint_url)
            .unwrap_or(true);
        let registry_result = if changed {
            let row = self.gateway_row(endpoint_url.clone(), now);
            registry.upsert_self(row).await.map(|()| true)
        } else {
            registry
                .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                .await
                .map(|()| false)
        };
        let upserted = match registry_result {
            Ok(upserted) => {
                if changed {
                    self.last_endpoint_url = Some(endpoint_url.clone());
                }
                upserted
            }
            Err(err) => {
                if changed {
                    let _ = registry
                        .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                        .await;
                }
                let registry_error_change = self.record_registry_error(&err, now);
                let (registry_error, registry_error_since) = self.registry_error_fields();
                return Ok(ExposureTick {
                    endpoint_url: self.last_endpoint_url.clone().flatten(),
                    upserted: false,
                    resigned: false,
                    self_tamper: None,
                    exposure_error: None,
                    exposure_error_since: None,
                    exposure_error_change,
                    registry_error,
                    registry_error_since,
                    registry_error_change,
                });
            }
        };
        let registry_error_change = self.clear_registry_error();
        Ok(ExposureTick {
            endpoint_url,
            upserted,
            resigned: false,
            self_tamper: None,
            exposure_error: None,
            exposure_error_since: None,
            exposure_error_change,
            registry_error: None,
            registry_error_since: None,
            registry_error_change,
        })
    }

    async fn signed_tick<R, E>(
        &mut self,
        registry: &R,
        exposure: &E,
        now: DateTime<Utc>,
        ctx: &RowSigningContext,
    ) -> Result<ExposureTick, AppError>
    where
        R: Registry,
        E: Exposure,
    {
        // Keep discover -> writer verification -> repair in this one awaited tick. The caller
        // never selects over individual phases, so cancellation cannot skip only the repair.
        let rows_result = match (
            registry::reader_roots(&ctx.home),
            RowMarkStore::open(&ctx.home),
        ) {
            (Ok(roots), Ok(marks)) => {
                let mut verify_cfg = RowVerifyCfg::new(now);
                verify_cfg.max_accepted_row_resign_secs = ctx.max_accepted_row_resign_secs;
                registry::discover_verified(
                    registry,
                    &self.row.fleet_id,
                    &roots,
                    &marks,
                    &verify_cfg,
                )
                .await
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        let rows = match rows_result {
            Ok(rows) => rows,
            Err(err) => {
                let registry_error_change = self.record_registry_error(&err, now);
                let (registry_error, registry_error_since) = self.registry_error_fields();
                let (exposure_error, exposure_error_since) = self.exposure_error_fields();
                return Ok(ExposureTick {
                    endpoint_url: self.last_endpoint_url.clone().flatten(),
                    upserted: false,
                    resigned: false,
                    self_tamper: None,
                    exposure_error,
                    exposure_error_since,
                    exposure_error_change: None,
                    registry_error,
                    registry_error_since,
                    registry_error_change,
                });
            }
        };
        let existing = rows
            .into_iter()
            .find(|(row, _verdict)| row.gateway_id == self.row.gateway_id);
        if let Some((row, _verdict)) = &existing {
            self.created_at = Some(row.created_at);
        }
        let self_verdict = existing.as_ref().map(|(_row, verdict)| *verdict);
        let existing_msg = existing
            .as_ref()
            .and_then(|(row, _verdict)| row.row_msg.as_deref())
            .and_then(|text| row_sig::parse_row_msg_strict(text).ok());
        let detected_self_tamper = match self_verdict {
            Some(RowVerdict::ValidSigned) => None,
            Some(verdict) => Some(verdict.label().to_string()),
            None => Some("MISSING".to_string()),
        };
        let revocation_blocks_repair = matches!(
            self_verdict,
            Some(RowVerdict::Revoked | RowVerdict::RowRevocationMisplaced)
        );
        let repair = detected_self_tamper.is_some() && !revocation_blocks_repair;
        let self_tamper = self.self_tamper_transition(detected_self_tamper);

        let (endpoint_url, exposure_error, exposure_error_since, exposure_error_change) =
            match exposure.current_url().await {
                Ok(endpoint_url) => {
                    let change = self.exposure_error.take().map(|state| ExposureErrorChange {
                        error: None,
                        since: Some(state.since),
                        suppressed: state.suppressed,
                    });
                    (endpoint_url, None, None, change)
                }
                Err(err) => {
                    let message = format!("{}: {}", err.code, err.message);
                    let change = match &mut self.exposure_error {
                        Some(state) => {
                            state.suppressed = state.suppressed.saturating_add(1);
                            None
                        }
                        None => {
                            self.exposure_error = Some(ExposureErrorState {
                                message: message.clone(),
                                since: now,
                                suppressed: 0,
                            });
                            Some(ExposureErrorChange {
                                error: Some(message),
                                since: Some(now),
                                suppressed: 0,
                            })
                        }
                    };
                    let fallback = self
                        .last_endpoint_url
                        .clone()
                        .or_else(|| existing_msg.as_ref().map(|msg| msg.endpoint_url.clone()));
                    if repair || self.last_signed_msg.is_none() {
                        if let Some(endpoint_url) = fallback {
                            let state = self
                                .exposure_error
                                .as_ref()
                                .expect("exposure error recorded");
                            (
                                endpoint_url,
                                Some(state.message.clone()),
                                Some(state.since),
                                change,
                            )
                        } else {
                            return self
                                .signed_exposure_error_tick(
                                    registry,
                                    now,
                                    self_tamper,
                                    change,
                                )
                                .await;
                        }
                    } else {
                        return self
                            .signed_exposure_error_tick(registry, now, None, change)
                            .await;
                    }
                }
            };

        let candidate = match self.signed_row(ctx, endpoint_url.clone(), now) {
            Ok(candidate) => candidate,
            Err(error) if error.code == "ROOTS_UNREADABLE" => {
                let _ = registry
                    .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                    .await;
                let registry_error_change = self.record_registry_error(&error, now);
                let (registry_error, registry_error_since) = self.registry_error_fields();
                return Ok(ExposureTick {
                    endpoint_url,
                    upserted: false,
                    resigned: false,
                    self_tamper,
                    exposure_error,
                    exposure_error_since,
                    exposure_error_change,
                    registry_error,
                    registry_error_since,
                    registry_error_change,
                });
            }
            Err(error) => return Err(error),
        };
        let candidate_msg = signed_message(&candidate)?;
        let endpoint_changed = self
            .last_endpoint_url
            .as_ref()
            .map(|last| last != &endpoint_url)
            .unwrap_or(true);
        let signed_fields_changed = self
            .last_signed_msg
            .as_ref()
            .map(|last| signed_fields_differ(last, &candidate_msg))
            .unwrap_or(true);
        let cadence_elapsed = self
            .last_signed_msg
            .as_ref()
            .map(|last| resign_cadence_elapsed(last.signed_at, now, ctx.row_resign_secs))
            .unwrap_or(true);
        // row_resign_secs is only the unchanged-row cadence. A failed self-check takes the
        // repair path above on every registry heartbeat, bounding tamper persistence to one tick.
        let should_resign = !revocation_blocks_repair
            && (repair || endpoint_changed || signed_fields_changed || cadence_elapsed);
        let registry_result = if should_resign {
            registry
                .upsert_self(candidate.clone())
                .await
                .map(|()| true)
        } else {
            registry
                .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                .await
                .map(|()| false)
        };
        let resigned = match registry_result {
            Ok(resigned) => resigned,
            Err(err) => {
                if should_resign {
                    let _ = registry
                        .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
                        .await;
                }
                let registry_error_change = self.record_registry_error(&err, now);
                let (registry_error, registry_error_since) = self.registry_error_fields();
                return Ok(ExposureTick {
                    endpoint_url: self.last_endpoint_url.clone().flatten(),
                    upserted: false,
                    resigned: false,
                    self_tamper,
                    exposure_error,
                    exposure_error_since,
                    exposure_error_change,
                    registry_error,
                    registry_error_since,
                    registry_error_change,
                });
            }
        };
        let mark_error = if resigned {
            persist_verified_row_mark(&ctx.home, &candidate, &candidate_msg).err()
        } else {
            None
        };
        if resigned {
            self.last_endpoint_url = Some(endpoint_url.clone());
            self.last_signed_msg = Some(candidate_msg);
            if repair {
                self.last_self_tamper = None;
            }
        }
        let registry_error_change = match mark_error.as_ref() {
            Some(error) => self.record_registry_error(error, now),
            None => self.clear_registry_error(),
        };
        let (registry_error, registry_error_since) = self.registry_error_fields();
        Ok(ExposureTick {
            endpoint_url,
            upserted: resigned,
            resigned,
            self_tamper,
            exposure_error,
            exposure_error_since,
            exposure_error_change,
            registry_error,
            registry_error_since,
            registry_error_change,
        })
    }

    async fn signed_exposure_error_tick<R: Registry>(
        &mut self,
        registry: &R,
        now: DateTime<Utc>,
        self_tamper: Option<String>,
        exposure_error_change: Option<ExposureErrorChange>,
    ) -> Result<ExposureTick, AppError> {
        let registry_error_change = match registry
            .heartbeat(&self.row.fleet_id, &self.row.gateway_id, now)
            .await
        {
            Ok(()) => self.clear_registry_error(),
            Err(err) => self.record_registry_error(&err, now),
        };
        let (registry_error, registry_error_since) = self.registry_error_fields();
        let (exposure_error, exposure_error_since) = self.exposure_error_fields();
        Ok(ExposureTick {
            endpoint_url: self.last_endpoint_url.clone().flatten(),
            upserted: false,
            resigned: false,
            self_tamper,
            exposure_error,
            exposure_error_since,
            exposure_error_change,
            registry_error,
            registry_error_since,
            registry_error_change,
        })
    }

    fn signed_row(
        &mut self,
        ctx: &RowSigningContext,
        endpoint_url: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<GatewayRow, AppError> {
        let token = keys::gateway_token(&ctx.home)?;
        let roots = registry::reader_roots(&ctx.home)?;
        let mut root_fingerprints = roots.fingerprints();
        root_fingerprints.sort();
        let roots_held = serde_json::to_string(&root_fingerprints).map_err(|err| {
            AppError::new(
                "SERIALIZE",
                ExitCode::Internal,
                format!("serializing held root fingerprints: {err}"),
            )
        })?;
        let key_epoch = keys::verify_token_no_clock(&token, &roots)?.0.key_epoch;
        let signing_key = keys::gateway_signing_key(&ctx.home)?;
        let signed_at = trunc_us(now);
        let mut row = self.gateway_row(endpoint_url, now);
        let msg = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: row.fleet_id.clone(),
            gateway_id: row.gateway_id.clone(),
            endpoint_url: row.endpoint_url.clone(),
            pubkey_fpr: row.pubkey_fpr.clone(),
            cert_fpr: row.cert_fpr.clone(),
            version: row.version.clone(),
            owner: row.owner.clone(),
            created_at: row.created_at,
            schema_version: row.schema_version,
            key_epoch,
            roots_held,
            row_token_sha256: sha256_hex(token.as_bytes()),
            row_resign_secs: ctx.row_resign_secs,
            registry_heartbeat_secs: ctx.registry_heartbeat_secs,
            enforce_require_signed_rows: ctx.enforce_require_signed_rows,
            enforce_auto_push: ctx.enforce_auto_push,
            signed_at,
        };
        let (row_msg, row_sig) = row_sig::sign_row(&msg, &signing_key)?;
        row.row_msg = Some(row_msg);
        row.row_token = Some(token);
        row.row_sig = Some(row_sig);
        row.signed_at = Some(signed_at);
        Ok(row)
    }

    fn record_registry_error(
        &mut self,
        err: &AppError,
        now: DateTime<Utc>,
    ) -> Option<RegistryErrorChange> {
        let message = format!("{}: {}", err.code, err.message);
        match &mut self.registry_error {
            Some(state) => {
                state.message = message;
                state.suppressed = state.suppressed.saturating_add(1);
                None
            }
            None => {
                self.registry_error = Some(ExposureErrorState {
                    message: message.clone(),
                    since: now,
                    suppressed: 0,
                });
                Some(RegistryErrorChange {
                    error: Some(message),
                    since: Some(now),
                    suppressed: 0,
                })
            }
        }
    }

    fn clear_registry_error(&mut self) -> Option<RegistryErrorChange> {
        self.registry_error.take().map(|state| RegistryErrorChange {
            error: None,
            since: Some(state.since),
            suppressed: state.suppressed,
        })
    }

    fn registry_error_fields(&self) -> (Option<String>, Option<DateTime<Utc>>) {
        self.registry_error
            .as_ref()
            .map(|state| (Some(state.message.clone()), Some(state.since)))
            .unwrap_or((None, None))
    }

    fn exposure_error_fields(&self) -> (Option<String>, Option<DateTime<Utc>>) {
        self.exposure_error
            .as_ref()
            .map(|state| (Some(state.message.clone()), Some(state.since)))
            .unwrap_or((None, None))
    }

    fn self_tamper_transition(&mut self, detected: Option<String>) -> Option<String> {
        let changed = detected.is_some() && detected != self.last_self_tamper;
        self.last_self_tamper = detected.clone();
        changed.then_some(detected).flatten()
    }

    fn gateway_row(&mut self, endpoint_url: Option<String>, now: DateTime<Utc>) -> GatewayRow {
        let created_at = trunc_us(*self.created_at.get_or_insert_with(|| trunc_us(now)));
        self.created_at = Some(created_at);
        GatewayRow {
            fleet_id: self.row.fleet_id.clone(),
            gateway_id: self.row.gateway_id.clone(),
            owner: self.row.owner.clone(),
            cert_fpr: endpoint_url.as_ref().map(|_| self.row.cert_fpr.clone()),
            endpoint_url,
            pubkey_fpr: self.row.pubkey_fpr.clone(),
            version: self.row.version.clone(),
            created_at,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: REGISTRY_SCHEMA_VERSION,
            row_msg: None,
            row_token: None,
            row_sig: None,
            signed_at: None,
            revocation: None,
        }
    }
}

fn persist_verified_row_mark(
    home: &A2aHome,
    row: &GatewayRow,
    msg: &RowMsg,
) -> Result<(), AppError> {
    let token = row.row_token.as_deref().ok_or_else(|| {
        AppError::new(
            "INTERNAL",
            ExitCode::Internal,
            "locally signed registry row is missing its enrollment token",
        )
    })?;
    let roots = registry::reader_roots(home)?;
    let claims = keys::verify_token_no_clock(token, &roots)?.0;
    let mut marks = RowMarkStore::open(home)?;
    marks.apply_verified(&row.gateway_id, msg, &claims)
}

fn signed_message(row: &GatewayRow) -> Result<RowMsg, AppError> {
    row.row_msg
        .as_deref()
        .ok_or_else(|| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                "signed gateway row is missing row_msg",
            )
        })
        .and_then(|text| {
            row_sig::parse_row_msg_strict(text).map_err(|err| {
                AppError::new(
                    "INTERNAL",
                    ExitCode::Internal,
                    format!("newly signed gateway row could not be parsed: {err}"),
                )
            })
        })
}

fn signed_fields_differ(previous: &RowMsg, current: &RowMsg) -> bool {
    let mut current = current.clone();
    current.signed_at = previous.signed_at;
    &current != previous
}

fn resign_cadence_elapsed(
    last_signed_at: DateTime<Utc>,
    now: DateTime<Utc>,
    row_resign_secs: u64,
) -> bool {
    let cadence = i64::try_from(row_resign_secs).unwrap_or(i64::MAX);
    now.signed_duration_since(last_signed_at) >= chrono::Duration::seconds(cadence)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn trunc_us(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(dt.timestamp_subsec_micros() * 1_000)
        .expect("microseconds fit")
}

pub async fn run_exposure_loop<R, E, C>(
    registry: &R,
    exposure: &E,
    publisher: &mut ExposurePublisher,
    heartbeat_interval: Duration,
    clock: &C,
    token: CancellationToken,
) -> Result<(), AppError>
where
    R: Registry,
    E: Exposure,
    C: Clock + ?Sized,
{
    run_exposure_loop_with_events(
        registry,
        exposure,
        publisher,
        heartbeat_interval,
        clock,
        token,
        None,
    )
    .await
}

pub async fn run_exposure_loop_with_events<R, E, C>(
    registry: &R,
    exposure: &E,
    publisher: &mut ExposurePublisher,
    heartbeat_interval: Duration,
    clock: &C,
    token: CancellationToken,
    events: Option<mpsc::Sender<ExposureTick>>,
) -> Result<(), AppError>
where
    R: Registry,
    E: Exposure,
    C: Clock + ?Sized,
{
    loop {
        let tick = publisher.tick(registry, exposure, clock.now()).await?;
        if let Some(tx) = &events {
            let _ = tx.send(tick).await;
        }
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            _ = tokio::time::sleep(jittered_heartbeat(heartbeat_interval)) => {}
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait NgrokApi {
    async fn tunnels(&self) -> Result<NgrokTunnels, AppError>;
    async fn start_tunnel(&self, listen_port: u64) -> Result<NgrokTunnel, AppError>;
}

#[derive(Clone, Debug)]
pub struct HttpNgrokApi {
    api_base: String,
}

impl HttpNgrokApi {
    pub fn new(api_base: String) -> Self {
        Self { api_base }
    }
}

impl NgrokApi for HttpNgrokApi {
    async fn tunnels(&self) -> Result<NgrokTunnels, AppError> {
        ngrok_json("GET", &self.api_base, "/api/tunnels", None).await
    }

    async fn start_tunnel(&self, listen_port: u64) -> Result<NgrokTunnel, AppError> {
        ngrok_json(
            "POST",
            &self.api_base,
            "/api/tunnels",
            Some(json!({
                "name": format!("a2a-mesh-{listen_port}"),
                "proto": "tcp",
                "addr": format!("localhost:{listen_port}"),
            })),
        )
        .await
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct NgrokTunnels {
    pub tunnels: Vec<NgrokTunnel>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NgrokTunnel {
    pub proto: Option<String>,
    pub public_url: String,
    pub config: Option<NgrokTunnelConfig>,
}

impl NgrokTunnel {
    fn matches_port(&self, listen_port: u64) -> bool {
        self.proto.as_deref() == Some("tcp")
            && self
                .config
                .as_ref()
                .map(|cfg| addr_matches_listen_port(&cfg.addr, listen_port))
                .unwrap_or(false)
    }

    fn to_wss_url(&self) -> Result<String, AppError> {
        let tcp = self.public_url.strip_prefix("tcp://").ok_or_else(|| {
            AppError::new(
                "NGROK_TUNNEL_INVALID",
                ExitCode::BrokerUnavailable,
                "ngrok TCP tunnel did not return a tcp:// public_url",
            )
        })?;
        let (host, port) = tcp.rsplit_once(':').ok_or_else(|| {
            AppError::new(
                "NGROK_TUNNEL_INVALID",
                ExitCode::BrokerUnavailable,
                "ngrok TCP tunnel public_url did not include a port",
            )
        })?;
        if host.is_empty() || port.parse::<u16>().is_err() {
            return Err(AppError::new(
                "NGROK_TUNNEL_INVALID",
                ExitCode::BrokerUnavailable,
                "ngrok TCP tunnel public_url host or port is invalid",
            ));
        }
        Ok(format!("wss://{host}:{port}"))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct NgrokTunnelConfig {
    pub addr: String,
}

#[derive(Clone, Debug)]
struct HttpEndpoint {
    host: String,
    port: u16,
    base_path: String,
}

async fn ngrok_json<T>(
    method: &str,
    api_base: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<T, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    let endpoint = parse_http_base(api_base)?;
    let full_path = format!("{}{}", endpoint.base_path, path);
    let body = body.map(|value| value.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {full_path} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        endpoint.host,
        endpoint.port,
        body.len(),
        body
    );
    tokio::time::timeout(NGROK_API_TIMEOUT, async move {
        let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(|e| ngrok_unavailable(format!("connecting to ngrok agent API: {e}")))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| ngrok_unavailable(format!("writing to ngrok agent API: {e}")))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| ngrok_unavailable(format!("reading from ngrok agent API: {e}")))?;
        parse_json_response(&response)
    })
    .await
    .map_err(|_| ngrok_unavailable("ngrok agent API timed out".to_string()))?
}

fn parse_json_response<T>(response: &[u8]) -> Result<T, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    let response = std::str::from_utf8(response).map_err(|e| {
        AppError::new(
            "NGROK_API_INVALID",
            ExitCode::BrokerUnavailable,
            format!("ngrok agent response was not utf-8: {e}"),
        )
    })?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        AppError::new(
            "NGROK_API_INVALID",
            ExitCode::BrokerUnavailable,
            "ngrok agent response was not an HTTP response",
        )
    })?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(AppError::new(
            "NGROK_API",
            ExitCode::BrokerUnavailable,
            format!("ngrok agent API returned HTTP {status}"),
        ));
    }
    serde_json::from_str(body).map_err(|e| {
        AppError::new(
            "NGROK_API_INVALID",
            ExitCode::BrokerUnavailable,
            format!("ngrok agent response was invalid JSON: {e}"),
        )
    })
}

fn parse_http_base(api_base: &str) -> Result<HttpEndpoint, AppError> {
    let rest = api_base.strip_prefix("http://").ok_or_else(|| {
        AppError::new(
            "NGROK_API_URL_INVALID",
            ExitCode::Precondition,
            "ngrok API URL must start with http://",
        )
    })?;
    let (authority, base_path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
        AppError::new(
            "NGROK_API_URL_INVALID",
            ExitCode::Precondition,
            "ngrok API URL must include an explicit port",
        )
    })?;
    let port = port.parse::<u16>().map_err(|_| {
        AppError::new(
            "NGROK_API_URL_INVALID",
            ExitCode::Precondition,
            "ngrok API URL port is invalid",
        )
    })?;
    Ok(HttpEndpoint {
        host: host.to_string(),
        port,
        base_path: if base_path.is_empty() {
            String::new()
        } else {
            format!("/{base_path}")
        },
    })
}

fn addr_matches_listen_port(addr: &str, listen_port: u64) -> bool {
    addr == listen_port.to_string()
        || addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u64>().ok())
            == Some(listen_port)
}

fn ngrok_unavailable(message: String) -> AppError {
    AppError::new("NGROK_UNAVAILABLE", ExitCode::BrokerUnavailable, message).remedy(
        "install ngrok, authenticate it, and start the local ngrok agent so http://127.0.0.1:4040/api/tunnels is available",
    )
}

fn jittered_heartbeat(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let lower = base_ms.saturating_mul(9) / 10;
    let upper = base_ms.saturating_mul(11) / 10;
    Duration::from_millis(rand::thread_rng().gen_range(lower..=upper.max(lower + 1)))
}
