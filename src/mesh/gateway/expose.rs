use crate::config::Config;
use crate::mesh::keys::Clock;
use crate::mesh::registry::{GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use crate::output::{AppError, ExitCode};
use super::RegistryErrorChange;
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExposureTick {
    pub endpoint_url: Option<String>,
    pub upserted: bool,
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
    last_endpoint_url: Option<Option<String>>,
    created_at: Option<DateTime<Utc>>,
    exposure_error: Option<ExposureErrorState>,
    registry_error: Option<ExposureErrorState>,
}

impl ExposurePublisher {
    pub fn new(row: ExposureRow) -> Self {
        Self {
            row,
            last_endpoint_url: None,
            created_at: None,
            exposure_error: None,
            registry_error: None,
        }
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
                let registry_error_change = self.record_registry_error(&err, now);
                let (registry_error, registry_error_since) = self.registry_error_fields();
                return Ok(ExposureTick {
                    endpoint_url: self.last_endpoint_url.clone().flatten(),
                    upserted: false,
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
            exposure_error: None,
            exposure_error_since: None,
            exposure_error_change,
            registry_error: None,
            registry_error_since: None,
            registry_error_change,
        })
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

    fn gateway_row(&mut self, endpoint_url: Option<String>, now: DateTime<Utc>) -> GatewayRow {
        let created_at = *self.created_at.get_or_insert(now);
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
        }
    }
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
