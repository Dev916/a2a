use crate::mesh::registry::GatewayRow;
use crate::mesh::wire::{
    accept_pinned, accept_pinned_io, connect_pinned, connect_pinned_io, Frame, Side, TlsIdentity,
};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub const PRESENCE_MAILBOX_CAPACITY: usize = 64;
pub const OUTBOUND_QUEUE_CAPACITY: usize = 1024;
pub const SHED_LOG_INTERVAL: chrono::Duration = chrono::Duration::seconds(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkFlappingEvent {
    pub kind: &'static str,
    pub peer_gateway_id: String,
    pub cooldown_until: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct PeerFlapState {
    drops: VecDeque<DateTime<Utc>>,
    cooldown_until: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct PeerDamping {
    window: Duration,
    peers: BTreeMap<String, PeerFlapState>,
}

impl PeerDamping {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            peers: BTreeMap::new(),
        }
    }

    pub fn admit(
        &self,
        local_gateway_id: &str,
        peer_gateway_id: &str,
        direction: LinkDirection,
        now: DateTime<Utc>,
    ) -> bool {
        match direction {
            LinkDirection::Outbound => {
                local_gateway_id < peer_gateway_id && self.can_dial(peer_gateway_id, now)
            }
            LinkDirection::Inbound => {
                peer_gateway_id < local_gateway_id || !self.can_dial(peer_gateway_id, now)
            }
        }
    }

    pub fn can_dial(&self, peer_gateway_id: &str, now: DateTime<Utc>) -> bool {
        self.peers
            .get(peer_gateway_id)
            .and_then(|state| state.cooldown_until)
            .map(|until| now >= until)
            .unwrap_or(true)
    }

    pub fn record_connect_drop(
        &mut self,
        peer_gateway_id: &str,
        now: DateTime<Utc>,
        cooldown: Duration,
    ) -> Option<LinkFlappingEvent> {
        let window = chrono::Duration::from_std(self.window).unwrap_or(chrono::Duration::MAX);
        let state = self
            .peers
            .entry(peer_gateway_id.to_string())
            .or_insert_with(|| PeerFlapState {
                drops: VecDeque::new(),
                cooldown_until: None,
            });
        while state
            .drops
            .front()
            .map(|drop_at| now.signed_duration_since(*drop_at) > window)
            .unwrap_or(false)
        {
            state.drops.pop_front();
        }
        state.drops.push_back(now);
        if state.drops.len() < 3 {
            return None;
        }
        if state
            .cooldown_until
            .map(|until| until > now)
            .unwrap_or(false)
        {
            return None;
        }
        let cooldown = chrono::Duration::from_std(cooldown).unwrap_or(chrono::Duration::MAX);
        let cooldown_until = now + cooldown;
        state.cooldown_until = Some(cooldown_until);
        Some(LinkFlappingEvent {
            kind: "link_flapping",
            peer_gateway_id: peer_gateway_id.to_string(),
            cooldown_until,
        })
    }

    pub fn health(&self, now: DateTime<Utc>) -> Value {
        let mut peers = Map::new();
        for (peer, state) in &self.peers {
            let state_name = match state.cooldown_until {
                Some(until) if now < until => "cooldown",
                _ => "ready",
            };
            peers.insert(
                peer.clone(),
                json!({
                    "state": state_name,
                    "recent_connect_drops": state.drops.len(),
                    "cooldown_until": state.cooldown_until,
                }),
            );
        }
        Value::Object(peers)
    }
}

pub fn discover_outbound_peers<'a>(
    local_gateway_id: &str,
    rows: &'a [GatewayRow],
) -> Vec<&'a GatewayRow> {
    rows.iter()
        .filter(|row| row.gateway_id != local_gateway_id)
        .filter(|row| row.endpoint_url.is_some())
        .filter(|row| local_gateway_id < row.gateway_id.as_str())
        .collect()
}

pub async fn dial_gateway(
    row: &GatewayRow,
) -> Result<Side<tokio_rustls::client::TlsStream<TcpStream>>, AppError> {
    let url = row.endpoint_url.as_deref().ok_or_else(|| {
        AppError::new(
            "GATEWAY_ENDPOINT_MISSING",
            ExitCode::BrokerUnavailable,
            format!("gateway {} has no endpoint_url", row.gateway_id),
        )
    })?;
    let cert_fpr = row.cert_fpr.as_deref().ok_or_else(|| {
        AppError::new(
            "GATEWAY_CERT_MISSING",
            ExitCode::Precondition,
            format!("gateway {} has no cert_fpr", row.gateway_id),
        )
    })?;
    connect_pinned(url, cert_fpr).await
}

pub async fn dial_gateway_io<S>(
    stream: S,
    row: &GatewayRow,
) -> Result<Side<tokio_rustls::client::TlsStream<S>>, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let url = row.endpoint_url.as_deref().ok_or_else(|| {
        AppError::new(
            "GATEWAY_ENDPOINT_MISSING",
            ExitCode::BrokerUnavailable,
            format!("gateway {} has no endpoint_url", row.gateway_id),
        )
    })?;
    let cert_fpr = row.cert_fpr.as_deref().ok_or_else(|| {
        AppError::new(
            "GATEWAY_CERT_MISSING",
            ExitCode::Precondition,
            format!("gateway {} has no cert_fpr", row.gateway_id),
        )
    })?;
    connect_pinned_io(stream, url, cert_fpr).await
}

pub async fn accept_once_io<S>(
    stream: S,
    tls: TlsIdentity,
) -> Result<Side<tokio_rustls::server::TlsStream<S>>, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    accept_pinned_io(stream, tls).await
}

pub async fn accept_loop(
    listener: TcpListener,
    tls: TlsIdentity,
    accepted: mpsc::Sender<SocketAddr>,
    token: CancellationToken,
) -> Result<(), AppError> {
    let mut links = JoinSet::new();
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                while let Some(joined) = links.join_next().await {
                    joined.map_err(join_err)??;
                }
                return Ok(());
            }
            accepted_socket = listener.accept() => {
                let (stream, addr) = accepted_socket.map_err(|e| AppError::new(
                    "GATEWAY_ACCEPT",
                    ExitCode::BrokerUnavailable,
                    format!("accepting gateway link: {e}"),
                ))?;
                let tls = tls.clone();
                let accepted = accepted.clone();
                links.spawn(async move {
                    let mut side = accept_pinned(stream, tls).await?;
                    let _ = accepted.send(addr).await;
                    let _ = side.close().await;
                    Ok::<(), AppError>(())
                });
            }
            Some(joined) = links.join_next(), if !links.is_empty() => {
                joined.map_err(join_err)??;
            }
        }
    }
}

fn join_err(err: tokio::task::JoinError) -> AppError {
    AppError::new(
        "GATEWAY_LINK_TASK",
        ExitCode::Internal,
        format!("gateway link task exited unexpectedly: {err}"),
    )
}

#[derive(Clone, Debug, PartialEq)]
pub enum InboundAdmission {
    Accepted,
    Backpressure,
    Busy(Frame),
}

impl InboundAdmission {
    pub fn is_accepted(&self) -> bool {
        matches!(self, InboundAdmission::Accepted)
    }

    pub fn is_backpressure(&self) -> bool {
        matches!(self, InboundAdmission::Backpressure)
    }

    pub fn err_frame(&self) -> Option<Frame> {
        match self {
            InboundAdmission::Busy(frame) => Some(frame.clone()),
            InboundAdmission::Accepted | InboundAdmission::Backpressure => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InboundMailboxes {
    payload_cap: usize,
    payloads: VecDeque<u64>,
    payload_full_since: Option<DateTime<Utc>>,
    backpressure_window: Duration,
    presence: VecDeque<u64>,
}

impl InboundMailboxes {
    pub fn new(payload_cap: usize, backpressure_window: Duration) -> Self {
        Self {
            payload_cap,
            payloads: VecDeque::with_capacity(payload_cap),
            payload_full_since: None,
            backpressure_window,
            presence: VecDeque::with_capacity(PRESENCE_MAILBOX_CAPACITY),
        }
    }

    pub fn offer_note(&mut self, seq: u64, now: DateTime<Utc>) -> InboundAdmission {
        self.offer_payload(seq, now)
    }

    pub fn offer_feed(&mut self, seq: u64, now: DateTime<Utc>) -> InboundAdmission {
        self.offer_payload(seq, now)
    }

    pub fn offer_presence(&mut self, seq: u64, _now: DateTime<Utc>) -> InboundAdmission {
        if self.presence.len() == PRESENCE_MAILBOX_CAPACITY {
            self.presence.pop_front();
        }
        self.presence.push_back(seq);
        InboundAdmission::Accepted
    }

    pub fn lossy_payload_drops(&self) -> u64 {
        0
    }

    pub fn drain_payloads(&mut self) -> Vec<u64> {
        self.payload_full_since = None;
        self.payloads.drain(..).collect()
    }

    pub fn drain_presence(&mut self) -> Vec<u64> {
        self.presence.drain(..).collect()
    }

    fn offer_payload(&mut self, seq: u64, now: DateTime<Utc>) -> InboundAdmission {
        if self.payloads.len() >= self.payload_cap {
            let since = self.payload_full_since.get_or_insert(now);
            let limit = chrono::Duration::from_std(self.backpressure_window)
                .unwrap_or(chrono::Duration::MAX);
            if now.signed_duration_since(*since) >= limit {
                return InboundAdmission::Busy(Frame::Err {
                    code: "BUSY".to_string(),
                    detail: format!(
                        "inbound payload mailbox remained full for {}s",
                        self.backpressure_window.as_secs()
                    ),
                });
            }
            return InboundAdmission::Backpressure;
        }
        self.payloads.push_back(seq);
        if self.payloads.len() == self.payload_cap {
            self.payload_full_since = Some(now);
        }
        InboundAdmission::Accepted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShedOutcome {
    pub queued: bool,
    pub log_outbound_shed: bool,
}

#[derive(Clone, Debug)]
pub struct OutboundQueue {
    capacity: usize,
    frames: VecDeque<Value>,
    outbound_shed: u64,
    last_shed_log_at: Option<DateTime<Utc>>,
}

impl OutboundQueue {
    pub fn new() -> Self {
        Self::with_capacity(OUTBOUND_QUEUE_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            frames: VecDeque::with_capacity(capacity),
            outbound_shed: 0,
            last_shed_log_at: None,
        }
    }

    pub fn enqueue_feed(&mut self, frame: Value, now: DateTime<Utc>) -> ShedOutcome {
        self.enqueue_lossy(frame, now)
    }

    pub fn enqueue_presence(&mut self, frame: Value, now: DateTime<Utc>) -> ShedOutcome {
        self.enqueue_lossy(frame, now)
    }

    pub fn outbound_shed_count(&self) -> u64 {
        self.outbound_shed
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn drain(&mut self) -> Vec<Value> {
        self.frames.drain(..).collect()
    }

    fn enqueue_lossy(&mut self, frame: Value, now: DateTime<Utc>) -> ShedOutcome {
        if self.frames.len() < self.capacity {
            self.frames.push_back(frame);
            return ShedOutcome {
                queued: true,
                log_outbound_shed: false,
            };
        }
        self.outbound_shed = self.outbound_shed.saturating_add(1);
        let log_outbound_shed = self
            .last_shed_log_at
            .map(|last| now.signed_duration_since(last) >= SHED_LOG_INTERVAL)
            .unwrap_or(true);
        if log_outbound_shed {
            self.last_shed_log_at = Some(now);
        }
        ShedOutcome {
            queued: false,
            log_outbound_shed,
        }
    }
}

impl Default for OutboundQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LinkWriteOp {
    Ping(Vec<u8>),
    Close(Frame),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkRuntimeEvent {
    ClosedForLiveness,
}

#[derive(Clone, Copy, Debug)]
pub struct LinkLiveness {
    ping_every: Duration,
}

impl LinkLiveness {
    pub fn new(ping_every: Duration) -> Self {
        Self { ping_every }
    }

    pub async fn run(
        self,
        mut read_activity: mpsc::Receiver<()>,
        writer: mpsc::Sender<LinkWriteOp>,
        events: mpsc::Sender<LinkRuntimeEvent>,
        token: CancellationToken,
    ) {
        let mut last_read = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                activity = read_activity.recv() => {
                    if activity.is_none() {
                        return;
                    }
                    last_read = tokio::time::Instant::now();
                }
                _ = tokio::time::sleep(self.ping_every) => {
                    if last_read.elapsed() > self.ping_every.saturating_mul(2) {
                        let _ = writer.send(LinkWriteOp::Close(Frame::Err {
                            code: "LIVENESS_TIMEOUT".to_string(),
                            detail: format!("no read activity for {}s", self.ping_every.as_secs().saturating_mul(2)),
                        })).await;
                        let _ = events.send(LinkRuntimeEvent::ClosedForLiveness).await;
                        return;
                    }
                    if writer.send(LinkWriteOp::Ping(Vec::new())).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LinkSpawnState {
    pub peer_gateway_id: String,
    pub cursors: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkTaskExit {
    pub peer_gateway_id: String,
}

pub struct LinkTaskSet {
    tasks: JoinSet<LinkTaskExit>,
    spawned: Vec<LinkSpawnState>,
}

impl LinkTaskSet {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            spawned: Vec::new(),
        }
    }

    pub fn spawn_pending(
        &mut self,
        peer_gateway_id: impl Into<String>,
        cursors: Value,
        token: CancellationToken,
    ) {
        let peer_gateway_id = peer_gateway_id.into();
        self.spawned.push(LinkSpawnState {
            peer_gateway_id: peer_gateway_id.clone(),
            cursors,
        });
        self.tasks.spawn(async move {
            token.cancelled().await;
            LinkTaskExit { peer_gateway_id }
        });
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn spawned_peer_states(&self) -> Vec<LinkSpawnState> {
        self.spawned.clone()
    }

    pub async fn join_next(&mut self) -> Option<LinkTaskExit> {
        loop {
            let joined = self.tasks.join_next().await?;
            match joined {
                Ok(exit) => return Some(exit),
                Err(err) if err.is_cancelled() => continue,
                Err(_) => continue,
            }
        }
    }
}

impl Default for LinkTaskSet {
    fn default() -> Self {
        Self::new()
    }
}

pub fn jittered_redial_backoff<R: Rng + ?Sized>(
    attempt: u32,
    cap: Duration,
    rng: &mut R,
) -> Duration {
    let base_ms = 1_000u128;
    let factor = 2u128.saturating_pow(attempt.min(32));
    let cap_ms = cap.as_millis().max(1);
    let upper = base_ms.saturating_mul(factor).min(cap_ms).max(1);
    Duration::from_millis(rng.gen_range(0..=upper) as u64)
}
