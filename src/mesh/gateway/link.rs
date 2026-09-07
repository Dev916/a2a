use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::gateway::{limits, notes_wire, presence_wire, replicate};
use crate::mesh::registry::GatewayRow;
use crate::mesh::row_sig::{endpoint_conflicts, Disposition, RowVerdict};
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
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub const PRESENCE_MAILBOX_CAPACITY: usize = 64;
pub const OUTBOUND_QUEUE_CAPACITY: usize = 1024;
pub const SHED_LOG_INTERVAL: chrono::Duration = chrono::Duration::seconds(60);
pub const LINK_OUTBOUND_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkDirection {
    Inbound,
    Outbound,
}

impl LinkDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            LinkDirection::Inbound => "inbound",
            LinkDirection::Outbound => "outbound",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkHealth {
    pub peer: String,
    pub direction: LinkDirection,
    pub established_at: DateTime<Utc>,
    pub last_rx: Option<DateTime<Utc>>,
    pub last_tx: Option<DateTime<Utc>>,
    pub frames_rx: u64,
    pub frames_tx: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkHealthEvent {
    Up {
        peer: String,
        direction: LinkDirection,
        established_at: DateTime<Utc>,
    },
    Stat {
        peer: String,
        direction: LinkDirection,
        last_rx: Option<DateTime<Utc>>,
        last_tx: Option<DateTime<Utc>>,
        frames_rx: u64,
        frames_tx: u64,
    },
    Down {
        peer: String,
        direction: LinkDirection,
    },
}

#[derive(Clone, Debug)]
pub struct LinkObserver {
    tx: mpsc::Sender<LinkHealthEvent>,
}

impl LinkObserver {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<LinkHealthEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }

    pub async fn send(&self, event: LinkHealthEvent) {
        let _ = self.tx.send(event).await;
    }
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

const ROOT_MISMATCH_THRESHOLD: u32 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootMismatchEvent {
    pub kind: &'static str,
    pub peer: String,
    pub count: u32,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct RootMismatchPeer {
    count: u32,
    last_refusal_at: DateTime<Utc>,
    suspected: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RootMismatchWatch {
    peers: BTreeMap<String, RootMismatchPeer>,
}

impl RootMismatchWatch {
    pub fn observe(
        &mut self,
        peer: &str,
        code: &str,
        now: DateTime<Utc>,
    ) -> Option<RootMismatchEvent> {
        if !matches!(code, "TOKEN_INVALID" | "GATEWAY_TOKEN_INVALID") {
            self.clear(peer);
            return None;
        }
        let state = self
            .peers
            .entry(peer.to_string())
            .or_insert(RootMismatchPeer {
                count: 0,
                last_refusal_at: now,
                suspected: false,
            });
        state.count = state.count.saturating_add(1);
        state.last_refusal_at = now;
        if state.suspected || state.count < ROOT_MISMATCH_THRESHOLD {
            return None;
        }
        state.suspected = true;
        Some(RootMismatchEvent {
            kind: "root_mismatch_suspected",
            peer: peer.to_string(),
            count: state.count,
            observed_at: now,
        })
    }

    pub fn clear(&mut self, peer: &str) {
        self.peers.remove(peer);
    }

    pub fn health(&self, now: DateTime<Utc>) -> Value {
        let mut peers = Map::new();
        for (peer, state) in &self.peers {
            if !state.suspected {
                continue;
            }
            peers.insert(
                peer.clone(),
                json!({
                    "count": state.count,
                    "last_refusal_at": state.last_refusal_at,
                    "seconds_since_last": now
                        .signed_duration_since(state.last_refusal_at)
                        .num_seconds()
                        .max(0),
                }),
            );
        }
        Value::Object(peers)
    }
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

    pub fn reset_peer(&mut self, peer_gateway_id: &str) {
        self.peers.remove(peer_gateway_id);
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
    rows: &'a [(GatewayRow, RowVerdict)],
    require_signed_rows: bool,
) -> Vec<&'a GatewayRow> {
    let conflicts = endpoint_conflicts(rows);
    rows.iter()
        .filter(|(_row, verdict)| {
            verdict.disposition(require_signed_rows) != Disposition::Refused
        })
        .map(|(row, _verdict)| row)
        .filter(|row| !conflicts.contains_key(&row.gateway_id))
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

pub async fn accept_and_handshake<S>(
    stream: S,
    tls: TlsIdentity,
    identity: crate::mesh::wire::HandshakeIdentity,
    roots: crate::mesh::keys::RootSet,
    clock: &dyn crate::mesh::keys::Clock,
    deadline: Duration,
) -> Result<
    (
        Side<tokio_rustls::server::TlsStream<S>>,
        crate::mesh::keys::TokenClaims,
    ),
    AppError,
>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let handshake = async move {
        let mut side = accept_once_io(stream, tls).await?;
        let peer = crate::mesh::wire::handshake_acceptor(&mut side, &identity, &roots, clock).await?;
        Ok::<_, AppError>((side, peer))
    };
    tokio::time::timeout(deadline, handshake)
        .await
        .map_err(|_| {
            AppError::new(
                "PREAUTH_LIMIT",
                ExitCode::Precondition,
                format!(
                    "inbound gateway handshake did not finish within {}ms",
                    deadline.as_millis()
                ),
            )
            .remedy("retry after the peer can complete TLS and mesh authentication promptly")
        })?
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
                let link_token = token.child_token();
                links.spawn(async move {
                    let mut side = accept_pinned(stream, tls).await?;
                    let _ = accepted.send(addr).await;
                    link_token.cancelled().await;
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

#[derive(Clone)]
pub struct LinkStateHandle {
    inner: Arc<LinkStateInner>,
}

/// A read-only cursor view whose reads are copied through the owning link handle.
#[derive(Clone)]
pub struct PeerCursorView {
    state: LinkStateHandle,
    peer_gateway_id: String,
}

struct LinkStateInner {
    peers: Mutex<BTreeMap<String, PeerLinkState>>,
    buckets: Mutex<BTreeMap<String, limits::PeerBuckets>>,
    cursors: Mutex<BTreeMap<String, replicate::CursorStore>>,
    damping: Mutex<PeerDamping>,
    health: Mutex<replicate::ReplicationHealth>,
    registry_cache: Mutex<()>,
    home: A2aHome,
    limits_cfg: StdRwLock<crate::config::MeshLimitsConfig>,
}

struct PeerLinkState {
    inbound: Option<LiveLink>,
    outbound: Option<LiveLink>,
    peer_version: Option<String>,
    handshakes: VecDeque<DateTime<Utc>>,
}

#[derive(Clone)]
struct LiveLink {
    tx: mpsc::Sender<Frame>,
    sent_cursors: BTreeMap<String, u64>,
}

impl PeerLinkState {
    fn selected(&self) -> Option<&LiveLink> {
        self.outbound.as_ref().or(self.inbound.as_ref())
    }

    fn selected_mut(&mut self) -> Option<&mut LiveLink> {
        if self.outbound.is_some() {
            self.outbound.as_mut()
        } else {
            self.inbound.as_mut()
        }
    }

    fn slot_mut(&mut self, direction: LinkDirection) -> &mut Option<LiveLink> {
        match direction {
            LinkDirection::Inbound => &mut self.inbound,
            LinkDirection::Outbound => &mut self.outbound,
        }
    }

    fn is_live(&self) -> bool {
        self.inbound.is_some() || self.outbound.is_some()
    }
}

impl LinkStateHandle {
    pub fn new(home: &A2aHome, damping_window: Duration) -> Result<Self, AppError> {
        replicate::CursorStore::load_or_rebuild(home)?;
        Ok(Self {
            inner: Arc::new(LinkStateInner {
                peers: Mutex::new(BTreeMap::new()),
                buckets: Mutex::new(BTreeMap::new()),
                cursors: Mutex::new(BTreeMap::new()),
                damping: Mutex::new(PeerDamping::new(damping_window)),
                health: Mutex::new(replicate::ReplicationHealth::default()),
                registry_cache: Mutex::new(()),
                home: home.clone(),
                limits_cfg: StdRwLock::new(crate::config::MeshLimitsConfig::default()),
            }),
        })
    }

    fn configure_limits(&self, cfg: &crate::config::MeshLimitsConfig) {
        *self
            .inner
            .limits_cfg
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = cfg.clone();
    }

    fn new_peer_state(&self, peer_gateway_id: &str) -> PeerLinkState {
        PeerLinkState {
            inbound: None,
            outbound: None,
            peer_version: presence_wire::cached_peer_version(&self.inner.home, peer_gateway_id),
            handshakes: VecDeque::new(),
        }
    }

    fn new_peer_buckets(&self) -> limits::PeerBuckets {
        let cfg = self
            .inner
            .limits_cfg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        limits::PeerBuckets::new(&cfg)
    }

    fn new_peer_cursors(&self, peer_gateway_id: &str) -> replicate::CursorStore {
        let owned = vec![peer_gateway_id.to_string()];
        let mut cursors = replicate::CursorStore::load_or_rebuild(&self.inner.home)
            .unwrap_or_else(|_| replicate::CursorStore::default());
        cursors.retain_origins(&owned);
        cursors
    }

    async fn ensure_peer_ingress(&self, peer_gateway_id: &str) {
        {
            let buckets = self.inner.buckets.lock().await;
            if !buckets.contains_key(peer_gateway_id) {
                drop(buckets);
                let candidate = self.new_peer_buckets();
                self.inner
                    .buckets
                    .lock()
                    .await
                    .entry(peer_gateway_id.to_string())
                    .or_insert(candidate);
            }
        }
        {
            let cursors = self.inner.cursors.lock().await;
            if !cursors.contains_key(peer_gateway_id) {
                drop(cursors);
                let candidate = self.new_peer_cursors(peer_gateway_id);
                self.inner
                    .cursors
                    .lock()
                    .await
                    .entry(peer_gateway_id.to_string())
                    .or_insert(candidate);
            }
        }
    }

    async fn with_peer_ingress<T>(
        &self,
        peer_gateway_id: &str,
        op: impl FnOnce(&mut limits::PeerBuckets, &mut replicate::CursorStore) -> T,
    ) -> T {
        self.ensure_peer_ingress(peer_gateway_id).await;
        loop {
            let mut cursors = self.inner.cursors.lock().await;
            if let Ok(mut buckets) = self.inner.buckets.try_lock() {
                let buckets = buckets
                    .get_mut(peer_gateway_id)
                    .expect("ensured peer buckets are present");
                let cursors = cursors
                    .get_mut(peer_gateway_id)
                    .expect("ensured peer cursors are present");
                return op(buckets, cursors);
            }
            drop(cursors);
            tokio::task::yield_now().await;
        }
    }

    pub async fn register(
        &self,
        peer_gateway_id: String,
        direction: LinkDirection,
        tx: mpsc::Sender<Frame>,
    ) -> Result<(), AppError> {
        let candidate = self.new_peer_state(&peer_gateway_id);
        let handshake_limit = self
            .inner
            .limits_cfg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .handshakes_per_min_per_peer;
        let now = Utc::now();
        let mut peers = self.inner.peers.lock().await;
        let peer = peers.entry(peer_gateway_id.clone()).or_insert(candidate);
        while peer
            .handshakes
            .front()
            .is_some_and(|at| now.signed_duration_since(*at) >= chrono::Duration::minutes(1))
        {
            peer.handshakes.pop_front();
        }
        if peer.handshakes.len() as u64 >= handshake_limit {
            return Err(session_limit_error(
                &peer_gateway_id,
                "authenticated handshake rate exceeded",
            ));
        }
        peer.handshakes.push_back(now);
        let slot = peer.slot_mut(direction);
        if slot.is_some() {
            return Err(session_limit_error(
                &peer_gateway_id,
                &format!("an incumbent {} session is already registered", direction.as_str()),
            ));
        }
        *slot = Some(LiveLink {
            tx,
            sent_cursors: BTreeMap::new(),
        });
        drop(peers);
        self.ensure_peer_ingress(&peer_gateway_id).await;
        Ok(())
    }

    pub async fn deregister(&self, peer_gateway_id: &str, direction: LinkDirection) {
        if let Some(peer) = self.inner.peers.lock().await.get_mut(peer_gateway_id) {
            *peer.slot_mut(direction) = None;
        }
    }

    pub async fn is_live(&self, peer_gateway_id: &str) -> bool {
        self.inner
            .peers
            .lock()
            .await
            .get(peer_gateway_id)
            .is_some_and(PeerLinkState::is_live)
    }

    pub async fn live_peers(&self) -> Vec<String> {
        self.inner
            .peers
            .lock()
            .await
            .iter()
            .filter(|(_, peer)| peer.is_live())
            .map(|(gateway_id, _)| gateway_id.clone())
            .collect()
    }

    pub async fn sender(&self, peer_gateway_id: &str) -> Option<mpsc::Sender<Frame>> {
        self.inner
            .peers
            .lock()
            .await
            .get(peer_gateway_id)
            .and_then(PeerLinkState::selected)
            .map(|link| link.tx.clone())
    }

    pub async fn send(&self, peer_gateway_id: &str, frame: Frame) -> bool {
        let tx = self.sender(peer_gateway_id).await;
        match tx {
            Some(tx) => tx.send(frame).await.is_ok(),
            None => false,
        }
    }

    pub async fn broadcast(&self, frame: Frame) {
        let senders = self
            .inner
            .peers
            .lock()
            .await
            .values()
            .filter_map(PeerLinkState::selected)
            .map(|link| link.tx.clone())
            .collect::<Vec<_>>();
        for tx in senders {
            let _ = tx.send(frame.clone()).await;
        }
    }

    pub async fn can_dial(&self, peer_gateway_id: &str, now: DateTime<Utc>) -> bool {
        self.inner
            .damping
            .lock()
            .await
            .can_dial(peer_gateway_id, now)
    }

    pub async fn record_drop(
        &self,
        peer_gateway_id: &str,
        now: DateTime<Utc>,
        cooldown: Duration,
    ) -> Option<LinkFlappingEvent> {
        self.inner
            .damping
            .lock()
            .await
            .record_connect_drop(peer_gateway_id, now, cooldown)
    }

    pub async fn reset_peer_damping(&self, peer_gateway_id: &str) {
        self.inner.damping.lock().await.reset_peer(peer_gateway_id);
    }

    pub async fn health(&self) -> replicate::ReplicationHealth {
        self.inner.health.lock().await.clone()
    }

    async fn peer_version(&self, peer_gateway_id: &str) -> Option<String> {
        self.inner
            .peers
            .lock()
            .await
            .get(peer_gateway_id)
            .and_then(|peer| peer.peer_version.clone())
    }

    pub async fn limits_health(&self) -> BTreeMap<String, limits::PeerLimitHealth> {
        let buckets = self.inner.buckets.lock().await;
        limits::health_by_peer(&buckets)
    }

    pub async fn peer_cursors(&self, peer_gateway_id: &str) -> PeerCursorView {
        self.ensure_peer_ingress(peer_gateway_id).await;
        PeerCursorView {
            state: self.clone(),
            peer_gateway_id: peer_gateway_id.to_string(),
        }
    }

    async fn peer_cursor_snapshot(&self, peer_gateway_id: &str) -> replicate::CursorStore {
        self.ensure_peer_ingress(peer_gateway_id).await;
        self.inner
            .cursors
            .lock()
            .await
            .get(peer_gateway_id)
            .expect("ensured peer cursors are present")
            .clone()
    }

    fn try_peer_cursor_snapshot(
        &self,
        peer_gateway_id: &str,
    ) -> Result<replicate::CursorStore, tokio::sync::TryLockError> {
        let cursors = self.inner.cursors.try_lock()?;
        Ok(cursors
            .get(peer_gateway_id)
            .expect("peer cursor views are created only after initialization")
            .clone())
    }

    pub async fn compact_feed(
        &self,
        cfg: &Config,
        now: DateTime<Utc>,
        retain: chrono::Duration,
    ) -> Result<replicate::FeedCompaction, AppError> {
        let mut cursors = self.inner.cursors.lock().await;
        replicate::compact_feed_with_peer_cursors(
            &self.inner.home,
            cfg,
            now,
            retain,
            &mut cursors,
        )
    }

    pub async fn refresh_registry_cache(
        &self,
        home: &A2aHome,
        rows: &[(GatewayRow, RowVerdict)],
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let _guard = self.inner.registry_cache.lock().await;
        presence_wire::refresh_registry_cache(home, rows, now)?;
        let versions = rows
            .iter()
            .map(|(row, _)| (row.gateway_id.clone(), row.version.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut peers = self.inner.peers.lock().await;
        for (gateway_id, peer) in peers.iter_mut() {
            peer.peer_version = versions.get(gateway_id).cloned();
        }
        Ok(())
    }
}

impl PeerCursorView {
    pub async fn lock(&self) -> replicate::CursorStore {
        self.state
            .peer_cursor_snapshot(&self.peer_gateway_id)
            .await
    }

    pub fn try_lock(
        &self,
    ) -> Result<replicate::CursorStore, tokio::sync::TryLockError> {
        self.state.try_peer_cursor_snapshot(&self.peer_gateway_id)
    }
}

fn session_limit_error(peer_gateway_id: &str, detail: &str) -> AppError {
    AppError::new(
        "SESSION_LIMIT",
        ExitCode::Precondition,
        format!("refusing surplus session for {peer_gateway_id}: {detail}"),
    )
    .remedy("keep at most one inbound and one outbound session per authenticated gateway")
}

#[derive(Clone)]
pub struct LinkSessionParams {
    pub home: A2aHome,
    pub cfg: Config,
    pub local_gateway_id: String,
    pub peer_gateway_id: String,
    pub direction: LinkDirection,
    pub state: LinkStateHandle,
    pub token: CancellationToken,
    pub observer: Option<LinkObserver>,
    pub ping_every: Duration,
    pub mailbox_payload_cap: usize,
    pub mailbox_backpressure_window: Duration,
}

impl LinkSessionParams {
    pub fn new(
        home: A2aHome,
        cfg: Config,
        local_gateway_id: impl Into<String>,
        peer_gateway_id: impl Into<String>,
        direction: LinkDirection,
        state: LinkStateHandle,
        token: CancellationToken,
    ) -> Self {
        let ping_every = Duration::from_secs(cfg.mesh.gateway.ping_secs.max(1));
        Self {
            home,
            cfg,
            local_gateway_id: local_gateway_id.into(),
            peer_gateway_id: peer_gateway_id.into(),
            direction,
            state,
            token,
            observer: None,
            ping_every,
            mailbox_payload_cap: 64,
            mailbox_backpressure_window: ping_every.saturating_mul(2),
        }
    }

    pub fn with_observer(mut self, observer: LinkObserver) -> Self {
        self.observer = Some(observer);
        self
    }
}

pub async fn run_link_session<S>(side: Side<S>, params: LinkSessionParams) -> Result<(), AppError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let LinkSessionParams {
        home,
        cfg,
        local_gateway_id,
        peer_gateway_id,
        direction,
        state,
        token,
        observer,
        ping_every,
        mailbox_payload_cap,
        mailbox_backpressure_window,
    } = params;
    state.configure_limits(&cfg.mesh.limits);
    let (mut writer, mut reader) = side.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Frame>(LINK_OUTBOUND_CHANNEL_CAPACITY);
    if let Err(error) = state
        .register(peer_gateway_id.clone(), direction, out_tx.clone())
        .await
    {
        limits::log_limit_refusal(&home, &peer_gateway_id, error.code, &error.message);
        let _ = writer.close().await;
        return Err(error);
    }
    if let Some(observer) = &observer {
        observer
            .send(LinkHealthEvent::Up {
                peer: peer_gateway_id.clone(),
                direction,
                established_at: Utc::now(),
            })
            .await;
    }

    let (activity_tx, activity_rx) = mpsc::channel(32);
    let (write_op_tx, mut write_op_rx) = mpsc::channel(32);
    let close_tx = write_op_tx.clone();
    let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
    let liveness_token = token.child_token();
    let liveness = tokio::spawn(LinkLiveness::new(ping_every).run(
        activity_rx,
        write_op_tx,
        runtime_tx,
        liveness_token,
    ));

    let writer_token = token.child_token();
    let writer_observer = observer.clone();
    let writer_peer = peer_gateway_id.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_token.cancelled() => {
                    let _ = writer.close().await;
                    return Ok(());
                }
                Some(frame) = out_rx.recv() => {
                    writer.send(&frame).await?;
                    if let Some(observer) = &writer_observer {
                        observer.send(LinkHealthEvent::Stat {
                            peer: writer_peer.clone(),
                            direction,
                            last_rx: None,
                            last_tx: Some(Utc::now()),
                            frames_rx: 0,
                            frames_tx: 1,
                        }).await;
                    }
                }
                Some(op) = write_op_rx.recv() => {
                    match op {
                        LinkWriteOp::Ping(payload) => writer.ping(payload).await?,
                        LinkWriteOp::Close(frame) => {
                            if writer.send(&frame).await.is_ok() {
                                if let Some(observer) = &writer_observer {
                                    observer.send(LinkHealthEvent::Stat {
                                        peer: writer_peer.clone(),
                                        direction,
                                        last_rx: None,
                                        last_tx: Some(Utc::now()),
                                        frames_rx: 0,
                                        frames_tx: 1,
                                    }).await;
                                }
                            }
                            let _ = writer.close().await;
                            return Ok(());
                        }
                    }
                }
                else => {
                    let _ = writer.close().await;
                    return Ok(());
                }
            }
        }
    });

    let mut initial_digest = replicate::digest_for_home(&home)?;
    // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
    initial_digest.retain(|origin| origin.origin == local_gateway_id);
    let _ = out_tx
        .send(Frame::Digest {
            origins: initial_digest,
        })
        .await;
    let mut mailboxes = InboundMailboxes::new(mailbox_payload_cap, mailbox_backpressure_window);
    // Both bounds are per-link; evicted gap work is deferred to bounded anti-entropy repair.
    let mut reorder = replicate::FeedReorderBuffers::new_with_total(
        replicate::DEFAULT_REORDER_BUFFER_CAP,
        replicate::DEFAULT_REORDER_TOTAL_CAP,
        1,
    );
    let result = loop {
        tokio::select! {
            _ = token.cancelled() => break Ok(()),
            event = runtime_rx.recv() => {
                if matches!(event, Some(LinkRuntimeEvent::ClosedForLiveness) | None) {
                    break Ok(());
                }
            }
            joined = &mut writer_task => {
                break match joined {
                    Ok(result) => result,
                    Err(err) => Err(join_err(err)),
                };
            }
            frame = reader.recv() => {
                let Some(frame) = frame? else {
                    break Ok(());
                };
                if let Some(observer) = &observer {
                    observer.send(LinkHealthEvent::Stat {
                        peer: peer_gateway_id.clone(),
                        direction,
                        last_rx: Some(Utc::now()),
                        last_tx: None,
                        frames_rx: 1,
                        frames_tx: 0,
                    }).await;
                }
                let _ = activity_tx.send(()).await;
                if let Some(err) = admit_frame(&mut mailboxes, &frame).await {
                    let _ = close_tx.send(LinkWriteOp::Close(err)).await;
                    let joined = (&mut writer_task).await;
                    break match joined {
                        Ok(result) => result,
                        Err(err) => Err(join_err(err)),
                    };
                }
                handle_inbound_frame(
                    InboundCtx {
                        home: &home,
                        cfg: &cfg,
                        local_gateway_id: &local_gateway_id,
                        peer_gateway_id: &peer_gateway_id,
                    },
                    &state,
                    &mut reorder,
                    &out_tx,
                    frame,
                )
                .await?;
            }
        }
    };

    token.cancel();
    liveness.abort();
    state.deregister(&peer_gateway_id, direction).await;
    if let Some(observer) = &observer {
        observer
            .send(LinkHealthEvent::Down {
                peer: peer_gateway_id.clone(),
                direction,
            })
            .await;
    }
    let _ = state
        .record_drop(
            &peer_gateway_id,
            Utc::now(),
            Duration::from_secs(cfg.mesh.gateway.outbound_retry_max_secs),
        )
        .await;
    result
}

async fn admit_frame(mailboxes: &mut InboundMailboxes, frame: &Frame) -> Option<Frame> {
    let now = Utc::now();
    let admission = match frame {
        Frame::Feed { .. } => mailboxes.offer_feed(0, now),
        Frame::Note { .. } => mailboxes.offer_note(0, now),
        Frame::Presence { .. } => mailboxes.offer_presence(0, now),
        _ => InboundAdmission::Accepted,
    };
    match admission {
        InboundAdmission::Accepted => {
            let _ = mailboxes.drain_payloads();
            None
        }
        InboundAdmission::Backpressure => {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = mailboxes.drain_payloads();
            None
        }
        InboundAdmission::Busy(frame) => Some(frame),
    }
}

struct InboundCtx<'a> {
    home: &'a A2aHome,
    cfg: &'a Config,
    local_gateway_id: &'a str,
    peer_gateway_id: &'a str,
}

async fn handle_inbound_frame(
    ctx: InboundCtx<'_>,
    state: &LinkStateHandle,
    reorder: &mut replicate::FeedReorderBuffers,
    out_tx: &mpsc::Sender<Frame>,
    frame: Frame,
) -> Result<(), AppError> {
    let InboundCtx {
        home,
        cfg,
        local_gateway_id,
        peer_gateway_id,
    } = ctx;
    let feed_bytes = if matches!(&frame, Frame::Feed { .. }) {
        serde_json::to_vec(&frame)
            .map_err(|error| {
                AppError::new(
                    "DATA",
                    ExitCode::Internal,
                    format!("serializing feed frame for metering: {error}"),
                )
            })?
            .len() as u64
    } else {
        0
    };
    let peer_version = state.peer_version(peer_gateway_id).await;
    let peer_supports_deferred = limits::peer_supports_deferred(peer_version.as_deref());
    let result = state
        .with_peer_ingress(peer_gateway_id, |buckets, cursors| {
            let now = Utc::now();
            let mut deferred = false;
            let mut deferred_ack = None;
            let mut outbound = Vec::new();
            match frame {
                Frame::Digest { origins } => {
                    let outcome = buckets.allow(limits::BucketKind::Digests, now);
                    if observe_limit_outcome(
                        home,
                        peer_gateway_id,
                        limits::BucketKind::Digests,
                        outcome,
                        peer_supports_deferred,
                        buckets,
                        &mut deferred_ack,
                    ) {
                        // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
                        let origins = origins
                            .into_iter()
                            .take(limits::MAX_ORIGINS_PER_REQUEST_FRAME)
                            .filter(|origin| origin.origin == peer_gateway_id)
                            .collect::<Vec<_>>();
                        let wants = replicate::wants_from_digest(home, cursors, &origins)?;
                        if !wants.is_empty() {
                            outbound.push(Frame::Want { origins: wants });
                        }
                    } else {
                        deferred = true;
                    }
                }
                Frame::Want { origins } => {
                    let outcome = buckets.allow(limits::BucketKind::Wants, now);
                    if observe_limit_outcome(
                        home,
                        peer_gateway_id,
                        limits::BucketKind::Wants,
                        outcome,
                        peer_supports_deferred,
                        buckets,
                        &mut deferred_ack,
                    ) {
                        // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
                        for want in origins
                            .into_iter()
                            .take(limits::MAX_ORIGINS_PER_REQUEST_FRAME)
                            .filter(|want| want.origin == local_gateway_id)
                        {
                            match replicate::entries_for_want_capped(
                                home,
                                &want.origin,
                                want.from_seq,
                                cursors,
                                replicate::DEFAULT_WANT_ENTRY_CAP,
                            )? {
                                replicate::WantReply::Entries(entries) if !entries.is_empty() => {
                                    outbound.push(Frame::Feed {
                                        origin: want.origin,
                                        entries,
                                    });
                                }
                                replicate::WantReply::Entries(_) => {}
                                replicate::WantReply::Compacted { from_seq } => {
                                    outbound.push(replicate::compacted_err(from_seq));
                                }
                            }
                        }
                    } else {
                        deferred = true;
                    }
                }
                Frame::Feed { origin, entries } => {
                    // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
                    if origin != peer_gateway_id {
                        limits::log_origin_unbound(home, peer_gateway_id, &origin);
                        return Ok::<_, AppError>(None);
                    }
                    let bytes_outcome =
                        buckets.charge(limits::BucketKind::FeedBytes, feed_bytes, now);
                    if !observe_limit_outcome(
                        home,
                        peer_gateway_id,
                        limits::BucketKind::FeedBytes,
                        bytes_outcome,
                        peer_supports_deferred,
                        buckets,
                        &mut deferred_ack,
                    ) {
                        deferred = true;
                    } else {
                        let mut frame_deferred = false;
                        let report = replicate::receive_feed_entries_metered(
                            home,
                            cfg,
                            cursors,
                            reorder,
                            &origin,
                            entries,
                            |entry, charge_feed_entry| {
                                let kind = if charge_feed_entry {
                                    limits::BucketKind::FeedEntries
                                } else if entry.refs.get("work").is_some() {
                                    limits::BucketKind::WorkOffers
                                } else {
                                    return true;
                                };
                                let outcome = buckets.allow(kind, now);
                                if !observe_limit_outcome(
                                    home,
                                    peer_gateway_id,
                                    kind,
                                    outcome,
                                    peer_supports_deferred,
                                    buckets,
                                    &mut deferred_ack,
                                ) {
                                    frame_deferred = true;
                                    return false;
                                }
                                true
                            },
                        )?;
                        if report.feed_entry_refunds > 0 {
                            buckets
                                .refund(limits::BucketKind::FeedEntries, report.feed_entry_refunds);
                        }
                        deferred = frame_deferred || report.deferred;
                        cursors.persist_merged(home, std::slice::from_ref(&origin))?;
                    }
                }
                Frame::Note { note } => {
                    let kind = if note.kind == crate::mesh::work_store::WORK_OFFER_KIND {
                        limits::BucketKind::WorkOffers
                    } else {
                        limits::BucketKind::Notes
                    };
                    let outcome = buckets.allow(kind, now);
                    if observe_limit_outcome(
                        home,
                        peer_gateway_id,
                        kind,
                        outcome,
                        peer_supports_deferred,
                        buckets,
                        &mut deferred_ack,
                    ) {
                        let trace_id = note.note_id.clone().unwrap_or_default();
                        let envelope = notes_wire::SpooledNote {
                            schema_version: 1,
                            trace_id: trace_id.clone(),
                            from_gateway: peer_gateway_id.to_string(),
                            to_gateway: local_gateway_id.to_string(),
                            to_peer: note.to.clone(),
                            attempts: 0,
                            next_attempt_after: None,
                            note,
                        };
                        let ack = notes_wire::receive_note(home, cfg, &envelope).unwrap_or(
                            notes_wire::NoteAck::Rejected {
                                code: "ingress_refused".to_string(),
                            },
                        );
                        let outcome = match ack {
                            notes_wire::NoteAck::Delivered | notes_wire::NoteAck::Duplicate => {
                                "delivered".to_string()
                            }
                            notes_wire::NoteAck::Rejected { code } => code,
                        };
                        outbound.push(Frame::Ack {
                            ref_id: trace_id,
                            outcome,
                        });
                    } else {
                        deferred = true;
                    }
                }
                Frame::Ack { ref_id, outcome } => {
                    let _ = notes_wire::handle_ack(home, peer_gateway_id, &ref_id, &outcome)?;
                }
                Frame::Deferred {
                    code,
                    retry_after_secs,
                } => {
                    let _ = notes_wire::defer_spool_for_peer(
                        home,
                        cfg,
                        peer_gateway_id,
                        retry_after_secs,
                        Utc::now(),
                        &mut rand::thread_rng(),
                    )?;
                    limits::log_deferral_received(home, peer_gateway_id, &code, retry_after_secs);
                }
                Frame::Presence { peers } => {
                    presence_wire::merge_presence_frame(home, peer_gateway_id, peers)?;
                }
                Frame::Err { .. } | Frame::Challenge { .. } | Frame::Hello { .. } => {}
            }
            Ok::<_, AppError>(Some((deferred, deferred_ack, outbound)))
        })
        .await?;
    let Some((deferred, deferred_ack, outbound)) = result else {
        return Ok(());
    };
    if deferred {
        if let Some(frame) = deferred_ack {
            let _ = out_tx.try_send(frame);
        }
        return Ok(());
    }
    for frame in outbound {
        let _ = out_tx.send(frame).await;
    }
    Ok(())
}

fn observe_limit_outcome(
    home: &A2aHome,
    peer_gateway_id: &str,
    kind: limits::BucketKind,
    outcome: limits::LimitOutcome,
    peer_supports_deferred: bool,
    buckets: &mut limits::PeerBuckets,
    deferred_ack: &mut Option<Frame>,
) -> bool {
    let limits::LimitOutcome::Deferred {
        transitioned,
        retry_after_secs,
    } = outcome
    else {
        return true;
    };
    if transitioned {
        limits::log_bucket_transition(home, peer_gateway_id, kind, "over");
    }
    if peer_supports_deferred {
        if deferred_ack.is_none() {
            *deferred_ack = Some(limits::deferred_frame(
                "RATE_LIMITED",
                Some(retry_after_secs),
            ));
        }
    } else {
        buckets.record_drop(kind);
    }
    false
}

pub async fn push_watcher_deltas(
    home: &A2aHome,
    cfg: &Config,
    local_gateway_id: &str,
    state: &LinkStateHandle,
) -> Result<usize, AppError> {
    let peers = state.live_peers().await;
    let mut sent = 0usize;
    for peer in peers {
        let (tx, cursors) = {
            let peers = state.inner.peers.lock().await;
            let Some(link) = peers.get(&peer).and_then(PeerLinkState::selected) else {
                continue;
            };
            (link.tx.clone(), link.sent_cursors.clone())
        };
        // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
        let entries = replicate::deltas_for_link(home, local_gateway_id, &cursors)?
            .into_iter()
            .filter(|entry| entry.origin.as_deref() == Some(local_gateway_id))
            .collect::<Vec<_>>();
        if entries.is_empty() {
            continue;
        }
        for (origin, batch) in group_entries(entries) {
            tx.send(Frame::Feed {
                origin: origin.clone(),
                entries: batch.clone(),
            })
            .await
            .map_err(|_| {
                AppError::new(
                    "GATEWAY_LINK_SEND",
                    ExitCode::BrokerUnavailable,
                    format!("link to {peer} is closed"),
                )
            })?;
            sent += batch.len();
            let mut peers = state.inner.peers.lock().await;
            if let Some(link) = peers.get_mut(&peer).and_then(PeerLinkState::selected_mut) {
                let max_seq = batch
                    .iter()
                    .filter_map(|entry| entry.origin_seq)
                    .max()
                    .unwrap_or(0);
                link.sent_cursors
                    .entry(origin)
                    .and_modify(|seq| *seq = (*seq).max(max_seq))
                    .or_insert(max_seq);
            }
        }
    }
    let _ = cfg;
    Ok(sent)
}

pub async fn push_presence_gossip(
    home: &A2aHome,
    local_gateway_id: &str,
    state: &LinkStateHandle,
) -> Result<(), AppError> {
    state
        .broadcast(presence_wire::gossip_frame(home, local_gateway_id)?)
        .await;
    Ok(())
}

pub async fn push_spooled_notes<R: Rng + Send + ?Sized>(
    home: &A2aHome,
    cfg: &Config,
    state: &LinkStateHandle,
    now: DateTime<Utc>,
    rng: &mut R,
) -> Result<usize, AppError> {
    let root = crate::mesh::mesh_root(home).join("outbound");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading outbound spool: {e}"),
            ))
        }
    };
    let mut sent = 0usize;
    for entry in entries {
        let entry = entry.map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading outbound spool: {e}"),
            )
        })?;
        if !entry
            .file_type()
            .map_err(|e| {
                AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("reading outbound spool: {e}"),
                )
            })?
            .is_dir()
        {
            continue;
        }
        let gateway = entry.file_name().to_string_lossy().into_owned();
        if matches!(gateway.as_str(), "sent" | "rejected" | "retired") {
            continue;
        }
        let Some(tx) = state.sender(&gateway).await else {
            continue;
        };
        let notes = std::fs::read_dir(entry.path()).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading outbound notes: {e}"),
            )
        })?;
        for note in notes {
            let note = note.map_err(|e| {
                AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("reading outbound notes: {e}"),
                )
            })?;
            if !note.file_name().to_string_lossy().ends_with(".note.json") {
                continue;
            }
            let path = note.path();
            let mut envelope = match notes_wire::read_spooled(&path) {
                Ok(envelope) => envelope,
                Err(_) if matches!(path.try_exists(), Ok(false)) => continue,
                Err(err) => return Err(err),
            };
            if envelope
                .next_attempt_after
                .map(|due| due > now)
                .unwrap_or(false)
            {
                continue;
            }
            notes_wire::schedule_retry(cfg, &path, &mut envelope, now, rng)?;
            if tx
                .send(Frame::Note {
                    note: envelope.note.clone(),
                })
                .await
                .is_ok()
            {
                sent += 1;
            }
        }
    }
    Ok(sent)
}

pub async fn push_anti_entropy_digest(
    home: &A2aHome,
    state: &LinkStateHandle,
) -> Result<(), AppError> {
    let local_gateway_id = crate::mesh::feed::existing_gateway_id(home);
    // Spec §5 deliberately forfeits transitive relay in v1; §6's three-member proof shifts to pool claims.
    let origins = replicate::digest_for_home(home)?
        .into_iter()
        .filter(|origin| local_gateway_id.as_deref() == Some(origin.origin.as_str()))
        .collect::<Vec<_>>();
    state
        .broadcast(Frame::Digest {
            origins: origins.clone(),
        })
        .await;
    let mut health = state.inner.health.lock().await;
    health.last_anti_entropy = Some(Utc::now());
    health.per_origin_lag = origins
        .into_iter()
        .map(|origin| (origin.origin, 0))
        .collect();
    Ok(())
}

fn group_entries(
    entries: Vec<crate::mesh::FeedEntry>,
) -> BTreeMap<String, Vec<crate::mesh::FeedEntry>> {
    let mut grouped = BTreeMap::<String, Vec<crate::mesh::FeedEntry>>::new();
    for entry in entries {
        if let Some(origin) = entry.origin.clone() {
            grouped.entry(origin).or_default().push(entry);
        }
    }
    grouped
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
