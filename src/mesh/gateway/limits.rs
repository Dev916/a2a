use crate::config::MeshLimitsConfig;
use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::wire::Frame;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::net::IpAddr;

pub const MIN_DEFERRED_PEER_VERSION: &str = "0.4.0";
pub const MAX_ORIGINS_PER_REQUEST_FRAME: usize = 64;
pub const FEED_ORIGIN_UNBOUND_REMEDY: &str = "the peer sent entries for another member's origin; v1 pins feed origin to the authenticated link peer — relay is deferred until entries are signed end-to-end";

pub fn peer_supports_deferred(peer_version: Option<&str>) -> bool {
    peer_version.is_some_and(|version| {
        crate::commands::work_cmd::version_at_least(version, MIN_DEFERRED_PEER_VERSION)
    })
}

pub fn deferred_frame(code: &str, retry_after_secs: Option<u64>) -> Frame {
    Frame::Deferred {
        code: code.to_string(),
        retry_after_secs,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BucketKind {
    Notes,
    FeedEntries,
    FeedBytes,
    WorkOffers,
    Wants,
    Digests,
}

impl BucketKind {
    pub const ALL: [Self; 6] = [
        Self::Notes,
        Self::FeedEntries,
        Self::FeedBytes,
        Self::WorkOffers,
        Self::Wants,
        Self::Digests,
    ];

    pub fn per_hour(self) -> bool {
        self == Self::WorkOffers
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Notes => "notes",
            Self::FeedEntries => "feed_entries",
            Self::FeedBytes => "feed_bytes",
            Self::WorkOffers => "work_offers",
            Self::Wants => "wants",
            Self::Digests => "digests",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitOutcome {
    Off,
    Allowed,
    Deferred {
        transitioned: bool,
        retry_after_secs: u64,
    },
}

#[derive(Clone, Debug)]
pub struct Bucket {
    pub capacity: u64,
    pub tokens: f64,
    pub refill_per_sec: f64,
    pub over: bool,
    last_refill: Option<DateTime<Utc>>,
}

impl Bucket {
    pub fn new(rate: u64, kind: BucketKind) -> Self {
        let denominator = if kind.per_hour() { 3_600.0 } else { 60.0 };
        Self {
            capacity: rate,
            tokens: rate as f64,
            refill_per_sec: rate as f64 / denominator,
            over: false,
            last_refill: None,
        }
    }

    pub fn take(&mut self, amount: u64, now: DateTime<Utc>) -> LimitOutcome {
        if self.capacity == 0 {
            return LimitOutcome::Off;
        }
        if let Some(last) = self.last_refill {
            let elapsed = now
                .signed_duration_since(last)
                .to_std()
                .map(|duration| duration.as_secs_f64())
                .unwrap_or(0.0);
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity as f64);
        }
        self.last_refill = Some(now);
        if amount == 0 || self.tokens >= amount as f64 {
            self.tokens -= amount as f64;
            self.over = false;
            return LimitOutcome::Allowed;
        }

        let transitioned = !self.over;
        self.over = true;
        let until_one = ((1.0 - self.tokens).max(0.0) / self.refill_per_sec).ceil();
        let retry_after_secs = if until_one.is_finite() {
            until_one.max(1.0).min(u64::MAX as f64) as u64
        } else {
            u64::MAX
        };
        LimitOutcome::Deferred {
            transitioned,
            retry_after_secs,
        }
    }

    fn refund(&mut self, amount: u64) {
        self.tokens = (self.tokens + amount as f64).min(self.capacity as f64);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerLimitHealth {
    #[serde(default)]
    pub defers: BTreeMap<String, u64>,
    #[serde(default)]
    pub drops: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub struct PeerBuckets {
    buckets: BTreeMap<BucketKind, Bucket>,
    defers: BTreeMap<String, u64>,
    drops: BTreeMap<String, u64>,
}

impl PeerBuckets {
    pub fn new(cfg: &MeshLimitsConfig) -> Self {
        let rates = [
            (BucketKind::Notes, cfg.notes_per_min),
            (BucketKind::FeedEntries, cfg.feed_entries_per_min),
            (BucketKind::FeedBytes, cfg.feed_bytes_per_min),
            (BucketKind::WorkOffers, cfg.work_offers_per_hour),
            (BucketKind::Wants, cfg.wants_per_min),
            (BucketKind::Digests, cfg.digests_per_min),
        ];
        Self {
            buckets: rates
                .into_iter()
                .map(|(kind, rate)| (kind, Bucket::new(rate, kind)))
                .collect(),
            defers: BTreeMap::new(),
            drops: BTreeMap::new(),
        }
    }

    pub fn charge(
        &mut self,
        kind: BucketKind,
        amount: u64,
        now: DateTime<Utc>,
    ) -> LimitOutcome {
        let outcome = self
            .buckets
            .get_mut(&kind)
            .expect("all peer buckets are seeded")
            .take(amount, now);
        if matches!(outcome, LimitOutcome::Deferred { .. }) {
            let count = self.defers.entry(kind.as_str().to_string()).or_default();
            *count = count.saturating_add(1);
        }
        outcome
    }

    pub fn allow(&mut self, kind: BucketKind, now: DateTime<Utc>) -> LimitOutcome {
        self.charge(kind, 1, now)
    }

    pub(crate) fn record_drop(&mut self, kind: BucketKind) {
        let count = self.drops.entry(kind.as_str().to_string()).or_default();
        *count = count.saturating_add(1);
    }

    pub(crate) fn refund(&mut self, kind: BucketKind, amount: u64) {
        self.buckets
            .get_mut(&kind)
            .expect("all peer buckets are seeded")
            .refund(amount);
    }

    pub fn health(&self) -> PeerLimitHealth {
        PeerLimitHealth {
            defers: self.defers.clone(),
            drops: self.drops.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreAuthAdmission {
    Admitted,
    TooManyUnauthenticated,
    SourceRateLimited,
}

#[derive(Clone, Debug)]
pub struct PreAuthGate {
    max_unauthenticated_sessions: u64,
    connects_per_min_per_source: u64,
    live_unauthenticated: u64,
    live_by_source: BTreeMap<IpAddr, u64>,
    connects: BTreeMap<IpAddr, VecDeque<DateTime<Utc>>>,
    too_many_unauthenticated: u64,
    source_rate_limited: u64,
}

impl PreAuthGate {
    pub fn new(cfg: &MeshLimitsConfig) -> Self {
        Self {
            max_unauthenticated_sessions: cfg.max_unauthenticated_sessions,
            connects_per_min_per_source: cfg.connects_per_min_per_source,
            live_unauthenticated: 0,
            live_by_source: BTreeMap::new(),
            connects: BTreeMap::new(),
            too_many_unauthenticated: 0,
            source_rate_limited: 0,
        }
    }

    pub fn admit(&mut self, source: IpAddr, now: DateTime<Utc>) -> PreAuthAdmission {
        let recent = self.connects.entry(source).or_default();
        while recent
            .front()
            .is_some_and(|at| now.signed_duration_since(*at) >= chrono::Duration::minutes(1))
        {
            recent.pop_front();
        }
        if recent.len() as u64 >= self.connects_per_min_per_source {
            self.source_rate_limited = self.source_rate_limited.saturating_add(1);
            return PreAuthAdmission::SourceRateLimited;
        }
        recent.push_back(now);
        if self.live_unauthenticated >= self.max_unauthenticated_sessions {
            self.too_many_unauthenticated = self.too_many_unauthenticated.saturating_add(1);
            return PreAuthAdmission::TooManyUnauthenticated;
        }
        self.live_unauthenticated = self.live_unauthenticated.saturating_add(1);
        let live = self.live_by_source.entry(source).or_default();
        *live = live.saturating_add(1);
        PreAuthAdmission::Admitted
    }

    pub fn release(&mut self, source: IpAddr) {
        let mut remove = false;
        if let Some(live) = self.live_by_source.get_mut(&source) {
            if *live > 0 {
                *live -= 1;
                self.live_unauthenticated = self.live_unauthenticated.saturating_sub(1);
            }
            remove = *live == 0;
        }
        if remove {
            self.live_by_source.remove(&source);
        }
    }

    pub fn health(&self) -> Value {
        let sources = self
            .connects
            .iter()
            .map(|(source, recent)| {
                (
                    source.to_string(),
                    json!({
                        "connects_in_window": recent.len(),
                        "live_unauthenticated": self.live_by_source.get(source).copied().unwrap_or(0),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        json!({
            "live_unauthenticated": self.live_unauthenticated,
            "max_unauthenticated_sessions": self.max_unauthenticated_sessions,
            "connects_per_min_per_source": self.connects_per_min_per_source,
            "refusals": {
                "too_many_unauthenticated": self.too_many_unauthenticated,
                "source_rate_limited": self.source_rate_limited,
            },
            "sources": sources,
        })
    }
}

pub fn log_bucket_transition(
    home: &A2aHome,
    peer_gateway_id: &str,
    kind: BucketKind,
    state: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut file) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "limit_state_change",
            "peer_gateway_id": peer_gateway_id,
            "bucket": kind.as_str(),
            "state": state,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut file, &bytes);
        }
    }
}

pub(crate) fn log_limit_refusal(
    home: &A2aHome,
    peer_or_source: &str,
    code: &str,
    reason: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut file) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "link_limit",
            "peer_or_source": peer_or_source,
            "action": "closed",
            "code": code,
            "reason": reason,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut file, &bytes);
        }
    }
}

pub(crate) fn log_deferral_received(
    home: &A2aHome,
    peer_gateway_id: &str,
    code: &str,
    retry_after_secs: Option<u64>,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut file) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "link",
            "peer_gateway_id": peer_gateway_id,
            "action": "deferral_received",
            "code": code,
            "retry_after_secs": retry_after_secs,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut file, &bytes);
        }
    }
}

pub(crate) fn log_origin_unbound(
    home: &A2aHome,
    peer_gateway_id: &str,
    frame_origin: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut file) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "feed",
            "action": "origin_unbound_refused",
            "code": "FEED_ORIGIN_UNBOUND",
            "frame_origin": frame_origin,
            "peer_gateway_id": peer_gateway_id,
            "remedy": FEED_ORIGIN_UNBOUND_REMEDY,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut file, &bytes);
        }
    }
}
