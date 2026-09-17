use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::enroll_ledger::EnrollLedger;
use crate::mesh::gateway::GatewayHealth;
use crate::mesh::keys::{self, RootRole, RootSet};
use crate::mesh::registry::GatewayRow;
use crate::mesh::row_sig::{parse_row_msg_strict, RowVerdict};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

const ROTATION_STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotationState {
    #[serde(default)]
    pub staged_at: Option<i64>,
    #[serde(default)]
    pub promoted_at: Option<i64>,
    #[serde(default)]
    pub promoted_from_fpr: Option<String>,
    #[serde(default)]
    pub promoted_to_fpr: Option<String>,
    #[serde(default)]
    pub handshakes: BTreeMap<String, HandshakeMark>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakeMark {
    pub issuer_fpr: String,
    pub at: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RotationStateFile {
    v: u8,
    #[serde(flatten)]
    state: RotationState,
}

impl RotationState {
    pub fn load(home: &A2aHome) -> Result<Self, AppError> {
        let path = rotation_state_path(home);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(io_error("reading root rotation state", error)),
        };
        let file: RotationStateFile = serde_json::from_slice(&bytes).map_err(|error| {
            AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("root rotation state is invalid: {error}"),
            )
            .remedy("restore mesh/fleet/rotation.json from trusted owner-side state")
        })?;
        if file.v != ROTATION_STATE_VERSION {
            return Err(AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("root rotation state version {} is unsupported", file.v),
            )
            .remedy("upgrade a2a before changing fleet root rotation state"));
        }
        Ok(file.state)
    }

    pub fn save(&self, home: &A2aHome) -> Result<(), AppError> {
        let path = rotation_state_path(home);
        let parent = path.parent().expect("rotation state path has a parent");
        create_dir_0700(parent).map_err(|error| {
            io_error("creating fleet root rotation state directory", error)
        })?;
        let mut bytes = serde_json::to_vec_pretty(&RotationStateFile {
            v: ROTATION_STATE_VERSION,
            state: self.clone(),
        })
        .map_err(|error| {
            AppError::new("SERIALIZE", ExitCode::Internal, error.to_string())
        })?;
        bytes.push(b'\n');
        crate::store::write_atomic(&path, &bytes)
            .map_err(|error| io_error("writing root rotation state", error))
    }

    pub fn record_handshake(
        home: &A2aHome,
        gateway_id: &str,
        mark: HandshakeMark,
    ) -> Result<(), AppError> {
        let mut state = Self::load(home)?;
        let should_write = state.handshakes.get(gateway_id).is_none_or(|stored| {
            stored.issuer_fpr != mark.issuer_fpr || mark.at.saturating_sub(stored.at) > 60
        });
        if !should_write {
            return Ok(());
        }
        state.handshakes.insert(gateway_id.to_string(), mark);
        state.save(home)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotationStatus {
    pub fleet_id: String,
    pub current_fpr: String,
    pub staged_fpr: Option<String>,
    pub prev_fpr: Option<String>,
    pub phase: RotationPhase,
    pub soak: SoakState,
    pub members: Vec<RotationMemberStatus>,
    pub unaccounted: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotationMemberStatus {
    pub gateway_id: String,
    pub verified_issuer_fpr: Option<String>,
    pub issuer_role: Option<RootRole>,
    /// A valid row signature authenticates this self-report, but possession is
    /// not independently provable; this field never gates a rotation phase.
    pub roots_held_advisory: Vec<String>,
    pub verdict: String,
    pub soak: MemberSoak,
    pub token_expires_at: Option<DateTime<Utc>>,
    pub days_remaining: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MemberSoak {
    NotPromoted,
    Pending,
    Satisfied { observed_at: i64 },
}

impl RotationStatus {
    pub fn from_evidence(
        fleet_id: String,
        roots: &RootSet,
        rows: &[(GatewayRow, RowVerdict)],
        evidence: RotationEvidence,
        now: i64,
        soak_secs: u64,
    ) -> Self {
        let phase = phase(&evidence);
        let soak = soak_state(&evidence, now, soak_secs);
        let rows_by_gateway = rows
            .iter()
            .map(|pair| (pair.0.gateway_id.as_str(), pair))
            .collect::<BTreeMap<_, _>>();
        let members = evidence
            .members
            .iter()
            .map(|member| {
                let row_pair = rows_by_gateway.get(member.gateway_id.as_str()).copied();
                let verdict = row_pair
                    .map(|(_row, verdict)| verdict.label().to_string())
                    .unwrap_or_else(|| "MISSING".to_string());
                let token_expires_at = row_pair
                    .filter(|(_row, verdict)| *verdict == RowVerdict::ValidSigned)
                    .and_then(|(row, _verdict)| verified_row_token_claims(row, roots))
                    .and_then(|claims| DateTime::from_timestamp(claims.exp, 0));
                let days_remaining = token_expires_at
                    .map(|expires_at| token_days_remaining(expires_at.timestamp(), now));
                let issuer_role = member
                    .verified_issuer_fpr
                    .as_deref()
                    .and_then(|fingerprint| roots.role_of(fingerprint));
                RotationMemberStatus {
                    gateway_id: member.gateway_id.clone(),
                    verified_issuer_fpr: member.verified_issuer_fpr.clone(),
                    issuer_role,
                    roots_held_advisory: member.roots_held.clone(),
                    verdict,
                    soak: member_soak(member, evidence.promoted_at, &evidence.current_fpr),
                    token_expires_at,
                    days_remaining,
                }
            })
            .collect();
        Self {
            fleet_id,
            current_fpr: evidence.current_fpr,
            staged_fpr: evidence.next_fpr,
            prev_fpr: evidence.prev_fpr,
            phase,
            soak,
            members,
            unaccounted: evidence.unaccounted,
        }
    }

    pub fn render_human(&self) -> String {
        let mut lines = self
            .members
            .iter()
            .map(|member| {
                let gateway_id =
                    crate::mesh::render::escape_untrusted(&member.gateway_id, 128).text;
                let issuer = member
                    .verified_issuer_fpr
                    .as_deref()
                    .map(short_fingerprint)
                    .unwrap_or("unknown");
                let role = member
                    .issuer_role
                    .map(root_role_label)
                    .unwrap_or("unknown");
                let token_expires_at = member
                    .token_expires_at
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "unknown".to_string());
                let days_remaining = member
                    .days_remaining
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                format!(
                    "{} issuer={} role={} soak={} token_expires_at={} days_remaining={}",
                    gateway_id,
                    issuer,
                    role,
                    member.soak.label(),
                    token_expires_at,
                    days_remaining,
                )
            })
            .collect::<Vec<_>>();
        lines.push(format!(
            "phase={} soak={}",
            phase_label(self.phase),
            self.soak.label()
        ));
        lines.join("\n")
    }
}

impl MemberSoak {
    fn label(&self) -> &'static str {
        match self {
            Self::NotPromoted => "not_promoted",
            Self::Pending => "pending",
            Self::Satisfied { .. } => "satisfied",
        }
    }
}

impl SoakState {
    fn label(&self) -> &'static str {
        match self {
            Self::NotPromoted => "not_promoted",
            Self::Pending { .. } => "pending",
            Self::Satisfied => "satisfied",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberEvidence {
    pub gateway_id: String,
    pub verified_issuer_fpr: Option<String>,
    pub roots_held: Vec<String>,
    pub last_handshake: Option<HandshakeMark>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotationEvidence {
    pub current_fpr: String,
    pub next_fpr: Option<String>,
    pub prev_fpr: Option<String>,
    pub promoted_at: Option<i64>,
    pub members: Vec<MemberEvidence>,
    pub unaccounted: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RotationPhase {
    NotStaged,
    Phase1Incomplete,
    Phase1,
    Phase2,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SoakState {
    NotPromoted,
    Pending {
        missing: Vec<String>,
        seconds_remaining: i64,
    },
    Satisfied,
}

pub fn phase(evidence: &RotationEvidence) -> RotationPhase {
    if evidence.next_fpr.is_none() && evidence.prev_fpr.is_none() {
        return RotationPhase::NotStaged;
    }
    if !evidence.unaccounted.is_empty() {
        return RotationPhase::Unknown;
    }
    let (old_fpr, target_fpr) = match (
        evidence.next_fpr.as_deref(),
        evidence.prev_fpr.as_deref(),
        evidence.promoted_at,
    ) {
        (Some(next), None, None) => (evidence.current_fpr.as_str(), next),
        (None, Some(prev), Some(_)) => (prev, evidence.current_fpr.as_str()),
        _ => return RotationPhase::Unknown,
    };
    if evidence.members.is_empty() {
        return RotationPhase::Phase1Incomplete;
    }
    let mut old_seen = false;
    for member in &evidence.members {
        let Some(issuer) = member.verified_issuer_fpr.as_deref() else {
            return RotationPhase::Phase1Incomplete;
        };
        if issuer == old_fpr {
            old_seen = true;
        } else if issuer != target_fpr {
            return RotationPhase::Unknown;
        }
    }
    if old_seen {
        RotationPhase::Phase1
    } else {
        RotationPhase::Phase2
    }
}

pub fn soak_state(evidence: &RotationEvidence, now: i64, soak_secs: u64) -> SoakState {
    // Soak measures how long the previous root has been held after promotion,
    // so it applies only while that old root is still present (post-promote,
    // pre-retire). Once retire drops prev_fpr there is nothing left soaking,
    // even if promoted_at lingers in the rotation state (a2a-7ku).
    if evidence.prev_fpr.is_none() {
        return SoakState::NotPromoted;
    }
    let Some(promoted_at) = evidence.promoted_at else {
        return SoakState::NotPromoted;
    };
    let mut missing = evidence.unaccounted.clone();
    for member in &evidence.members {
        let satisfied = member.last_handshake.as_ref().is_some_and(|mark| {
            mark.issuer_fpr == evidence.current_fpr && mark.at >= promoted_at
        });
        if !satisfied {
            missing.push(member.gateway_id.clone());
        }
    }
    missing.sort();
    missing.dedup();
    let soak_secs = i64::try_from(soak_secs).unwrap_or(i64::MAX);
    let seconds_remaining = promoted_at
        .saturating_add(soak_secs)
        .saturating_sub(now)
        .max(0);
    if missing.is_empty() && seconds_remaining == 0 {
        SoakState::Satisfied
    } else {
        SoakState::Pending {
            missing,
            seconds_remaining,
        }
    }
}

pub fn gather_evidence(
    home: &A2aHome,
    roots: &RootSet,
    rows: &[(GatewayRow, RowVerdict)],
    ledger: &EnrollLedger,
    state: &RotationState,
    _now: i64,
) -> Result<RotationEvidence, AppError> {
    let current_fpr = root_fpr(roots, RootRole::Current).ok_or_else(|| {
        AppError::new(
            "ROOTS_UNREADABLE",
            ExitCode::Precondition,
            "the current fleet root is missing from the local root set",
        )
        .remedy("restore mesh/fleet/root.pub before inspecting root rotation state")
    })?;
    let next_fpr = root_fpr(roots, RootRole::Next);
    let prev_fpr = root_fpr(roots, RootRole::Prev);
    let rows_by_gateway = rows
        .iter()
        .map(|pair| (pair.0.gateway_id.as_str(), pair))
        .collect::<BTreeMap<_, _>>();
    let mut latest = BTreeMap::new();
    for entry in ledger.entries() {
        latest.insert(entry.gateway_id.as_str(), entry);
    }

    let mut members = Vec::with_capacity(latest.len());
    let mut unaccounted = Vec::new();
    for gateway_id in latest.keys() {
        let row_pair = rows_by_gateway.get(gateway_id).copied();
        let (verified_issuer_fpr, roots_held) = row_pair
            .filter(|(_row, verdict)| *verdict == RowVerdict::ValidSigned)
            .map(|(row, _verdict)| verified_row_evidence(row, roots))
            .unwrap_or_default();
        if verified_issuer_fpr.is_none() {
            unaccounted.push((*gateway_id).to_string());
        }
        members.push(MemberEvidence {
            gateway_id: (*gateway_id).to_string(),
            last_handshake: member_soak_evidence(
                home,
                state,
                gateway_id,
                verified_issuer_fpr.as_deref(),
                &current_fpr,
            ),
            verified_issuer_fpr,
            roots_held,
        });
    }
    Ok(RotationEvidence {
        current_fpr,
        next_fpr,
        prev_fpr,
        promoted_at: state.promoted_at,
        members,
        unaccounted,
    })
}

/// V1's replaceable soak-evidence predicate. Until the wire reports the
/// verified issuer, a link counts only when the member's current signed row is
/// independently attested by the promoted root and the link was established
/// after promotion. L2 can replace this body with persisted handshake marks
/// without changing evidence-gathering callers.
pub fn member_soak_evidence(
    home: &A2aHome,
    state: &RotationState,
    gateway_id: &str,
    verified_issuer_fpr: Option<&str>,
    current_fpr: &str,
) -> Option<HandshakeMark> {
    let promoted_at = state.promoted_at?;
    if state.promoted_to_fpr.as_deref() != Some(current_fpr)
        || verified_issuer_fpr != Some(current_fpr)
    {
        return None;
    }
    let established_at = GatewayHealth::read(home)?
        .links
        .into_iter()
        .filter(|link| link.peer == gateway_id && link.established_at.timestamp() > promoted_at)
        .map(|link| link.established_at.timestamp())
        .max()?;
    Some(HandshakeMark {
        issuer_fpr: current_fpr.to_string(),
        at: established_at,
    })
}

fn verified_row_evidence(row: &GatewayRow, roots: &RootSet) -> (Option<String>, Vec<String>) {
    let issuer = row.row_token.as_deref().and_then(|token| {
        let (claims, issuer) = keys::verify_token_no_clock(token, roots).ok()?;
        (claims.fleet_id == row.fleet_id && claims.gateway_id == row.gateway_id)
            .then_some(issuer)
    });
    let mut roots_held = row
        .row_msg
        .as_deref()
        .and_then(|text| parse_row_msg_strict(text).ok())
        .and_then(|message| serde_json::from_str::<Vec<String>>(&message.roots_held).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|fingerprint| is_fingerprint(fingerprint))
        .collect::<Vec<_>>();
    roots_held.sort();
    roots_held.dedup();
    (issuer, roots_held)
}

fn verified_row_token_claims(
    row: &GatewayRow,
    roots: &RootSet,
) -> Option<keys::TokenClaims> {
    let (claims, _issuer) = keys::verify_token_no_clock(row.row_token.as_deref()?, roots).ok()?;
    (claims.fleet_id == row.fleet_id && claims.gateway_id == row.gateway_id).then_some(claims)
}

fn member_soak(
    member: &MemberEvidence,
    promoted_at: Option<i64>,
    current_fpr: &str,
) -> MemberSoak {
    let Some(promoted_at) = promoted_at else {
        return MemberSoak::NotPromoted;
    };
    match member.last_handshake.as_ref() {
        Some(mark) if mark.issuer_fpr == current_fpr && mark.at >= promoted_at => {
            MemberSoak::Satisfied {
                observed_at: mark.at,
            }
        }
        _ => MemberSoak::Pending,
    }
}

fn token_days_remaining(expires_at: i64, now: i64) -> i64 {
    let seconds = expires_at.saturating_sub(now);
    if seconds >= 0 {
        seconds.saturating_add(86_399) / 86_400
    } else {
        seconds.div_euclid(86_400)
    }
}

fn short_fingerprint(fingerprint: &str) -> &str {
    fingerprint.get(..12).unwrap_or(fingerprint)
}

fn root_role_label(role: RootRole) -> &'static str {
    match role {
        RootRole::Current => "current",
        RootRole::Next => "next",
        RootRole::Prev => "prev",
    }
}

fn phase_label(phase: RotationPhase) -> &'static str {
    match phase {
        RotationPhase::NotStaged => "not_staged",
        RotationPhase::Phase1Incomplete => "phase1_incomplete",
        RotationPhase::Phase1 => "phase1",
        RotationPhase::Phase2 => "phase2",
        RotationPhase::Unknown => "unknown",
    }
}

fn root_fpr(roots: &RootSet, role: RootRole) -> Option<String> {
    roots
        .entries()
        .iter()
        .find(|entry| entry.role == role)
        .map(|entry| entry.fpr.clone())
}

fn is_fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn rotation_state_path(home: &A2aHome) -> PathBuf {
    home.root()
        .join("mesh")
        .join("fleet")
        .join("rotation.json")
}

fn io_error(context: &str, error: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {error}"))
}
