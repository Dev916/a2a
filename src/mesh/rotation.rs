use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::enroll_ledger::EnrollLedger;
use crate::mesh::keys::{self, RootRole, RootSet};
use crate::mesh::registry::GatewayRow;
use crate::mesh::row_sig::{parse_row_msg_strict, RowVerdict};
use crate::output::{AppError, ExitCode};
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
    _home: &A2aHome,
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
            verified_issuer_fpr,
            roots_held,
            last_handshake: state.handshakes.get(*gateway_id).cloned(),
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
