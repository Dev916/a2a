use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::enroll_ledger::EnrollLedger;
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration as StdDuration, SystemTime};
use ulid::Ulid;

pub const SKEW_TOLERANCE_SECS: i64 = 300;

pub const ROOT_KEY: &str = "root.key";
const ROOT_KEY_NEXT: &str = "root.key.next";
const ROOT_KEY_PREV: &str = "root.key.prev";
const ROOT_PUB: &str = "root.pub";
const ROOT_PUB_NEXT: &str = "root.pub.next";
const ROOT_PUB_PREV: &str = "root.pub.prev";
const GW_KEY: &str = "gw.key";
pub const GW_TOKEN: &str = "gw.token";
const TLS_CERT: &str = "gw.crt";
const TLS_KEY: &str = "gw.tls.key";
const TLS_FPR: &str = "gw.cert.fpr";
const TOKEN_VERSION: u8 = 1;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug)]
pub struct TestClock {
    now: Mutex<DateTime<Utc>>,
}

impl TestClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    pub fn advance(&self, delta: chrono::Duration) {
        let mut now = self.now.lock().expect("test clock mutex poisoned");
        *now += delta;
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().expect("test clock mutex poisoned")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetRoot {
    pub fleet_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayIdentity {
    pub gateway_id: String,
    pub cert_fpr: String,
}

impl GatewayIdentity {
    pub fn cert_fpr(&self) -> &str {
        &self.cert_fpr
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenClaims {
    pub fleet_id: String,
    pub gateway_id: String,
    pub pubkey_b64: String,
    #[serde(default)]
    pub key_epoch: u64,
    pub issued_at: i64,
    pub exp: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevocationClaims {
    pub fleet_id: String,
    pub gateway_id: String,
    pub key_epoch: u64,
    pub revoked_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootRole {
    Current,
    Next,
    Prev,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootEntry {
    pub role: RootRole,
    pub fleet_id: String,
    pub pubkey_b64: String,
    pub fpr: String,
}

#[derive(Clone, Debug)]
pub struct RootSet(Vec<RootEntry>);

impl RootSet {
    pub fn load(home: &A2aHome) -> Result<Self, AppError> {
        let dir = fleet_dir(home)?;
        let current = read_root_pub(&dir.join(ROOT_PUB))?;
        let fleet_id = current.fleet_id.clone();
        let mut roots = vec![root_entry(current, RootRole::Current)?];
        for (name, role) in [
            (ROOT_PUB_NEXT, RootRole::Next),
            (ROOT_PUB_PREV, RootRole::Prev),
        ] {
            let path = dir.join(name);
            if !path.exists() {
                continue;
            }
            let root = read_root_pub(&path)?;
            if root.fleet_id != fleet_id {
                return Err(root_next_fleet_mismatch(
                    role,
                    &fleet_id,
                    &root.fleet_id,
                ));
            }
            roots.push(root_entry(root, role)?);
        }
        Ok(Self(roots))
    }

    pub fn entries(&self) -> &[RootEntry] {
        &self.0
    }

    pub fn fingerprints(&self) -> Vec<String> {
        self.0.iter().map(|root| root.fpr.clone()).collect()
    }

    pub fn role_of(&self, fpr: &str) -> Option<RootRole> {
        self.0
            .iter()
            .find(|entry| entry.fpr == fpr)
            .map(|entry| entry.role)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootStage {
    pub fleet_id: String,
    pub current_fingerprint: String,
    pub next_fingerprint: String,
    pub announcement: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotationAnnouncement {
    pub fleet_id: String,
    pub next_pubkey_b64: String,
    pub next_fpr: String,
    pub issued_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledRootNext {
    pub fleet_id: String,
    pub next_fpr: String,
    pub token_reverified: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RootSigner {
    Current,
    Next,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootKeyFile {
    fleet_id: String,
    secret_b64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootPubFile {
    fleet_id: String,
    pubkey_b64: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayKeyFile {
    gateway_id: String,
    secret_b64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenEnvelope {
    v: u8,
    claims_b64: String,
    sig_b64: String,
}

#[derive(Serialize)]
struct TokenEnvelopeOut<'a> {
    v: u8,
    claims_b64: &'a str,
    sig_b64: &'a str,
}

pub fn fleet_init(home: &A2aHome) -> Result<FleetRoot, AppError> {
    let dir = fleet_dir(home)?;
    let key_path = dir.join(ROOT_KEY);
    let pub_path = dir.join(ROOT_PUB);

    if key_path.exists() {
        let root = read_root_key(&key_path)?;
        if !pub_path.exists() {
            write_root_pub(&pub_path, &root)?;
        }
        return Ok(FleetRoot {
            fleet_id: root.fleet_id,
        });
    }

    // root.pub without root.key is the signature of a JOINER home. Minting
    // here would fork the machine onto a phantom fleet and clobber the
    // root.pub its owner installed (live incident: a joiner running the
    // read-only `fleet ls` was silently re-homed this way).
    if pub_path.exists() {
        let joined = read_root_pub(&pub_path)?;
        return Err(AppError::new(
            "FLEET_ALREADY_JOINED",
            ExitCode::Precondition,
            format!(
                "this machine is enrolled in {} as a joiner (mesh/fleet/root.pub without root.key); refusing to mint a new fleet root",
                joined.fleet_id
            ),
        )
        .remedy(
            "joiner machines never run `fleet init` — see docs/mesh-join.md; to genuinely re-init, remove mesh/fleet/root.pub first",
        ));
    }

    let signing = new_signing_key();
    let pubkey = signing.verifying_key().to_bytes();
    let fleet_id = format!("fleet-{}", &hex(&Sha256::digest(pubkey))[..8]);
    let root = RootKeyFile {
        fleet_id: fleet_id.clone(),
        secret_b64: b64_encode(&signing.to_bytes()),
    };
    write_json_0600(&key_path, &root)?;
    write_root_pub(&pub_path, &root)?;
    Ok(FleetRoot { fleet_id })
}

pub fn fleet_root_fingerprint(home: &A2aHome) -> Result<String, AppError> {
    let root = read_root_pub(&fleet_dir(home)?.join(ROOT_PUB))?;
    root_fingerprint(&root)
}

pub fn stage_root(
    home: &A2aHome,
    clock: &dyn Clock,
    force: bool,
) -> Result<RootStage, AppError> {
    let current = fleet_init(home)?;
    let dir = fleet_dir(home)?;
    let current_pub = read_root_pub(&dir.join(ROOT_PUB))?;
    let next_key_path = dir.join(ROOT_KEY_NEXT);
    let next_pub_path = dir.join(ROOT_PUB_NEXT);
    if (next_key_path.exists() || next_pub_path.exists()) && !force {
        return Err(root_rotation_staged());
    }
    if force {
        remove_file_if_present(&next_key_path, "removing staged fleet root key")?;
        remove_file_if_present(&next_pub_path, "removing staged fleet root public key")?;
    }
    let signing = new_signing_key();
    let next_key = RootKeyFile {
        fleet_id: current.fleet_id.clone(),
        secret_b64: b64_encode(&signing.to_bytes()),
    };
    write_json_0600(&next_key_path, &next_key)?;
    write_root_pub(&next_pub_path, &next_key)?;
    let next_pub = read_root_pub(&next_pub_path)?;
    let next_fingerprint = root_fingerprint(&next_pub)?;
    let staged_at = clock.now().timestamp();
    let ann = RotationAnnouncement {
        fleet_id: current.fleet_id.clone(),
        next_pubkey_b64: next_pub.pubkey_b64.clone(),
        next_fpr: next_fingerprint.clone(),
        issued_at: staged_at,
    };
    let announcement = sign_announcement(&ann, &dir.join(ROOT_KEY))?;
    crate::mesh::rotation::RotationState {
        staged_at: Some(staged_at),
        ..crate::mesh::rotation::RotationState::default()
    }
    .save(home)?;
    Ok(RootStage {
        fleet_id: current.fleet_id,
        current_fingerprint: root_fingerprint(&current_pub)?,
        next_fingerprint,
        announcement,
    })
}

pub fn abort_root_stage(
    home: &A2aHome,
    force: bool,
    ledger: &EnrollLedger,
) -> Result<String, AppError> {
    let dir = fleet_dir(home)?;
    let next_key_path = dir.join(ROOT_KEY_NEXT);
    let next_pub_path = dir.join(ROOT_PUB_NEXT);
    if !next_key_path.exists() || !next_pub_path.exists() {
        return Err(root_next_missing());
    }
    let staged_fpr = root_fingerprint(&read_root_pub(&next_pub_path)?)?;
    let mut enrolled = ledger
        .entries()
        .iter()
        .filter(|entry| entry.issuing_root_fpr == staged_fpr)
        .map(|entry| entry.gateway_id.clone())
        .collect::<Vec<_>>();
    enrolled.sort();
    enrolled.dedup();
    if !force && !enrolled.is_empty() {
        return Err(root_rotation_abort_refused(&staged_fpr, &enrolled));
    }
    remove_file_if_present(&next_key_path, "removing staged fleet root key")?;
    remove_file_if_present(&next_pub_path, "removing staged fleet root public key")?;
    Ok(staged_fpr)
}

pub fn promote_root(home: &A2aHome, clock: &dyn Clock) -> Result<(), AppError> {
    let dir = fleet_dir(home)?;
    let next_key_path = dir.join(ROOT_KEY_NEXT);
    let next_pub_path = dir.join(ROOT_PUB_NEXT);
    if !next_key_path.exists() || !next_pub_path.exists() {
        return Err(root_next_missing());
    }
    let roots = RootSet::load(home)?;
    let current_fpr = roots
        .entries()
        .iter()
        .find(|entry| entry.role == RootRole::Current)
        .map(|entry| entry.fpr.clone())
        .ok_or_else(root_next_missing)?;
    let next_fpr = roots
        .entries()
        .iter()
        .find(|entry| entry.role == RootRole::Next)
        .map(|entry| entry.fpr.clone())
        .ok_or_else(root_next_missing)?;
    let mut state = crate::mesh::rotation::RotationState::load(home)?;

    rename_file(
        &dir.join(ROOT_PUB),
        &dir.join(ROOT_PUB_PREV),
        "promoting fleet root public key",
    )?;
    rename_file(
        &dir.join(ROOT_KEY),
        &dir.join(ROOT_KEY_PREV),
        "promoting fleet root private key",
    )?;
    rename_file(
        &next_pub_path,
        &dir.join(ROOT_PUB),
        "installing promoted fleet root public key",
    )?;
    rename_file(
        &next_key_path,
        &dir.join(ROOT_KEY),
        "installing promoted fleet root private key",
    )?;

    state.promoted_at = Some(clock.now().timestamp());
    state.promoted_from_fpr = Some(current_fpr);
    state.promoted_to_fpr = Some(next_fpr);
    state.save(home)
}

pub fn retire_root(home: &A2aHome) -> Result<String, AppError> {
    let dir = fleet_dir(home)?;
    let prev_key_path = dir.join(ROOT_KEY_PREV);
    let prev_pub_path = dir.join(ROOT_PUB_PREV);
    if !prev_key_path.exists() || !prev_pub_path.exists() {
        return Err(root_prev_missing());
    }
    let retired_fpr = root_fingerprint(&read_root_pub(&prev_pub_path)?)?;
    remove_file_if_present(&prev_key_path, "retiring previous fleet root key")?;
    remove_file_if_present(&prev_pub_path, "retiring previous fleet root public key")?;
    Ok(retired_fpr)
}

pub fn gateway_identity(home: &A2aHome, fresh: bool) -> Result<GatewayIdentity, AppError> {
    let dir = gateway_dir(home)?;
    let key_path = dir.join(GW_KEY);

    let gateway = if key_path.exists() && !fresh {
        read_gateway_key(&key_path)?
    } else {
        let signing = new_signing_key();
        let suffix = &hex(&Sha256::digest(signing.verifying_key().to_bytes()))[..16];
        let gateway_id = format!("gw-{}-{suffix}", hostname_slug());
        let gateway = GatewayKeyFile {
            gateway_id,
            secret_b64: b64_encode(&signing.to_bytes()),
        };
        write_json_0600(&key_path, &gateway)?;
        gateway
    };

    let cert_fpr = ensure_tls_material(&dir)?;
    Ok(GatewayIdentity {
        gateway_id: gateway.gateway_id,
        cert_fpr,
    })
}

pub fn fleet_id(home: &A2aHome) -> Result<String, AppError> {
    let dir = fleet_dir(home)?;
    let key_path = dir.join(ROOT_KEY);
    if key_path.exists() {
        return Ok(read_root_key(&key_path)?.fleet_id);
    }
    read_root_pub(&dir.join(ROOT_PUB)).map(|root| root.fleet_id)
}

pub fn gateway_public_key_b64(home: &A2aHome) -> Result<String, AppError> {
    let gateway = read_gateway_key(&gateway_dir(home)?.join(GW_KEY))?;
    let signing = signing_key_from_b64(&gateway.secret_b64).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })?;
    Ok(b64_encode(&signing.verifying_key().to_bytes()))
}

pub fn gateway_id(home: &A2aHome) -> Result<String, AppError> {
    let gateway = read_gateway_key(&gateway_dir(home)?.join(GW_KEY))?;
    Ok(gateway.gateway_id)
}

pub fn gateway_public_key_fingerprint(home: &A2aHome) -> Result<String, AppError> {
    let pubkey_b64 = gateway_public_key_b64(home)?;
    let pubkey = b64_decode(&pubkey_b64).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })?;
    Ok(hex(&Sha256::digest(pubkey)))
}

pub fn fpr_of_pubkey_b64(pubkey_b64: &str) -> Result<String, AppError> {
    let key = verifying_key_from_b64(pubkey_b64).map_err(|_| {
        AppError::new(
            "INVALID_PUBKEY",
            ExitCode::Precondition,
            "gateway public key must be a base64-encoded ed25519 verifying key",
        )
    })?;
    Ok(hex(&Sha256::digest(key.to_bytes())))
}

pub fn gateway_signing_key(home: &A2aHome) -> Result<SigningKey, AppError> {
    let gateway = read_gateway_key(&gateway_dir(home)?.join(GW_KEY))?;
    signing_key_from_b64(&gateway.secret_b64).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })
}

pub fn gateway_token(home: &A2aHome) -> Result<String, AppError> {
    let path = gateway_dir(home)?.join(GW_TOKEN);
    fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .map_err(|e| io_err("reading gateway token", e))
}

pub fn ensure_and_verify_gateway_enrollment(
    home: &A2aHome,
    clock: &dyn Clock,
) -> Result<GatewayIdentity, AppError> {
    let identity = gateway_identity(home, false)?;
    ensure_gateway_token(home, clock)?;
    let pubkey_b64 = gateway_public_key_b64(home)?;
    verify_gateway_token(home, &identity.gateway_id, &pubkey_b64, clock)?;
    Ok(identity)
}

pub fn verify_gateway_token(
    home: &A2aHome,
    gateway_id: &str,
    pubkey_b64: &str,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError> {
    let token = gateway_token(home)?;
    let home_fleet_id = fleet_id(home)?;
    let roots = RootSet::load(home)?;
    let claims = match verify_token(token.trim(), &roots, clock) {
        Ok(claims) => claims,
        Err(err) if err.code == "TOKEN_EXPIRED" => return Err(gateway_token_expired(err.message)),
        Err(err) => {
            if err.code == "TOKEN_INVALID" {
                if let Ok(claims) = unverified_claims(token.trim()) {
                    if claims.fleet_id != home_fleet_id {
                        return Err(gateway_fleet_mismatch(&claims.fleet_id, &home_fleet_id));
                    }
                }
            }
            return Err(gateway_token_invalid(err.message));
        }
    };
    if claims.fleet_id != home_fleet_id {
        return Err(gateway_fleet_mismatch(&claims.fleet_id, &home_fleet_id));
    }
    if claims.gateway_id != gateway_id {
        return Err(gateway_token_mismatch(
            "gateway_id",
            gateway_id,
            &claims.gateway_id,
        ));
    }
    if claims.pubkey_b64 != pubkey_b64 {
        return Err(gateway_token_mismatch(
            "pubkey_b64",
            pubkey_b64,
            &claims.pubkey_b64,
        ));
    }
    Ok(claims)
}

pub fn ensure_gateway_token(home: &A2aHome, clock: &dyn Clock) -> Result<bool, AppError> {
    let token_path = gateway_dir(home)?.join(GW_TOKEN);
    if token_path.exists() {
        return Ok(false);
    }
    let root_key_path = fleet_dir(home)?.join(ROOT_KEY);
    if !root_key_path.exists() {
        return Err(AppError::new(
            "GATEWAY_NOT_ENROLLED",
            ExitCode::Precondition,
            "gateway enrollment token is missing and this home has no fleet root key",
        )
        .remedy("on the fleet's root machine run `a2a mesh fleet enroll` for this gateway's pubkey, then install it here with `a2a mesh gateway install-token` (docs/mesh-join.md); a brand-new fleet starts with `a2a mesh fleet init` on the machine that will hold the root"));
    }

    let identity = gateway_identity(home, false)?;
    let issued_at = clock.now().timestamp();
    let claims = TokenClaims {
        fleet_id: String::new(),
        gateway_id: identity.gateway_id,
        pubkey_b64: gateway_public_key_b64(home)?,
        key_epoch: 0,
        issued_at,
        exp: issued_at + 90 * 24 * 60 * 60,
    };
    let token = sign_token(&claims, home)?;
    write_string_0600(&token_path, &(token + "\n"))?;
    Ok(true)
}

pub fn enroll_gateway_token(
    home: &A2aHome,
    gateway_id: &str,
    pubkey_b64: &str,
    clock: &dyn Clock,
) -> Result<String, AppError> {
    enroll_gateway_token_at_epoch(home, gateway_id, pubkey_b64, 0, clock)
        .map(|(token, _claims, _issuer)| token)
}

pub fn enroll_gateway_token_with(
    home: &A2aHome,
    gateway_id: &str,
    pubkey_b64: &str,
    signer: RootSigner,
    clock: &dyn Clock,
) -> Result<(String, String), AppError> {
    enroll_gateway_token_at_epoch_with(home, gateway_id, pubkey_b64, 0, signer, clock)
        .map(|(token, _claims, issuer)| (token, issuer))
}

pub fn enroll_gateway_token_at_epoch(
    home: &A2aHome,
    gateway_id: &str,
    pubkey_b64: &str,
    key_epoch: u64,
    clock: &dyn Clock,
) -> Result<(String, TokenClaims, String), AppError> {
    enroll_gateway_token_at_epoch_with(
        home,
        gateway_id,
        pubkey_b64,
        key_epoch,
        RootSigner::Current,
        clock,
    )
}

pub fn enroll_gateway_token_at_epoch_with(
    home: &A2aHome,
    gateway_id: &str,
    pubkey_b64: &str,
    key_epoch: u64,
    signer: RootSigner,
    clock: &dyn Clock,
) -> Result<(String, TokenClaims, String), AppError> {
    validate_gateway_pubkey_b64(pubkey_b64)?;
    let dir = fleet_dir(home)?;
    let (key_name, pub_name) = match signer {
        RootSigner::Current => (ROOT_KEY, ROOT_PUB),
        RootSigner::Next => (ROOT_KEY_NEXT, ROOT_PUB_NEXT),
    };
    let root_key_path = dir.join(key_name);
    if !root_key_path.exists() {
        if signer == RootSigner::Next {
            return Err(root_next_missing());
        }
        return Err(AppError::new(
            "FLEET_ROOT_KEY_MISSING",
            ExitCode::Precondition,
            "fleet root key is required to enroll a remote gateway",
        )
        .remedy("run this command on the fleet root machine that holds mesh/fleet/root.key"));
    }
    let root_pub_path = dir.join(pub_name);
    let root = read_root_key(&root_key_path)?;
    if signer == RootSigner::Next {
        let current_fleet = fleet_id(home)?;
        if root.fleet_id != current_fleet {
            return Err(root_next_fleet_mismatch(
                RootRole::Next,
                &current_fleet,
                &root.fleet_id,
            ));
        }
    }
    if !root_pub_path.exists() {
        write_root_pub(&root_pub_path, &root)?;
    }
    let issued_at = clock.now().timestamp();
    let claims = TokenClaims {
        fleet_id: root.fleet_id,
        gateway_id: gateway_id.to_string(),
        pubkey_b64: pubkey_b64.to_string(),
        key_epoch,
        issued_at,
        exp: issued_at + 90 * 24 * 60 * 60,
    };
    let claims_bytes = canonical_claims(&claims)?;
    let token = sign_claim_bytes_with_key(&claims_bytes, &root_key_path)?;
    let issuer = root_fingerprint(&read_root_pub(&root_pub_path)?)?;
    Ok((token, claims, issuer))
}

pub fn install_gateway_token(
    home: &A2aHome,
    token: &str,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError> {
    let identity = gateway_identity(home, false)?;
    let local_pubkey_b64 = gateway_public_key_b64(home)?;
    let root_pub_path = fleet_dir(home)?.join(ROOT_PUB);
    if !root_pub_path.exists() {
        return Err(AppError::new(
            "TOKEN_UNVERIFIABLE",
            ExitCode::Precondition,
            "gateway token cannot be verified because fleet root public key is missing",
        )
        .remedy("install mesh/fleet/root.pub first"));
    }
    let claims = verify_token(token.trim(), &RootSet::load(home)?, clock)?;
    if claims.gateway_id != identity.gateway_id {
        return Err(token_mismatch(
            "gateway_id",
            &identity.gateway_id,
            &claims.gateway_id,
        ));
    }
    if claims.pubkey_b64 != local_pubkey_b64 {
        return Err(token_mismatch(
            "pubkey_b64",
            &local_pubkey_b64,
            &claims.pubkey_b64,
        ));
    }
    write_string_0600(
        &gateway_dir(home)?.join(GW_TOKEN),
        &(token.trim().to_string() + "\n"),
    )?;
    Ok(claims)
}

pub fn install_root_next(
    home: &A2aHome,
    announcement_text: &str,
    clock: &dyn Clock,
) -> Result<InstalledRootNext, AppError> {
    let roots = RootSet::load(home)?;
    let (announcement, issuer_fpr) = verify_announcement(announcement_text.trim(), &roots)?;
    if roots.role_of(&issuer_fpr) != Some(RootRole::Current) {
        return Err(root_announcement_invalid(
            "rotation announcement was not signed by the current root",
        ));
    }
    let local_fleet_id = fleet_id(home)?;
    if announcement.fleet_id != local_fleet_id {
        return Err(root_announcement_fleet_mismatch(
            &local_fleet_id,
            &announcement.fleet_id,
        ));
    }
    let next_key = verifying_key_from_b64(&announcement.next_pubkey_b64)
        .map_err(|_| root_announcement_invalid("announced next root public key is invalid"))?;
    let computed_fpr = hex(&Sha256::digest(next_key.to_bytes()));
    if computed_fpr != announcement.next_fpr {
        return Err(root_announcement_invalid(
            "announced next root fingerprint does not match its public key",
        ));
    }
    let next_pub = RootPubFile {
        fleet_id: announcement.fleet_id.clone(),
        pubkey_b64: announcement.next_pubkey_b64,
    };
    write_root_pub_file(&fleet_dir(home)?.join(ROOT_PUB_NEXT), &next_pub)?;

    let token_path = gateway_dir(home)?.join(GW_TOKEN);
    let token_reverified = if token_path.exists() {
        let gateway_id = gateway_id(home)?;
        let pubkey_b64 = gateway_public_key_b64(home)?;
        verify_gateway_token(home, &gateway_id, &pubkey_b64, clock)?;
        true
    } else {
        false
    };
    Ok(InstalledRootNext {
        fleet_id: announcement.fleet_id,
        next_fpr: computed_fpr,
        token_reverified,
    })
}

pub fn sign_token(claims: &TokenClaims, home: &A2aHome) -> Result<String, AppError> {
    let root = fleet_init(home)?;
    let mut claims = claims.clone();
    claims.fleet_id = root.fleet_id;
    let claims_bytes = canonical_claims(&claims)?;
    sign_claim_bytes(&claims_bytes, home)
}

pub fn sign_announcement(
    announcement: &RotationAnnouncement,
    key_path: &Path,
) -> Result<String, AppError> {
    sign_claim_bytes_with_key(&canonical_announcement(announcement)?, key_path)
}

pub fn verify_announcement(
    text: &str,
    roots: &RootSet,
) -> Result<(RotationAnnouncement, String), AppError> {
    let encoded = b64_decode(text)
        .map_err(|_| root_announcement_invalid("rotation announcement is not base64"))?;
    let envelope: TokenEnvelope = serde_json::from_slice(&encoded)
        .map_err(|_| root_announcement_invalid("rotation announcement envelope is invalid"))?;
    if envelope.v != TOKEN_VERSION {
        return Err(root_announcement_invalid(
            "rotation announcement version is unsupported",
        ));
    }
    let claims_bytes = b64_decode(&envelope.claims_b64)
        .map_err(|_| root_announcement_invalid("announcement claims are not base64"))?;
    let sig_bytes = b64_decode(&envelope.sig_b64)
        .map_err(|_| root_announcement_invalid("announcement signature is not base64"))?;
    let sig: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        root_announcement_invalid("announcement signature has invalid length")
    })?;
    let signature = Signature::from_bytes(&sig);
    let announcement: RotationAnnouncement = serde_json::from_slice(&claims_bytes).map_err(|_| {
        root_announcement_invalid("announcement claims are invalid or contain unknown fields")
    })?;
    let canonical = canonical_announcement(&announcement)?;
    if canonical != claims_bytes {
        return Err(root_announcement_invalid(
            "rotation announcement claims are not canonical",
        ));
    }
    for root in roots.entries() {
        let key = verifying_key_from_b64(&root.pubkey_b64).map_err(|_| {
            root_announcement_invalid("trusted root public key is invalid")
        })?;
        if key.verify(&claims_bytes, &signature).is_ok() {
            return Ok((announcement, root.fpr.clone()));
        }
    }
    Err(root_announcement_invalid(
        "announcement signature does not verify against trusted roots",
    ))
}

pub fn verify_token(
    token_b64: &str,
    roots: &RootSet,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError> {
    verify_token_with_issuer(token_b64, roots, clock).map(|(claims, _issuer)| claims)
}

pub fn verify_token_with_issuer(
    token_b64: &str,
    roots: &RootSet,
    clock: &dyn Clock,
) -> Result<(TokenClaims, String), AppError> {
    let (claims, issuer) = verify_token_no_clock(token_b64, roots)?;
    check_claim_clock(&claims, clock.now().timestamp())?;
    Ok((claims, issuer))
}

pub fn verify_token_no_clock(
    token_b64: &str,
    roots: &RootSet,
) -> Result<(TokenClaims, String), AppError> {
    let token = b64_decode(token_b64).map_err(|_| token_invalid("token is not base64"))?;
    let envelope: TokenEnvelope =
        serde_json::from_slice(&token).map_err(|_| token_invalid("token envelope is invalid"))?;
    if envelope.v != TOKEN_VERSION {
        return Err(token_invalid("token version is unsupported"));
    }
    let claims_bytes =
        b64_decode(&envelope.claims_b64).map_err(|_| token_invalid("claims are not base64"))?;
    let sig_bytes =
        b64_decode(&envelope.sig_b64).map_err(|_| token_invalid("signature is not base64"))?;
    let sig: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| token_invalid("signature has invalid length"))?;
    let signature = Signature::from_bytes(&sig);

    let claims: TokenClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|_| claims_invalid("claims are invalid or contain unknown fields"))?;
    let canonical = canonical_claims(&claims)?;
    if canonical != claims_bytes {
        return Err(claims_invalid("claims are not canonical"));
    }

    for root in roots.entries() {
        let key = verifying_key_from_b64(&root.pubkey_b64)
            .map_err(|_| token_invalid("root public key is invalid"))?;
        if key.verify(&claims_bytes, &signature).is_ok() {
            return Ok((claims, root.fpr.clone()));
        }
    }
    Err(token_invalid(
        "signature does not verify against trusted roots",
    ))
}

pub fn mint_revocation(
    home: &A2aHome,
    gateway_id: &str,
    key_epoch: u64,
    clock: &dyn Clock,
) -> Result<String, AppError> {
    let key_path = fleet_dir(home)?.join(ROOT_KEY);
    let root = read_root_key(&key_path)?;
    let claims = RevocationClaims {
        fleet_id: root.fleet_id,
        gateway_id: gateway_id.to_string(),
        key_epoch,
        revoked_at: clock.now().timestamp(),
    };
    let claims_bytes = canonical_revocation_claims(&claims)?;
    sign_claim_bytes_with_key(&claims_bytes, &key_path)
}

pub fn verify_revocation(
    record_b64: &str,
    roots: &RootSet,
) -> Result<RevocationClaims, AppError> {
    let record = b64_decode(record_b64)
        .map_err(|_| revocation_invalid("revocation record is not base64"))?;
    let envelope: TokenEnvelope = serde_json::from_slice(&record)
        .map_err(|_| revocation_invalid("revocation envelope is invalid"))?;
    if envelope.v != TOKEN_VERSION {
        return Err(revocation_invalid("revocation version is unsupported"));
    }
    let claims_bytes = b64_decode(&envelope.claims_b64)
        .map_err(|_| revocation_invalid("revocation claims are not base64"))?;
    let sig_bytes = b64_decode(&envelope.sig_b64)
        .map_err(|_| revocation_invalid("revocation signature is not base64"))?;
    let sig: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| revocation_invalid("revocation signature has invalid length"))?;
    let signature = Signature::from_bytes(&sig);
    let claims: RevocationClaims = serde_json::from_slice(&claims_bytes).map_err(|_| {
        revocation_invalid("revocation claims are invalid or contain unknown fields")
    })?;
    let canonical = canonical_revocation_claims(&claims)?;
    if canonical != claims_bytes {
        return Err(revocation_invalid("revocation claims are not canonical"));
    }
    for root in roots.entries() {
        let key = verifying_key_from_b64(&root.pubkey_b64)
            .map_err(|_| revocation_invalid("root public key is invalid"))?;
        if key.verify(&claims_bytes, &signature).is_ok() {
            return Ok(claims);
        }
    }
    Err(revocation_invalid(
        "revocation signature does not verify against trusted roots",
    ))
}

pub fn unverified_claims(token_b64: &str) -> Result<TokenClaims, AppError> {
    let token = b64_decode(token_b64).map_err(|_| token_invalid("token is not base64"))?;
    let envelope: TokenEnvelope =
        serde_json::from_slice(&token).map_err(|_| token_invalid("token envelope is invalid"))?;
    let claims_bytes =
        b64_decode(&envelope.claims_b64).map_err(|_| token_invalid("claims are not base64"))?;
    serde_json::from_slice(&claims_bytes)
        .map_err(|_| claims_invalid("claims are invalid or contain unknown fields"))
}

pub fn mint_ulid(clock: &dyn Clock) -> String {
    let millis = clock.now().timestamp_millis().max(0) as u64;
    let when = SystemTime::UNIX_EPOCH + StdDuration::from_millis(millis);
    Ulid::from_datetime(when).to_string()
}

fn sign_claim_bytes(claims_bytes: &[u8], home: &A2aHome) -> Result<String, AppError> {
    sign_claim_bytes_with_key(claims_bytes, &fleet_dir(home)?.join(ROOT_KEY))
}

pub(crate) fn sign_claim_bytes_with_key(
    claims_bytes: &[u8],
    key_path: &Path,
) -> Result<String, AppError> {
    let root = read_root_key(key_path)?;
    let signing = signing_key_from_b64(&root.secret_b64).map_err(|_| root_key_invalid())?;
    let sig = signing.sign(claims_bytes);
    let claims_b64 = b64_encode(claims_bytes);
    let sig_b64 = b64_encode(&sig.to_bytes());
    let envelope = TokenEnvelopeOut {
        v: TOKEN_VERSION,
        claims_b64: &claims_b64,
        sig_b64: &sig_b64,
    };
    let bytes = serde_json::to_vec(&envelope)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
    Ok(b64_encode(&bytes))
}

fn root_fingerprint(root: &RootPubFile) -> Result<String, AppError> {
    let key = b64_decode(&root.pubkey_b64).map_err(|_| {
        AppError::new(
            "ROOT_PUB_INVALID",
            ExitCode::Precondition,
            "fleet root public key file is invalid",
        )
    })?;
    Ok(hex(&Sha256::digest(key)))
}

fn root_entry(root: RootPubFile, role: RootRole) -> Result<RootEntry, AppError> {
    let fpr = root_fingerprint(&root)?;
    Ok(RootEntry {
        role,
        fleet_id: root.fleet_id,
        pubkey_b64: root.pubkey_b64,
        fpr,
    })
}

fn canonical_claims(claims: &TokenClaims) -> Result<Vec<u8>, AppError> {
    let mut fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("exp", json!(claims.exp)),
        ("fleet_id", json!(claims.fleet_id)),
        ("gateway_id", json!(claims.gateway_id)),
        ("issued_at", json!(claims.issued_at)),
        ("pubkey_b64", json!(claims.pubkey_b64)),
    ]);
    if claims.key_epoch != 0 {
        fields.insert("key_epoch", json!(claims.key_epoch));
    }
    canonical_json_fields(fields)
}

fn canonical_announcement(
    announcement: &RotationAnnouncement,
) -> Result<Vec<u8>, AppError> {
    canonical_json_fields(BTreeMap::from([
        ("fleet_id", json!(&announcement.fleet_id)),
        ("issued_at", json!(announcement.issued_at)),
        ("next_fpr", json!(&announcement.next_fpr)),
        ("next_pubkey_b64", json!(&announcement.next_pubkey_b64)),
    ]))
}

fn canonical_revocation_claims(claims: &RevocationClaims) -> Result<Vec<u8>, AppError> {
    canonical_json_fields(BTreeMap::from([
        ("fleet_id", json!(claims.fleet_id)),
        ("gateway_id", json!(claims.gateway_id)),
        ("key_epoch", json!(claims.key_epoch)),
        ("revoked_at", json!(claims.revoked_at)),
    ]))
}

pub(crate) fn canonical_json_fields(fields: BTreeMap<&str, Value>) -> Result<Vec<u8>, AppError> {
    serde_json::to_vec(&fields)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))
}

fn check_claim_clock(claims: &TokenClaims, now: i64) -> Result<(), AppError> {
    if now > claims.exp + SKEW_TOLERANCE_SECS {
        return Err(AppError::new(
            "TOKEN_EXPIRED",
            ExitCode::Precondition,
            format!("token expired: exp={} now={now}", claims.exp),
        ));
    }
    if now < claims.issued_at - SKEW_TOLERANCE_SECS {
        return Err(AppError::new(
            "TOKEN_NOT_YET_VALID",
            ExitCode::Precondition,
            format!(
                "token not yet valid: issued_at={} now={now}",
                claims.issued_at
            ),
        ));
    }
    Ok(())
}

fn ensure_tls_material(dir: &Path) -> Result<String, AppError> {
    let cert_path = dir.join(TLS_CERT);
    let key_path = dir.join(TLS_KEY);
    let fpr_path = dir.join(TLS_FPR);
    if cert_path.exists() && key_path.exists() && fpr_path.exists() {
        let fpr = fs::read_to_string(&fpr_path)
            .map_err(|e| io_err("reading gateway TLS fingerprint", e))?;
        let fpr = fpr.trim().to_string();
        if is_lower_hex(&fpr, 64) {
            return Ok(fpr);
        }
    }

    let cert = rcgen::generate_simple_self_signed(vec![hostname_slug()])
        .map_err(|e| AppError::new("TLS_CERT", ExitCode::Internal, e.to_string()))?;
    let spki = cert.get_key_pair().public_key_der();
    let fpr = hex(&Sha256::digest(spki));
    write_string_0644(&cert_path, &cert.serialize_pem().map_err(tls_err)?)?;
    write_string_0600(&key_path, &cert.serialize_private_key_pem())?;
    write_string_0644(&fpr_path, &(fpr.clone() + "\n"))?;
    Ok(fpr)
}

fn write_root_pub(path: &Path, root: &RootKeyFile) -> Result<(), AppError> {
    let signing = signing_key_from_b64(&root.secret_b64).map_err(|_| root_key_invalid())?;
    let pub_file = RootPubFile {
        fleet_id: root.fleet_id.clone(),
        pubkey_b64: b64_encode(&signing.verifying_key().to_bytes()),
    };
    write_json_0644(path, &pub_file)
}

fn write_root_pub_file(path: &Path, root: &RootPubFile) -> Result<(), AppError> {
    write_json_0644(path, root)
}

fn read_root_key(path: &Path) -> Result<RootKeyFile, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading fleet root key", e))?;
    serde_json::from_slice(&bytes).map_err(|_| root_key_invalid())
}

fn read_root_pub(path: &Path) -> Result<RootPubFile, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading fleet root public key", e))?;
    serde_json::from_slice(&bytes).map_err(|_| {
        AppError::new(
            "ROOT_PUB_INVALID",
            ExitCode::Precondition,
            "fleet root public key file is invalid",
        )
    })
}

fn read_gateway_key(path: &Path) -> Result<GatewayKeyFile, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading gateway key", e))?;
    let key: GatewayKeyFile = serde_json::from_slice(&bytes).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })?;
    signing_key_from_b64(&key.secret_b64).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })?;
    Ok(key)
}

fn new_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    OsRng.fill_bytes(&mut seed);
    SigningKey::from_bytes(&seed)
}

fn signing_key_from_b64(s: &str) -> Result<SigningKey, ()> {
    let bytes = b64_decode(s).map_err(|_| ())?;
    let seed: [u8; 32] = bytes.try_into().map_err(|_| ())?;
    Ok(SigningKey::from_bytes(&seed))
}

pub(crate) fn verifying_key_from_b64(s: &str) -> Result<VerifyingKey, ()> {
    let bytes = b64_decode(s).map_err(|_| ())?;
    let key: [u8; 32] = bytes.try_into().map_err(|_| ())?;
    VerifyingKey::from_bytes(&key).map_err(|_| ())
}

fn validate_gateway_pubkey_b64(pubkey_b64: &str) -> Result<(), AppError> {
    verifying_key_from_b64(pubkey_b64).map(|_| ()).map_err(|_| {
        AppError::new(
            "INVALID_PUBKEY",
            ExitCode::Precondition,
            "gateway public key must be a base64-encoded ed25519 verifying key",
        )
    })
}

fn remove_file_if_present(path: &Path, context: &'static str) -> Result<(), AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_err(context, error)),
    }
}

fn rename_file(from: &Path, to: &Path, context: &'static str) -> Result<(), AppError> {
    fs::rename(from, to).map_err(|error| io_err(context, error))
}

fn token_mismatch(field: &'static str, expected: &str, actual: &str) -> AppError {
    AppError::new(
        "TOKEN_MISMATCH",
        ExitCode::Precondition,
        format!("token {field} does not match this gateway: expected {expected}, got {actual}"),
    )
}

fn gateway_fleet_mismatch(token_fleet_id: &str, home_fleet_id: &str) -> AppError {
    AppError::new(
        "GATEWAY_FLEET_MISMATCH",
        ExitCode::Precondition,
        format!("token belongs to {token_fleet_id}, this home's root.pub says {home_fleet_id}"),
    )
    .remedy("restore the fleet's root.pub (docs/mesh-join.md) or re-enroll this gateway")
}

fn gateway_token_mismatch(field: &'static str, expected: &str, actual: &str) -> AppError {
    AppError::new(
        "GATEWAY_TOKEN_MISMATCH",
        ExitCode::Precondition,
        format!(
            "gateway token {field} mismatch: expected local {field} {expected}, got {actual}"
        ),
    )
    .remedy("re-enroll: `a2a mesh fleet enroll` on the root machine, `a2a mesh gateway install-token` here")
}

fn gateway_token_expired(detail: String) -> AppError {
    AppError::new("GATEWAY_TOKEN_EXPIRED", ExitCode::Precondition, detail)
        .remedy("re-enroll: `a2a mesh fleet enroll` on the root machine, `a2a mesh gateway install-token` here")
}

fn gateway_token_invalid(detail: String) -> AppError {
    AppError::new(
        "GATEWAY_TOKEN_INVALID",
        ExitCode::Precondition,
        format!("gateway token is invalid: {detail}"),
    )
    .remedy("re-enroll: `a2a mesh fleet enroll` on the root machine, `a2a mesh gateway install-token` here")
}

fn fleet_dir(home: &A2aHome) -> Result<PathBuf, AppError> {
    let dir = home.root().join("mesh").join("fleet");
    create_dir_0700(&dir).map_err(|e| io_err("creating fleet key directory", e))?;
    Ok(dir)
}

fn gateway_dir(home: &A2aHome) -> Result<PathBuf, AppError> {
    let dir = home.root().join("mesh").join("gateway");
    create_dir_0700(&dir).map_err(|e| io_err("creating gateway key directory", e))?;
    Ok(dir)
}

fn write_json_0600<T: Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
    write_bytes_0600(path, &bytes)
}

fn write_json_0644<T: Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
    bytes.push(b'\n');
    write_bytes_0644(path, &bytes)
}

fn write_string_0600(path: &Path, text: &str) -> Result<(), AppError> {
    write_bytes_0600(path, text.as_bytes())
}

fn write_string_0644(path: &Path, text: &str) -> Result<(), AppError> {
    write_bytes_0644(path, text.as_bytes())
}

fn write_bytes_0600(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let mut f = write_new_0600(path).map_err(|e| io_err("writing private key file", e))?;
    f.write_all(bytes)
        .map_err(|e| io_err("writing private key file", e))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|e| io_err("setting private key permissions", e))
}

fn write_bytes_0644(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o644)
        .open(path)
        .map_err(|e| io_err("writing public key file", e))?;
    f.write_all(bytes)
        .map_err(|e| io_err("writing public key file", e))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644))
        .map_err(|e| io_err("setting public key permissions", e))
}

fn hostname_slug() -> String {
    let raw = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_string());
    let mut out = String::new();
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if (c == '-' || c == '_' || c == '.') && !out.ends_with('-') {
            out.push('-');
        }
    }
    let out = out.trim_matches('-');
    if out.is_empty() {
        "host".to_string()
    } else {
        out.to_string()
    }
}

fn token_invalid(message: &'static str) -> AppError {
    AppError::new("TOKEN_INVALID", ExitCode::Precondition, message)
}

fn claims_invalid(message: &'static str) -> AppError {
    AppError::new("TOKEN_CLAIMS_INVALID", ExitCode::Precondition, message)
}

fn revocation_invalid(message: &'static str) -> AppError {
    AppError::new("REVOCATION_INVALID", ExitCode::Precondition, message)
        .remedy("discard the record and mint a fresh revocation on the fleet root machine")
}

fn root_rotation_staged() -> AppError {
    AppError::new(
        "ROOT_ROTATION_STAGED",
        ExitCode::Precondition,
        "a staged fleet root already exists",
    )
    .remedy(
        "run `a2a mesh fleet rotate-root --abort` after confirming it is unused, or restage deliberately with --force",
    )
}

fn root_rotation_abort_refused(staged_fpr: &str, gateway_ids: &[String]) -> AppError {
    AppError::new(
        "ROOT_ROTATION_ABORT_REFUSED",
        ExitCode::Precondition,
        format!(
            "staged root {staged_fpr} issued enrollment tokens for {}; refusing to strand those gateways",
            gateway_ids.join(", ")
        ),
    )
    .remedy(
        "re-enroll the named gateways under the current root, or use --force only after confirming the staged enrollments will be discarded",
    )
}

fn root_next_missing() -> AppError {
    AppError::new(
        "ROOT_NEXT_MISSING",
        ExitCode::Precondition,
        "the staged fleet root key pair is missing",
    )
    .remedy("run `a2a mesh fleet rotate-root --stage` on the fleet root machine")
}

fn root_prev_missing() -> AppError {
    AppError::new(
        "ROOT_PREV_MISSING",
        ExitCode::Precondition,
        "the previous fleet root key pair is missing",
    )
    .remedy("promote a staged root before attempting to retire the previous root")
}

fn root_next_fleet_mismatch(
    role: RootRole,
    current_fleet_id: &str,
    other_fleet_id: &str,
) -> AppError {
    let held = match role {
        RootRole::Current => "current",
        RootRole::Next => "next",
        RootRole::Prev => "previous",
    };
    AppError::new(
        "ROOT_NEXT_FLEET_MISMATCH",
        ExitCode::Precondition,
        format!(
            "{held} root belongs to {other_fleet_id}, but the current root belongs to {current_fleet_id}"
        ),
    )
    .remedy("remove the mismatched staged/previous root and install only roots for this fleet")
}

fn root_announcement_invalid(message: &'static str) -> AppError {
    AppError::new(
        "ROOT_ANNOUNCEMENT_INVALID",
        ExitCode::Precondition,
        message,
    )
    .remedy(
        "install only the current-root-signed rotation announcement; a bare root.pub.next is never installed",
    )
}

fn root_announcement_fleet_mismatch(
    local_fleet_id: &str,
    announced_fleet_id: &str,
) -> AppError {
    AppError::new(
        "ROOT_ANNOUNCEMENT_FLEET_MISMATCH",
        ExitCode::Precondition,
        format!(
            "rotation announcement belongs to {announced_fleet_id}, but this gateway belongs to {local_fleet_id}"
        ),
    )
    .remedy("obtain the signed rotation announcement for this gateway's fleet")
}

fn root_key_invalid() -> AppError {
    AppError::new(
        "ROOT_KEY_INVALID",
        ExitCode::Precondition,
        "fleet root key file is invalid",
    )
}

fn tls_err(e: rcgen::RcgenError) -> AppError {
    AppError::new("TLS_CERT", ExitCode::Internal, e.to_string())
}

fn io_err(ctx: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn b64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub(crate) fn b64_decode(s: &str) -> Result<Vec<u8>, ()> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let a = b64_val(chunk[0])?;
        let b = b64_val(chunk[1])?;
        let c_pad = chunk[2] == b'=';
        let d_pad = chunk[3] == b'=';
        let c = if c_pad { 0 } else { b64_val(chunk[2])? };
        let d = if d_pad { 0 } else { b64_val(chunk[3])? };
        if c_pad && !d_pad {
            return Err(());
        }
        out.push((a << 2) | (b >> 4));
        if !c_pad {
            out.push(((b & 0x0f) << 4) | (c >> 2));
        }
        if !d_pad {
            out.push(((c & 0x03) << 6) | d);
        }
    }
    Ok(out)
}

fn b64_val(b: u8) -> Result<u8, ()> {
    match b {
        b'A'..=b'Z' => Ok(b - b'A'),
        b'a'..=b'z' => Ok(b - b'a' + 26),
        b'0'..=b'9' => Ok(b - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn stuffed_claims_are_refused_even_when_signature_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        fleet_init(&home).unwrap();
        let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

        let stuffed = sign_claim_bytes(
            br#"{"exp":1700001000,"fleet_id":"fleet-1234abcd","gateway_id":"gw-testhost-abcdef","issued_at":1699999000,"pubkey_b64":"AQIDBA==","role":"admin"}"#,
            &home,
        )
        .unwrap();
        let err = verify_token(&stuffed, &RootSet::load(&home).unwrap(), &clock).unwrap_err();
        assert_eq!(err.exit, ExitCode::Precondition);
        assert_eq!(err.code, "TOKEN_CLAIMS_INVALID");
    }
}
