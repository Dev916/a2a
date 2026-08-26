use crate::home::{create_dir_0700, write_new_0600, A2aHome};
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
const ROOT_PUB: &str = "root.pub";
const ROOT_PUB_NEXT: &str = "root.pub.next";
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
    pub issued_at: i64,
    pub exp: i64,
}

#[derive(Clone, Debug)]
pub struct RootSet(Vec<RootPubFile>);

impl RootSet {
    pub fn load(home: &A2aHome) -> Result<Self, AppError> {
        let dir = fleet_dir(home)?;
        let mut roots = vec![read_root_pub(&dir.join(ROOT_PUB))?];
        let next = dir.join(ROOT_PUB_NEXT);
        if next.exists() {
            roots.push(read_root_pub(&next)?);
        }
        Ok(Self(roots))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootRotation {
    pub fleet_id: String,
    pub current_fingerprint: String,
    pub next_fingerprint: String,
    pub gateway_token_rotated: bool,
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

pub fn rotate_root(home: &A2aHome, clock: &dyn Clock) -> Result<RootRotation, AppError> {
    let current = fleet_init(home)?;
    let dir = fleet_dir(home)?;
    let current_pub = read_root_pub(&dir.join(ROOT_PUB))?;
    let signing = new_signing_key();
    let next_key = RootKeyFile {
        fleet_id: current.fleet_id.clone(),
        secret_b64: b64_encode(&signing.to_bytes()),
    };
    write_json_0600(&dir.join(ROOT_KEY_NEXT), &next_key)?;
    write_root_pub(&dir.join(ROOT_PUB_NEXT), &next_key)?;
    let next_pub = read_root_pub(&dir.join(ROOT_PUB_NEXT))?;
    let gateway_token_rotated = resign_gateway_token_with_next(home, clock)?.is_some();
    Ok(RootRotation {
        fleet_id: current.fleet_id,
        current_fingerprint: root_fingerprint(&current_pub)?,
        next_fingerprint: root_fingerprint(&next_pub)?,
        gateway_token_rotated,
    })
}

pub fn gateway_identity(home: &A2aHome, fresh: bool) -> Result<GatewayIdentity, AppError> {
    let dir = gateway_dir(home)?;
    let key_path = dir.join(GW_KEY);

    let gateway = if key_path.exists() && !fresh {
        read_gateway_key(&key_path)?
    } else {
        let signing = new_signing_key();
        let suffix = &hex(&Sha256::digest(signing.verifying_key().to_bytes()))[..6];
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
        .remedy("run `a2a mesh fleet init` on the root machine or install a token minted for this gateway"));
    }

    let identity = gateway_identity(home, false)?;
    let issued_at = clock.now().timestamp();
    let claims = TokenClaims {
        fleet_id: String::new(),
        gateway_id: identity.gateway_id,
        pubkey_b64: gateway_public_key_b64(home)?,
        issued_at,
        exp: issued_at + 90 * 24 * 60 * 60,
    };
    let token = sign_token(&claims, home)?;
    write_string_0600(&token_path, &(token + "\n"))?;
    Ok(true)
}

pub fn sign_token(claims: &TokenClaims, home: &A2aHome) -> Result<String, AppError> {
    let root = fleet_init(home)?;
    let mut claims = claims.clone();
    claims.fleet_id = root.fleet_id;
    let claims_bytes = canonical_claims(&claims)?;
    sign_claim_bytes(&claims_bytes, home)
}

fn resign_gateway_token_with_next(
    home: &A2aHome,
    clock: &dyn Clock,
) -> Result<Option<String>, AppError> {
    let gateway_path = gateway_dir(home)?.join(GW_KEY);
    if !gateway_path.exists() {
        return Ok(None);
    }
    let next_key_path = fleet_dir(home)?.join(ROOT_KEY_NEXT);
    if !next_key_path.exists() {
        return Ok(None);
    }
    let root = read_root_key(&next_key_path)?;
    let gateway = read_gateway_key(&gateway_path)?;
    let signing = signing_key_from_b64(&gateway.secret_b64).map_err(|_| {
        AppError::new(
            "GATEWAY_KEY_INVALID",
            ExitCode::Precondition,
            "gateway key file is invalid",
        )
    })?;
    let issued_at = clock.now().timestamp();
    let claims = TokenClaims {
        fleet_id: root.fleet_id.clone(),
        gateway_id: gateway.gateway_id,
        pubkey_b64: b64_encode(&signing.verifying_key().to_bytes()),
        issued_at,
        exp: issued_at + 90 * 24 * 60 * 60,
    };
    let claims_bytes = canonical_claims(&claims)?;
    let token = sign_claim_bytes_with_key(&claims_bytes, &next_key_path)?;
    write_string_0600(&gateway_dir(home)?.join(GW_TOKEN), &(token.clone() + "\n"))?;
    Ok(Some(token))
}

pub fn verify_token(
    token_b64: &str,
    roots: &RootSet,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError> {
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

    let mut verified = false;
    for root in &roots.0 {
        let key = verifying_key_from_b64(&root.pubkey_b64)
            .map_err(|_| token_invalid("root public key is invalid"))?;
        if key.verify(&claims_bytes, &signature).is_ok() {
            verified = true;
            break;
        }
    }
    if !verified {
        return Err(token_invalid(
            "signature does not verify against trusted roots",
        ));
    }

    check_claim_clock(&claims, clock.now().timestamp())?;
    Ok(claims)
}

pub fn mint_ulid(clock: &dyn Clock) -> String {
    let millis = clock.now().timestamp_millis().max(0) as u64;
    let when = SystemTime::UNIX_EPOCH + StdDuration::from_millis(millis);
    Ulid::from_datetime(when).to_string()
}

fn sign_claim_bytes(claims_bytes: &[u8], home: &A2aHome) -> Result<String, AppError> {
    sign_claim_bytes_with_key(claims_bytes, &fleet_dir(home)?.join(ROOT_KEY))
}

fn sign_claim_bytes_with_key(claims_bytes: &[u8], key_path: &Path) -> Result<String, AppError> {
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

fn canonical_claims(claims: &TokenClaims) -> Result<Vec<u8>, AppError> {
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("exp", json!(claims.exp)),
        ("fleet_id", json!(claims.fleet_id)),
        ("gateway_id", json!(claims.gateway_id)),
        ("issued_at", json!(claims.issued_at)),
        ("pubkey_b64", json!(claims.pubkey_b64)),
    ]);
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

fn verifying_key_from_b64(s: &str) -> Result<VerifyingKey, ()> {
    let bytes = b64_decode(s).map_err(|_| ())?;
    let key: [u8; 32] = bytes.try_into().map_err(|_| ())?;
    VerifyingKey::from_bytes(&key).map_err(|_| ())
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

fn b64_encode(bytes: &[u8]) -> String {
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

fn b64_decode(s: &str) -> Result<Vec<u8>, ()> {
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
