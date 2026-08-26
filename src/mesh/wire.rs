use crate::mesh::keys::{verify_token, Clock, RootSet, TokenClaims};
use crate::mesh::{FeedEntry, Note, Presence};
use crate::output::{AppError, ExitCode};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, ServerConfig, SignatureScheme};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Once};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, client_async, WebSocketStream};

pub const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;

static RUSTLS_PROVIDER: Once = Once::new();

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginDigest {
    pub origin: String,
    pub max_seq: u64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginWant {
    pub origin: String,
    pub from_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    Challenge { nonce: String },
    Hello { token: String, nonce_sig: String },
    Feed { origin: String, entries: Vec<FeedEntry> },
    Note { note: Note },
    Presence { peers: Vec<Presence> },
    Digest { origins: Vec<OriginDigest> },
    Want { origins: Vec<OriginWant> },
    Ack { ref_id: String, outcome: String },
    Err { code: String, detail: String },
}

impl Frame {
    pub fn to_ndjson(&self) -> Result<String, AppError> {
        let mut line = serde_json::to_string(self).map_err(data_err)?;
        line.push('\n');
        if line.len() > MAX_FRAME_BYTES {
            return Err(AppError::new(
                "WIRE_FRAME_TOO_LARGE",
                ExitCode::Precondition,
                format!("frame is {} bytes, exceeding cap {MAX_FRAME_BYTES} bytes", line.len()),
            ));
        }
        Ok(line)
    }

    pub fn from_ndjson(line: &str) -> Result<Option<Self>, AppError> {
        if line.len() > MAX_FRAME_BYTES {
            return Err(AppError::new(
                "WIRE_FRAME_TOO_LARGE",
                ExitCode::Precondition,
                format!("frame is {} bytes, exceeding cap {MAX_FRAME_BYTES} bytes", line.len()),
            ));
        }
        let value: serde_json::Value = serde_json::from_str(line.trim_end()).map_err(data_err)?;
        let Some(kind) = value.get("kind").and_then(serde_json::Value::as_str) else {
            return Err(AppError::new(
                "WIRE_FRAME_INVALID",
                ExitCode::Precondition,
                "frame is missing string kind",
            ));
        };
        if !is_known_kind(kind) {
            return Ok(None);
        }
        serde_json::from_value(value)
            .map(Some)
            .map_err(|e| AppError::new("WIRE_FRAME_INVALID", ExitCode::Precondition, e.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct HandshakeIdentity {
    token: String,
    signing_key: SigningKey,
}

impl HandshakeIdentity {
    pub fn new(token: String, signing_key: SigningKey) -> Self {
        Self { token, signing_key }
    }
}

pub struct Side<S> {
    ws: WebSocketStream<S>,
}

impl<S> std::fmt::Debug for Side<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Side").finish_non_exhaustive()
    }
}

impl<S> Side<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(ws: WebSocketStream<S>) -> Self {
        Self { ws }
    }

    pub async fn send(&mut self, frame: &Frame) -> Result<(), AppError> {
        let line = frame.to_ndjson()?;
        self.send_raw_text(&line).await
    }

    pub async fn send_raw_text(&mut self, text: &str) -> Result<(), AppError> {
        self.ws
            .send(Message::Text(text.to_string()))
            .await
            .map_err(ws_err)
    }

    pub async fn recv(&mut self) -> Result<Option<Frame>, AppError> {
        loop {
            let Some(message) = self.ws.next().await else {
                return Ok(None);
            };
            let message = match message {
                Ok(message) => message,
                Err(e) if is_clean_close_error(&e) => return Ok(None),
                Err(e) => return Err(ws_err(e)),
            };
            match message {
                Message::Text(text) => match Frame::from_ndjson(&text) {
                    Ok(Some(frame)) => return Ok(Some(frame)),
                    Ok(None) => continue,
                    Err(err) if err.code == "WIRE_FRAME_TOO_LARGE" => {
                        let _ = self.ws.close(None).await;
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                },
                Message::Binary(bytes) => {
                    if bytes.len() > MAX_FRAME_BYTES {
                        let _ = self.ws.close(None).await;
                        return Err(AppError::new(
                            "WIRE_FRAME_TOO_LARGE",
                            ExitCode::Precondition,
                            format!("frame is {} bytes, exceeding cap {MAX_FRAME_BYTES} bytes", bytes.len()),
                        ));
                    }
                    let text = String::from_utf8(bytes).map_err(|_| {
                        AppError::new("WIRE_FRAME_INVALID", ExitCode::Precondition, "frame is not utf-8")
                    })?;
                    if let Some(frame) = Frame::from_ndjson(&text)? {
                        return Ok(Some(frame));
                    }
                }
                Message::Close(_) => return Ok(None),
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Frame(_) => continue,
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), AppError> {
        self.ws.close(None).await.map_err(ws_err)
    }

    pub fn split(self) -> (FrameWriter<S>, FrameReader<S>) {
        let (sink, stream) = self.ws.split();
        (FrameWriter { sink }, FrameReader { stream })
    }
}

pub struct FrameWriter<S> {
    sink: SplitSink<WebSocketStream<S>, Message>,
}

impl<S> std::fmt::Debug for FrameWriter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriter").finish_non_exhaustive()
    }
}

impl<S> FrameWriter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn send(&mut self, frame: &Frame) -> Result<(), AppError> {
        let line = frame.to_ndjson()?;
        self.sink.send(Message::Text(line)).await.map_err(ws_err)
    }

    pub async fn close(&mut self) -> Result<(), AppError> {
        self.sink.close().await.map_err(ws_err)
    }
}

pub struct FrameReader<S> {
    stream: SplitStream<WebSocketStream<S>>,
}

impl<S> std::fmt::Debug for FrameReader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReader").finish_non_exhaustive()
    }
}

impl<S> FrameReader<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn recv(&mut self) -> Result<Option<Frame>, AppError> {
        loop {
            let Some(message) = self.stream.next().await else {
                return Ok(None);
            };
            let message = match message {
                Ok(message) => message,
                Err(e) if is_clean_close_error(&e) => return Ok(None),
                Err(e) => return Err(ws_err(e)),
            };
            match message {
                Message::Text(text) => match Frame::from_ndjson(&text)? {
                    Some(frame) => return Ok(Some(frame)),
                    None => continue,
                },
                Message::Binary(bytes) => {
                    if bytes.len() > MAX_FRAME_BYTES {
                        return Err(AppError::new(
                            "WIRE_FRAME_TOO_LARGE",
                            ExitCode::Precondition,
                            format!("frame is {} bytes, exceeding cap {MAX_FRAME_BYTES} bytes", bytes.len()),
                        ));
                    }
                    let text = String::from_utf8(bytes).map_err(|_| {
                        AppError::new("WIRE_FRAME_INVALID", ExitCode::Precondition, "frame is not utf-8")
                    })?;
                    if let Some(frame) = Frame::from_ndjson(&text)? {
                        return Ok(Some(frame));
                    }
                }
                Message::Close(_) => return Ok(None),
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Frame(_) => continue,
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TlsIdentity {
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
    spki_fpr: String,
}

impl TlsIdentity {
    pub fn self_signed(subject_alt_names: Vec<String>) -> Result<Self, AppError> {
        let cert = rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(|e| AppError::new("TLS_CERT", ExitCode::Internal, e.to_string()))?;
        let spki_fpr = hex(&Sha256::digest(cert.get_key_pair().public_key_der()));
        let cert_der = cert
            .serialize_der()
            .map_err(|e| AppError::new("TLS_CERT", ExitCode::Internal, e.to_string()))?;
        Ok(Self {
            cert_der,
            key_der: cert.serialize_private_key_der(),
            spki_fpr,
        })
    }

    pub fn spki_fpr(&self) -> &str {
        &self.spki_fpr
    }
}

pub async fn connect_pinned(
    url: &str,
    expected_cert_fpr: &str,
) -> Result<Side<tokio_rustls::client::TlsStream<TcpStream>>, AppError> {
    let endpoint = parse_wss_url(url)?;
    let stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(|e| AppError::new("TLS_CONNECT", ExitCode::BrokerUnavailable, e.to_string()))?;
    connect_pinned_io(stream, url, expected_cert_fpr).await
}

pub async fn connect_pinned_io<S>(
    stream: S,
    url: &str,
    expected_cert_fpr: &str,
) -> Result<Side<tokio_rustls::client::TlsStream<S>>, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    install_rustls_provider();
    let config = ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(tls_config_err)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedSpkiVerifier::new(expected_cert_fpr)?))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from("pinned.invalid")
        .map_err(|e| AppError::new("TLS_SERVER_NAME", ExitCode::Internal, e.to_string()))?;
    let tls = connector
        .connect(server_name, stream)
        .await
        .map_err(tls_connect_err)?;
    let endpoint = parse_wss_url(url)?;
    let request = format!("wss://{}:{}{}", endpoint.host, endpoint.port, endpoint.path);
    let (ws, _) = client_async(request, tls).await.map_err(ws_err)?;
    Ok(Side::new(ws))
}

pub async fn accept_pinned(
    stream: TcpStream,
    tls: TlsIdentity,
) -> Result<Side<tokio_rustls::server::TlsStream<TcpStream>>, AppError> {
    accept_pinned_io(stream, tls).await
}

pub async fn accept_pinned_io<S>(
    stream: S,
    tls: TlsIdentity,
) -> Result<Side<tokio_rustls::server::TlsStream<S>>, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    install_rustls_provider();
    let cert = CertificateDer::from(tls.cert_der);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(tls.key_der));
    let config = ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(tls_config_err)?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(tls_config_err)?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let tls = acceptor.accept(stream).await.map_err(tls_connect_err)?;
    let ws = accept_async(tls).await.map_err(ws_err)?;
    Ok(Side::new(ws))
}

pub async fn test_link<C: Clock + ?Sized>(_clock: &C) -> (Side<DuplexStream>, Side<DuplexStream>) {
    let (a, b) = tokio::io::duplex(MAX_FRAME_BYTES * 2 + 4096);
    let client = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
    (Side::new(client), Side::new(server))
}

pub async fn handshake_acceptor<S>(
    side: &mut Side<S>,
    identity: &HandshakeIdentity,
    roots: &RootSet,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let nonce = nonce();
    side.send(&Frame::Challenge { nonce: nonce.clone() }).await?;
    let peer = read_hello(side, roots, clock, &nonce).await?;
    let Frame::Challenge { nonce: peer_nonce } = expect_frame(side.recv().await?, "challenge")? else {
        return Err(unexpected_frame("challenge"));
    };
    side.send(&hello_frame(identity, &peer_nonce)).await?;
    Ok(peer)
}

pub async fn handshake_dialer<S>(
    side: &mut Side<S>,
    identity: &HandshakeIdentity,
    roots: &RootSet,
    clock: &dyn Clock,
) -> Result<TokenClaims, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Frame::Challenge { nonce: peer_nonce } = expect_frame(side.recv().await?, "challenge")? else {
        return Err(unexpected_frame("challenge"));
    };
    side.send(&hello_frame(identity, &peer_nonce)).await?;
    let local_nonce = nonce();
    side.send(&Frame::Challenge { nonce: local_nonce.clone() }).await?;
    read_hello(side, roots, clock, &local_nonce).await
}

async fn read_hello<S>(
    side: &mut Side<S>,
    roots: &RootSet,
    clock: &dyn Clock,
    nonce: &str,
) -> Result<TokenClaims, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match expect_frame(side.recv().await?, "hello")? {
        Frame::Hello { token, nonce_sig } => match verify_hello(&token, &nonce_sig, roots, clock, nonce) {
            Ok(claims) => Ok(claims),
            Err(err) => {
                let _ = side
                    .send(&Frame::Err { code: err.code.to_string(), detail: err.message.clone() })
                    .await;
                let _ = side.close().await;
                Err(err)
            }
        },
        Frame::Err { code, detail } => Err(AppError::new(
            "WIRE_PEER_ERR",
            ExitCode::Precondition,
            format!("peer refused handshake: {code}: {detail}"),
        )),
        _ => Err(unexpected_frame("hello")),
    }
}

fn verify_hello(
    token: &str,
    nonce_sig: &str,
    roots: &RootSet,
    clock: &dyn Clock,
    nonce: &str,
) -> Result<TokenClaims, AppError> {
    let claims = verify_token(token, roots, clock)?;
    let pubkey_bytes: [u8; 32] = b64_decode(&claims.pubkey_b64)
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "token public key is invalid"))?
        .try_into()
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "token public key length is invalid"))?;
    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "token public key is invalid"))?;
    let sig_bytes: [u8; 64] = b64_decode(nonce_sig)
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "nonce signature is not base64"))?
        .try_into()
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "nonce signature length is invalid"))?;
    let signature = Signature::from_bytes(&sig_bytes);
    verifying_key
        .verify(nonce.as_bytes(), &signature)
        .map_err(|_| AppError::new("NONCE_SIG_INVALID", ExitCode::Precondition, "nonce signature does not match token public key"))?;
    Ok(claims)
}

fn hello_frame(identity: &HandshakeIdentity, nonce: &str) -> Frame {
    let sig = identity.signing_key.sign(nonce.as_bytes());
    Frame::Hello {
        token: identity.token.clone(),
        nonce_sig: b64_encode(&sig.to_bytes()),
    }
}

fn expect_frame(frame: Option<Frame>, expected: &'static str) -> Result<Frame, AppError> {
    frame.ok_or_else(|| {
        AppError::new(
            "WIRE_CLOSED",
            ExitCode::Precondition,
            format!("connection closed before {expected}"),
        )
    })
}

fn unexpected_frame(expected: &'static str) -> AppError {
    AppError::new(
        "WIRE_FRAME_UNEXPECTED",
        ExitCode::Precondition,
        format!("expected {expected} frame"),
    )
}

#[derive(Debug)]
struct PinnedSpkiVerifier {
    expected: [u8; 32],
    supported: WebPkiSupportedAlgorithms,
}

impl PinnedSpkiVerifier {
    fn new(expected_hex: &str) -> Result<Self, AppError> {
        Ok(Self {
            expected: hex_decode_32(expected_hex)?,
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        })
    }
}

impl ServerCertVerifier for PinnedSpkiVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let spki = extract_spki(end_entity.as_ref())
            .ok_or_else(|| TlsError::General("TLS_CERT_PIN_MISMATCH: invalid cert".to_string()))?;
        let digest: [u8; 32] = Sha256::digest(spki).into();
        if digest == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::General("TLS_CERT_PIN_MISMATCH".to_string()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

fn is_known_kind(kind: &str) -> bool {
    matches!(
        kind,
        "challenge" | "hello" | "feed" | "note" | "presence" | "digest" | "want" | "ack" | "err"
    )
}

fn parse_wss_url(url: &str) -> Result<Endpoint, AppError> {
    let rest = url.strip_prefix("wss://").ok_or_else(|| {
        AppError::new("WIRE_URL_INVALID", ExitCode::Precondition, "wire URL must start with wss://")
    })?;
    let (authority, path) = rest.split_once('/').map_or((rest, "/"), |(a, p)| (a, p));
    let path = if path == "/" { "/".to_string() } else { format!("/{path}") };
    let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
        AppError::new("WIRE_URL_INVALID", ExitCode::Precondition, "wire URL must include an explicit port")
    })?;
    let port = port.parse::<u16>().map_err(|_| {
        AppError::new("WIRE_URL_INVALID", ExitCode::Precondition, "wire URL port is invalid")
    })?;
    Ok(Endpoint { host: host.to_string(), port, path })
}

struct Endpoint {
    host: String,
    port: u16,
    path: String,
}

fn nonce() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    b64_encode(&bytes)
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn tls_config_err(e: TlsError) -> AppError {
    AppError::new("TLS_CONFIG", ExitCode::Internal, e.to_string())
}

fn tls_connect_err(e: std::io::Error) -> AppError {
    let msg = e.to_string();
    if msg.contains("TLS_CERT_PIN_MISMATCH") {
        AppError::new("TLS_CERT_PIN_MISMATCH", ExitCode::Precondition, "server certificate SPKI fingerprint did not match pin")
    } else {
        AppError::new("TLS", ExitCode::BrokerUnavailable, msg)
    }
}

fn ws_err(e: tokio_tungstenite::tungstenite::Error) -> AppError {
    AppError::new("WIRE", ExitCode::BrokerUnavailable, e.to_string())
}

fn is_clean_close_error(e: &tokio_tungstenite::tungstenite::Error) -> bool {
    e.to_string()
        .contains("Connection reset without closing handshake")
}

fn data_err(e: serde_json::Error) -> AppError {
    AppError::new("WIRE_FRAME_INVALID", ExitCode::Precondition, e.to_string())
}

fn extract_spki(cert: &[u8]) -> Option<&[u8]> {
    let (_, cert_value, _, _) = der_tlv(cert, 0, 0x30)?;
    let (_, tbs_value, tbs_end, _) = der_tlv(cert, cert_value, 0x30)?;
    let mut pos = tbs_value;
    if cert.get(pos).copied()? == 0xa0 {
        let (_, _, next, _) = der_tlv_any(cert, pos)?;
        pos = next;
    }
    for _ in 0..5 {
        let (_, _, next, _) = der_tlv_any(cert, pos)?;
        pos = next;
        if pos > tbs_end {
            return None;
        }
    }
    let (spki_start, _, spki_end, tag) = der_tlv_any(cert, pos)?;
    if tag == 0x30 && spki_end <= tbs_end {
        Some(&cert[spki_start..spki_end])
    } else {
        None
    }
}

fn der_tlv(input: &[u8], pos: usize, expected_tag: u8) -> Option<(usize, usize, usize, u8)> {
    let out = der_tlv_any(input, pos)?;
    if out.3 == expected_tag {
        Some(out)
    } else {
        None
    }
}

fn der_tlv_any(input: &[u8], pos: usize) -> Option<(usize, usize, usize, u8)> {
    let tag = *input.get(pos)?;
    let len0 = *input.get(pos + 1)?;
    let (len, value_start) = if len0 & 0x80 == 0 {
        (len0 as usize, pos + 2)
    } else {
        let count = (len0 & 0x7f) as usize;
        if count == 0 || count > 4 {
            return None;
        }
        let mut len = 0usize;
        for b in input.get(pos + 2..pos + 2 + count)? {
            len = (len << 8) | *b as usize;
        }
        (len, pos + 2 + count)
    };
    let end = value_start.checked_add(len)?;
    if end <= input.len() {
        Some((pos, value_start, end, tag))
    } else {
        None
    }
}

fn hex_decode_32(s: &str) -> Result<[u8; 32], AppError> {
    if s.len() != 64 {
        return Err(AppError::new(
            "TLS_CERT_PIN_INVALID",
            ExitCode::Precondition,
            "certificate fingerprint pin must be 64 lowercase hex characters",
        ));
    }
    let mut out = [0u8; 32];
    for (idx, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0]).ok_or_else(invalid_pin)?;
        let lo = hex_val(chunk[1]).ok_or_else(invalid_pin)?;
        out[idx] = (hi << 4) | lo;
    }
    Ok(out)
}

fn invalid_pin() -> AppError {
    AppError::new(
        "TLS_CERT_PIN_INVALID",
        ExitCode::Precondition,
        "certificate fingerprint pin must be 64 lowercase hex characters",
    )
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
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
            out.push((b << 4) | (c >> 2));
        }
        if !d_pad {
            out.push((c << 6) | d);
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
