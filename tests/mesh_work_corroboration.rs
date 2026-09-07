use a2a::config::{self, Config};
use a2a::home::A2aHome;
use a2a::mesh::gateway::{notes_wire, presence_wire};
use a2a::mesh::keys::{
    self, enroll_gateway_token, enroll_gateway_token_at_epoch, fleet_init, gateway_identity,
    gateway_public_key_b64, install_gateway_token, RootSet, SystemClock,
};
use a2a::mesh::registry::{GatewayRow, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::row_mark::RevocationCache;
use a2a::mesh::row_sig::{
    sign_row, verify_row, RowMsg, RowVerdict, RowVerifyCfg, StoredRowColumns, ROW_MSG_VER,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, VerifiedEnvelope, WorkAddressing,
    WorkEnvelope, WorkRepo,
};
use a2a::mesh::work_exec::{
    approve_dry_run, corroborate_sender, CorroborationStage, GitAdapter, WorkSpawner,
};
use a2a::mesh::work_store::{self, WorkSource, WORK_OFFER_KIND};
use a2a::mesh::Note;
use a2a::output::AppError;
use chrono::{DateTime, Timelike, Utc};
use ed25519_dalek::SigningKey;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &A2aHome, joiner: &A2aHome) {
    fs::create_dir_all(joiner.root().join("mesh/fleet")).unwrap();
    fs::copy(
        root.root().join("mesh/fleet/root.pub"),
        joiner.root().join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, &SystemClock)
        .unwrap();
    install_gateway_token(joiner, &token, &SystemClock).unwrap();
    identity.gateway_id
}

fn trunc_us(now: DateTime<Utc>) -> DateTime<Utc> {
    now.with_nanosecond(now.nanosecond() / 1_000 * 1_000)
        .unwrap()
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Fixture {
    _tmp: tempfile::TempDir,
    root: A2aHome,
    sender: A2aHome,
    receiver: A2aHome,
    sender_gw: String,
    receiver_gw: String,
    envelope: WorkEnvelope,
    verified: VerifiedEnvelope,
}

impl Fixture {
    fn new(work_id: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = home_at(&tmp.path().join("root"));
        let sender = home_at(&tmp.path().join("sender"));
        let receiver = home_at(&tmp.path().join("receiver"));
        fleet_init(&root).unwrap();
        copy_root_pub(&root, &sender);
        copy_root_pub(&root, &receiver);
        let sender_gw = enroll_joiner(&root, &sender);
        let receiver_gw = enroll_joiner(&root, &receiver);
        let envelope = sign_envelope(
            &sender,
            UnsignedWorkEnvelope {
                work_id: work_id.to_string(),
                session: "codex:sender-session".to_string(),
                addressing: WorkAddressing::Directed(receiver_gw.clone()),
                repo: WorkRepo {
                    remote_url: "https://example.test/project.git".to_string(),
                    base_ref: "main".to_string(),
                },
                title: "Corroborate sender".to_string(),
                brief_text: "---\nlane: impl\n---\nRun the focused test.".to_string(),
                acceptance: "focused test passes".to_string(),
                ttl_days: 7,
                max_bytes: 262_144,
            },
            &SystemClock,
        )
        .unwrap();
        let verified = verify_envelope(&receiver, &envelope, &SystemClock).unwrap();
        Self {
            _tmp: tmp,
            root,
            sender,
            receiver,
            sender_gw,
            receiver_gw,
            envelope,
            verified,
        }
    }

    fn signed_sender_row_with(
        &self,
        token: String,
        pubkey_b64: String,
        signing: &SigningKey,
    ) -> GatewayRow {
        let now = trunc_us(Utc::now());
        let roots = RootSet::load(&self.receiver).unwrap();
        let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
        let msg = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: claims.fleet_id.clone(),
            gateway_id: claims.gateway_id.clone(),
            endpoint_url: Some("wss://sender.example.test:7787".to_string()),
            pubkey_fpr: keys::fpr_of_pubkey_b64(&pubkey_b64).unwrap(),
            cert_fpr: Some("sender-cert".to_string()),
            version: "0.3.0".to_string(),
            owner: "sender-owner".to_string(),
            created_at: now,
            schema_version: REGISTRY_SCHEMA_VERSION,
            key_epoch: claims.key_epoch,
            roots_held: "[]".to_string(),
            row_token_sha256: sha256_hex(token.as_bytes()),
            row_resign_secs: 86_400,
            registry_heartbeat_secs: 180,
            enforce_require_signed_rows: true,
            enforce_auto_push: false,
            signed_at: now,
        };
        let (row_msg, row_sig) = sign_row(&msg, signing).unwrap();
        GatewayRow {
            fleet_id: msg.fleet_id,
            gateway_id: msg.gateway_id,
            owner: msg.owner,
            endpoint_url: msg.endpoint_url,
            pubkey_fpr: msg.pubkey_fpr,
            cert_fpr: msg.cert_fpr,
            version: msg.version,
            created_at: msg.created_at,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: Some(token),
            row_sig: Some(row_sig),
            signed_at: Some(now),
            revocation: None,
        }
    }

    fn signed_sender_row(&self) -> GatewayRow {
        self.signed_sender_row_with(
            self.envelope.sender.token.clone(),
            self.envelope.sender.pubkey_b64.clone(),
            &keys::gateway_signing_key(&self.sender).unwrap(),
        )
    }

    fn valid_rekeyed_sender_row(&self) -> GatewayRow {
        let alternate = home_at(&self._tmp.path().join("alternate-key"));
        gateway_identity(&alternate, false).unwrap();
        let alternate_pubkey = gateway_public_key_b64(&alternate).unwrap();
        let signing = keys::gateway_signing_key(&alternate).unwrap();
        let (token, _claims, _issuer) = enroll_gateway_token_at_epoch(
            &self.root,
            &self.sender_gw,
            &alternate_pubkey,
            1,
            &SystemClock,
        )
        .unwrap();
        let row = self.signed_sender_row_with(token, alternate_pubkey, &signing);
        let verdict = verify_row(
            &StoredRowColumns::from(&row),
            &RootSet::load(&self.receiver).unwrap(),
            None,
            &RowVerifyCfg::new(Utc::now()),
        );
        assert_eq!(verdict, RowVerdict::ValidSigned);
        row
    }

    fn cache(&self, row: GatewayRow, verdict: RowVerdict) {
        presence_wire::refresh_registry_cache(&self.receiver, &[(row, verdict)], Utc::now())
            .unwrap();
    }

    fn cfg(&self, require_signed_rows: bool) -> Config {
        fs::write(
            self.receiver.root().join("config.toml"),
            format!("[mesh.gateway]\nrequire_signed_rows = {require_signed_rows}\n"),
        )
        .unwrap();
        config::load(&self.receiver).unwrap().0
    }

    fn spooled_offer(&self, note_id: &str) -> notes_wire::SpooledNote {
        let note = Note {
            note_id: Some(note_id.to_string()),
            from: "codex:sender-session".to_string(),
            to: format!("gateway:{}", self.receiver_gw),
            ts: Utc::now(),
            kind: WORK_OFFER_KIND.to_string(),
            text: format!("work offer: {} ({})", self.envelope.title, self.envelope.work_id),
            refs: json!({"work": self.envelope}),
            extra: serde_json::Map::new(),
        };
        notes_wire::SpooledNote {
            schema_version: 1,
            trace_id: note_id.to_string(),
            from_gateway: self.sender_gw.clone(),
            to_gateway: self.receiver_gw.clone(),
            to_peer: note.to.clone(),
            attempts: 0,
            next_attempt_after: None,
            note,
        }
    }

    fn store_for_approve(&self) {
        work_store::store_offer(&self.receiver, self.verified.clone(), WorkSource::Directed)
            .unwrap();
    }
}

#[derive(Default)]
struct FakeGit;

#[allow(async_fn_in_trait)]
impl GitAdapter for FakeGit {
    async fn ls_remote(&self, _remote_url: &str, _base_ref: &str) -> Result<(), AppError> {
        Ok(())
    }

    async fn clone_checkout(
        &self,
        _remote_url: &str,
        _base_ref: &str,
        _checkout: &Path,
    ) -> Result<(), AppError> {
        Ok(())
    }
}

#[derive(Default)]
struct FakeSpawner;

impl WorkSpawner for FakeSpawner {
    fn preflight(&self, _home: &A2aHome, _checkout: &Path) -> Result<(), AppError> {
        Ok(())
    }

    fn spawn(&self, _home: &A2aHome, _checkout: &Path, _brief: &Path) -> Result<String, AppError> {
        Ok("impl-corroborated".to_string())
    }
}

#[tokio::test]
async fn a_refused_sender_row_warns_at_ingest_and_refuses_at_approve() {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVBN");
    fixture.cache(fixture.signed_sender_row(), RowVerdict::BadSig);
    let cfg = fixture.cfg(true);

    let ack = notes_wire::receive_note(
        &fixture.receiver,
        &cfg,
        &fixture.spooled_offer("01K4CORROBORATE00000000001"),
    )
    .unwrap();
    assert_eq!(ack, notes_wire::NoteAck::Delivered);
    assert!(
        work_store::load(&fixture.receiver, &fixture.envelope.work_id)
            .unwrap()
            .is_some()
    );
    let log = fs::read_to_string(
        fixture
            .receiver
            .root()
            .join("mesh/gateway/gateway.jsonl"),
    )
    .unwrap();
    assert!(log.contains("work_ingest_warning"), "{log}");
    assert!(log.contains("BAD_SIG"), "{log}");

    let err = approve_dry_run(
        &fixture.receiver,
        &FakeGit,
        &FakeSpawner,
        &fixture.envelope.work_id,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_SENDER_ROW_INVALID");
}

#[tokio::test]
async fn a_revoked_sender_is_refused_at_both_ingest_and_approve() {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVDQ");
    fixture.cache(fixture.signed_sender_row(), RowVerdict::ValidSigned);
    let record = keys::mint_revocation(
        &fixture.root,
        &fixture.sender_gw,
        fixture.verified.sender_claims.key_epoch,
        &SystemClock,
    )
    .unwrap();
    let claims = keys::verify_revocation(&record, &RootSet::load(&fixture.receiver).unwrap())
        .unwrap();
    RevocationCache::open(&fixture.receiver)
        .unwrap()
        .observe(&fixture.sender_gw, &claims)
        .unwrap();
    let cfg = fixture.cfg(false);

    let ack = notes_wire::receive_note(
        &fixture.receiver,
        &cfg,
        &fixture.spooled_offer("01K4CORROBORATE00000000002"),
    )
    .unwrap();
    assert_eq!(
        ack,
        notes_wire::NoteAck::Rejected {
            code: "WORK_SENDER_ROW_INVALID".to_string()
        }
    );
    assert!(
        work_store::load(&fixture.receiver, &fixture.envelope.work_id)
            .unwrap()
            .is_none()
    );

    fixture.store_for_approve();
    let err = approve_dry_run(
        &fixture.receiver,
        &FakeGit,
        &FakeSpawner,
        &fixture.envelope.work_id,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_SENDER_ROW_INVALID");
    assert!(err.message.contains("revoked"), "{err:?}");
}

#[tokio::test]
async fn a_valid_signed_sender_row_whose_row_msg_pubkey_fpr_differs_from_the_envelope_key_is_refused_at_approve(
) {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVER");
    let row = fixture.valid_rekeyed_sender_row();
    assert_ne!(
        row.pubkey_fpr,
        keys::fpr_of_pubkey_b64(&fixture.envelope.sender.pubkey_b64).unwrap()
    );
    fixture.cache(row, RowVerdict::ValidSigned);
    fixture.cfg(false);
    fixture.store_for_approve();

    let err = approve_dry_run(
        &fixture.receiver,
        &FakeGit,
        &FakeSpawner,
        &fixture.envelope.work_id,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_SENDER_ROW_INVALID");
    assert!(err.message.contains("pubkey_fpr"), "{err:?}");
}

#[tokio::test]
async fn a_usable_unsigned_sender_row_under_flag_false_proceeds_with_the_fpr_check_skipped_and_warned(
) {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVFS");
    let mut row = fixture.signed_sender_row();
    row.pubkey_fpr = "bare-column-must-not-be-trusted".to_string();
    row.row_msg = None;
    row.row_token = None;
    row.row_sig = None;
    row.signed_at = None;
    fixture.cache(row, RowVerdict::Unsigned { stripped: false });
    fixture.cfg(false);
    fixture.store_for_approve();

    let warning = corroborate_sender(
        &fixture.receiver,
        &fixture.verified,
        CorroborationStage::Approve,
        false,
    )
    .unwrap()
    .expect("usable unsigned row warns");
    assert!(warning.contains("pubkey_fpr check was skipped"), "{warning}");

    approve_dry_run(
        &fixture.receiver,
        &FakeGit,
        &FakeSpawner,
        &fixture.envelope.work_id,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn an_absent_sender_row_follows_the_same_degrade_split() {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVAM");
    let mut other = fixture.signed_sender_row();
    other.gateway_id = "other-cached-gateway".to_string();
    fixture.cache(other, RowVerdict::BadSig);
    let cfg = fixture.cfg(false);

    assert_eq!(
        notes_wire::receive_note(
            &fixture.receiver,
            &cfg,
            &fixture.spooled_offer("01K4CORROBORATE00000000003"),
        )
        .unwrap(),
        notes_wire::NoteAck::Delivered
    );
    let err = approve_dry_run(
        &fixture.receiver,
        &FakeGit,
        &FakeSpawner,
        &fixture.envelope.work_id,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_SENDER_ROW_INVALID");
    assert!(err.message.contains("absent"), "{err:?}");
}

#[test]
fn a_registry_unreachable_at_ingest_warns_and_proceeds() {
    let fixture = Fixture::new("01BX5ZZKBKACTAV9WEVGEMMVCP");
    let cfg = fixture.cfg(true);
    assert!(!fixture.receiver.root().join("mesh/gateway/registry-cache.json").exists());

    assert_eq!(
        notes_wire::receive_note(
            &fixture.receiver,
            &cfg,
            &fixture.spooled_offer("01K4CORROBORATE00000000004"),
        )
        .unwrap(),
        notes_wire::NoteAck::Delivered
    );
    assert!(
        work_store::load(&fixture.receiver, &fixture.envelope.work_id)
            .unwrap()
            .is_some()
    );
    let warning = corroborate_sender(
        &fixture.receiver,
        &fixture.verified,
        CorroborationStage::Ingest,
        true,
    )
    .unwrap()
    .expect("missing registry cache degrades at ingest");
    assert!(warning.contains("registry cache is unavailable"), "{warning}");
}
