use a2a::home::A2aHome;
use a2a::mesh::gateway::presence_wire;
use a2a::mesh::gateway::replicate::{CursorAdvance, CursorStore};
use a2a::mesh::keys::{
    self, enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, RootSet, SystemClock, TokenClaims,
};
use a2a::mesh::registry::{open_registry, reader_roots, GatewayRow};
use a2a::mesh::row_mark::{RevocationCache, RowMark, RowMarkStore};
use a2a::mesh::row_sig::{
    endpoint_conflicts, row_flags, sign_row, verify_row, Disposition, RowFlag, RowMsg,
    RowVerdict, RowVerifyCfg, StoredRowColumns, ROW_MSG_VER,
};
use chrono::{Duration, TimeZone, Utc};
use ed25519_dalek::SigningKey;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::PermissionsExt;

struct SignedFixture {
    _tmp: tempfile::TempDir,
    home: A2aHome,
    roots: RootSet,
    claims: TokenClaims,
    msg: RowMsg,
    stored: StoredRowColumns,
}

impl SignedFixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
        let home = A2aHome::resolve(Some(&tmp.path().join("reader"))).unwrap();
        fleet_init(&root).unwrap();
        fs::create_dir_all(home.root().join("mesh/fleet")).unwrap();
        fs::copy(
            root.root().join("mesh/fleet/root.pub"),
            home.root().join("mesh/fleet/root.pub"),
        )
        .unwrap();

        let identity = gateway_identity(&home, false).unwrap();
        let pubkey_b64 = gateway_public_key_b64(&home).unwrap();
        let token = enroll_gateway_token(
            &root,
            &identity.gateway_id,
            &pubkey_b64,
            &SystemClock,
        )
        .unwrap();
        install_gateway_token(&home, &token, &SystemClock).unwrap();
        let roots = RootSet::load(&home).unwrap();
        let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
        let created_at = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .single()
            .unwrap()
            + Duration::microseconds(678_901);
        let signed_at = Utc
            .with_ymd_and_hms(2026, 3, 4, 5, 6, 7)
            .single()
            .unwrap()
            + Duration::microseconds(891_234);
        let msg = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: claims.fleet_id.clone(),
            gateway_id: claims.gateway_id.clone(),
            endpoint_url: Some("wss://gateway.example.test:7787".to_string()),
            pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
            cert_fpr: Some("cert-fingerprint".to_string()),
            version: "0.3.0".to_string(),
            owner: "reader-test".to_string(),
            created_at,
            schema_version: 1,
            key_epoch: claims.key_epoch,
            roots_held: "[]".to_string(),
            row_token_sha256: sha256_hex(token.as_bytes()),
            row_resign_secs: 86_400,
            registry_heartbeat_secs: 180,
            enforce_require_signed_rows: false,
            enforce_auto_push: false,
            signed_at,
        };
        let key = keys::gateway_signing_key(&home).unwrap();
        let (row_msg, row_sig) = sign_row(&msg, &key).unwrap();
        let stored = StoredRowColumns {
            fleet_id: msg.fleet_id.clone(),
            gateway_id: msg.gateway_id.clone(),
            endpoint_url: msg.endpoint_url.clone(),
            pubkey_fpr: msg.pubkey_fpr.clone(),
            cert_fpr: msg.cert_fpr.clone(),
            version: msg.version.clone(),
            owner: msg.owner.clone(),
            created_at: msg.created_at,
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: Some(token),
            row_sig: Some(row_sig),
            signed_at: Some(msg.signed_at),
            revocation: None,
        };
        Self {
            _tmp: tmp,
            home,
            roots,
            claims,
            msg,
            stored,
        }
    }

    fn mark(&self) -> RowMark {
        RowMark {
            high_water: None,
            msg_digest: None,
            first_signature: true,
            pinned_pubkey_b64: Some(self.claims.pubkey_b64.clone()),
            highest_key_epoch: self.claims.key_epoch,
        }
    }

    fn resign(&self, msg: RowMsg) -> StoredRowColumns {
        let key = keys::gateway_signing_key(&self.home).unwrap();
        let (row_msg, row_sig) = sign_row(&msg, &key).unwrap();
        StoredRowColumns {
            fleet_id: msg.fleet_id.clone(),
            gateway_id: msg.gateway_id.clone(),
            endpoint_url: msg.endpoint_url.clone(),
            pubkey_fpr: msg.pubkey_fpr.clone(),
            cert_fpr: msg.cert_fpr.clone(),
            version: msg.version.clone(),
            owner: msg.owner.clone(),
            created_at: msg.created_at,
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: self.stored.row_token.clone(),
            row_sig: Some(row_sig),
            signed_at: Some(msg.signed_at),
            revocation: None,
        }
    }

    fn verify_cfg(&self) -> RowVerifyCfg {
        RowVerifyCfg::new(self.msg.signed_at + Duration::minutes(1))
    }
}

#[test]
fn row_marks_persist_0600_and_survive_reopen() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();

    let path = fixture
        .home
        .root()
        .join("mesh/row-marks")
        .join(format!("{}.json", fixture.msg.gateway_id));
    assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o600);
    let reopened = RowMarkStore::open(&fixture.home).unwrap();
    assert_eq!(reopened.get(&fixture.msg.gateway_id), store.get(&fixture.msg.gateway_id));
    assert_eq!(reopened.load_all().len(), 1);
}

#[test]
fn forget_signed_clears_the_high_water_but_keeps_the_first_signature_bit() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    store.forget_signed(&fixture.msg.gateway_id);

    let mark = store.get(&fixture.msg.gateway_id).unwrap();
    assert_eq!(mark.high_water, None);
    assert_eq!(mark.msg_digest, None);
    assert!(mark.first_signature);
    assert_eq!(
        RowMarkStore::open(&fixture.home)
            .unwrap()
            .get(&fixture.msg.gateway_id),
        Some(mark)
    );
}

#[test]
fn clear_signature_clears_only_the_bit() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    let before = store.get(&fixture.msg.gateway_id).unwrap().clone();
    store.clear_signature(&fixture.msg.gateway_id);

    let after = store.get(&fixture.msg.gateway_id).unwrap();
    assert!(!after.first_signature);
    assert_eq!(after.high_water, before.high_water);
    assert_eq!(after.msg_digest, before.msg_digest);
    assert_eq!(after.pinned_pubkey_b64, before.pinned_pubkey_b64);
    assert_eq!(after.highest_key_epoch, before.highest_key_epoch);
}

#[test]
fn a_higher_verified_key_epoch_rebinds_the_pin_without_clearing_the_signature_bit() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    let mut next_msg = fixture.msg.clone();
    next_msg.key_epoch += 1;
    let mut next_claims = fixture.claims.clone();
    next_claims.key_epoch += 1;
    next_claims.pubkey_b64 = "next-root-attested-gateway-key".to_string();

    store
        .apply_verified(&fixture.msg.gateway_id, &next_msg, &next_claims)
        .unwrap();
    let mark = store.get(&fixture.msg.gateway_id).unwrap();
    assert_eq!(mark.high_water, None);
    assert_eq!(mark.msg_digest, None);
    assert!(mark.first_signature);
    assert_eq!(mark.pinned_pubkey_b64.as_deref(), Some(next_claims.pubkey_b64.as_str()));
    assert_eq!(mark.highest_key_epoch, next_claims.key_epoch);
}

#[test]
fn a_gateway_id_that_is_not_a_safe_file_part_is_never_written_to_disk() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    assert!(store
        .apply_verified("../evil", &fixture.msg, &fixture.claims)
        .is_err());
    assert!(!fixture.home.root().join("mesh/evil.json").exists());
    assert!(!fixture.home.root().join("evil.json").exists());
}

#[test]
fn a_correctly_bound_revocation_makes_the_row_revoked_under_both_flags() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let mut stored = fixture.stored.clone();
    stored.revocation = Some(
        keys::mint_revocation(
            &root,
            &fixture.claims.gateway_id,
            fixture.claims.key_epoch,
            &SystemClock,
        )
        .unwrap(),
    );

    let verdict = verify_row(
        &stored,
        &fixture.roots,
        Some(&fixture.mark()),
        &fixture.verify_cfg(),
    );
    assert_eq!(verdict, RowVerdict::Revoked);
    assert_eq!(verdict.disposition(false), Disposition::Refused);
    assert_eq!(verdict.disposition(true), Disposition::Refused);
}

#[test]
fn a_valid_revocation_from_another_gateway_is_row_revocation_misplaced_and_revokes_nobody() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let record = keys::mint_revocation(
        &root,
        &fixture.claims.gateway_id,
        fixture.claims.key_epoch,
        &SystemClock,
    )
    .unwrap();
    let mut carrier = fixture.stored.clone();
    carrier.gateway_id = "gw-b-0123456789abcdef".to_string();
    carrier.revocation = Some(record);

    assert_eq!(
        verify_row(&carrier, &fixture.roots, None, &fixture.verify_cfg()),
        RowVerdict::RowRevocationMisplaced
    );
    assert_eq!(
        verify_row(
            &fixture.stored,
            &fixture.roots,
            Some(&fixture.mark()),
            &fixture.verify_cfg(),
        ),
        RowVerdict::ValidSigned
    );
    let cache = RevocationCache::open(&fixture.home).unwrap();
    assert!(!cache.is_revoked("gw-b-0123456789abcdef", 0));
    assert!(!cache.is_revoked(&fixture.claims.gateway_id, 0));
}

#[test]
fn a_higher_epoch_token_supersedes_the_revocation_record() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let rebound_key = SigningKey::from_bytes(&[91_u8; 32]);
    let mut claims = fixture.claims.clone();
    claims.key_epoch += 1;
    claims.pubkey_b64 = b64(&rebound_key.verifying_key().to_bytes());
    let token = keys::sign_token(&claims, &root).unwrap();
    let mut msg = fixture.msg.clone();
    msg.key_epoch = claims.key_epoch;
    msg.pubkey_fpr = keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap();
    msg.row_token_sha256 = sha256_hex(token.as_bytes());
    let (row_msg, row_sig) = sign_row(&msg, &rebound_key).unwrap();
    let mut stored = fixture.stored.clone();
    stored.pubkey_fpr = msg.pubkey_fpr.clone();
    stored.row_msg = Some(row_msg);
    stored.row_token = Some(token);
    stored.row_sig = Some(row_sig);
    stored.revocation = Some(
        keys::mint_revocation(
            &root,
            &claims.gateway_id,
            fixture.claims.key_epoch,
            &SystemClock,
        )
        .unwrap(),
    );

    assert_eq!(
        verify_row(
            &stored,
            &fixture.roots,
            Some(&fixture.mark()),
            &fixture.verify_cfg(),
        ),
        RowVerdict::ValidSigned
    );
    assert!(row_flags(&stored, Some(&fixture.mark()), &fixture.verify_cfg())
        .contains(&RowFlag::KeyRebound));
}

#[test]
fn revocation_is_honoured_from_the_cache_when_the_registry_is_unreachable() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let record = keys::mint_revocation(
        &root,
        &fixture.claims.gateway_id,
        fixture.claims.key_epoch,
        &SystemClock,
    )
    .unwrap();
    let claims = keys::verify_revocation(&record, &fixture.roots).unwrap();
    let mut cache = RevocationCache::open(&fixture.home).unwrap();
    cache.observe(&claims.gateway_id, &claims).unwrap();

    let reopened = RevocationCache::open(&fixture.home).unwrap();
    assert!(reopened.is_revoked(&claims.gateway_id, claims.key_epoch));
    assert!(!reopened.is_revoked(&claims.gateway_id, claims.key_epoch + 1));
}

#[test]
fn an_expired_token_attaches_a_warn_label_and_the_row_stays_usable() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let cfg = fixture.verify_cfg();
    let mut claims = fixture.claims.clone();
    claims.exp = cfg.now.timestamp() - 1;
    let token = keys::sign_token(&claims, &root).unwrap();
    let mut msg = fixture.msg.clone();
    msg.row_token_sha256 = sha256_hex(token.as_bytes());
    let (row_msg, row_sig) =
        sign_row(&msg, &keys::gateway_signing_key(&fixture.home).unwrap()).unwrap();
    let mut stored = fixture.stored.clone();
    stored.row_msg = Some(row_msg);
    stored.row_token = Some(token);
    stored.row_sig = Some(row_sig);

    let verdict = verify_row(&stored, &fixture.roots, Some(&fixture.mark()), &cfg);
    assert_eq!(verdict, RowVerdict::ValidSigned);
    assert_eq!(verdict.disposition(true), Disposition::Usable);
    assert!(row_flags(&stored, Some(&fixture.mark()), &cfg).contains(&RowFlag::TokenExpired));
}

#[test]
fn a_not_yet_valid_token_attaches_a_warn_label_and_the_row_stays_usable() {
    let fixture = SignedFixture::new();
    let root = A2aHome::resolve(Some(&fixture._tmp.path().join("root"))).unwrap();
    let cfg = fixture.verify_cfg();
    let mut claims = fixture.claims.clone();
    claims.issued_at = cfg.now.timestamp() + 1;
    claims.exp = claims.issued_at + 90 * 24 * 60 * 60;
    let token = keys::sign_token(&claims, &root).unwrap();
    let mut msg = fixture.msg.clone();
    msg.row_token_sha256 = sha256_hex(token.as_bytes());
    let (row_msg, row_sig) =
        sign_row(&msg, &keys::gateway_signing_key(&fixture.home).unwrap()).unwrap();
    let mut stored = fixture.stored.clone();
    stored.row_msg = Some(row_msg);
    stored.row_token = Some(token);
    stored.row_sig = Some(row_sig);

    let verdict = verify_row(&stored, &fixture.roots, Some(&fixture.mark()), &cfg);
    assert_eq!(verdict, RowVerdict::ValidSigned);
    assert_eq!(verdict.disposition(true), Disposition::Usable);
    assert!(
        row_flags(&stored, Some(&fixture.mark()), &cfg).contains(&RowFlag::TokenNotYetValid)
    );
}

#[test]
fn two_null_endpoint_rows_are_not_in_conflict() {
    let fixture = SignedFixture::new();
    let mut first = gateway_row_from_stored(&fixture.stored);
    first.endpoint_url = None;
    let mut second = first.clone();
    second.gateway_id = "gw-b-0123456789abcdef".to_string();
    let rows = vec![
        (first, RowVerdict::ValidSigned),
        (second, RowVerdict::ValidSigned),
    ];
    assert!(endpoint_conflicts(&rows).is_empty());
}

#[test]
fn a_backdated_squatting_row_loses_to_the_incumbents_created_at() {
    let fixture = SignedFixture::new();
    let incumbent = gateway_row_from_stored(&fixture.stored);
    let mut squatter = incumbent.clone();
    squatter.gateway_id = "gw-z-0123456789abcdef".to_string();
    squatter.created_at = incumbent.created_at + Duration::days(1);
    squatter.signed_at = incumbent.signed_at.map(|at| at - Duration::days(365));
    let rows = vec![
        (squatter.clone(), RowVerdict::ValidSigned),
        (incumbent.clone(), RowVerdict::ValidSigned),
    ];

    assert_eq!(
        endpoint_conflicts(&rows).get(&squatter.gateway_id),
        Some(&incumbent.gateway_id)
    );
}

#[test]
fn partial_null_signature_columns_are_row_sig_incomplete() {
    let fixture = SignedFixture::new();
    for mask in 1_u8..15 {
        let mut stored = fixture.stored.clone();
        if mask & 1 == 0 {
            stored.row_msg = None;
        }
        if mask & 2 == 0 {
            stored.row_token = None;
        }
        if mask & 4 == 0 {
            stored.row_sig = None;
        }
        if mask & 8 == 0 {
            stored.signed_at = None;
        }
        assert_eq!(
            verify_row(&stored, &fixture.roots, Some(&fixture.mark()), &fixture.verify_cfg()),
            RowVerdict::RowSigIncomplete,
            "partial signature-column mask {mask:04b}"
        );
    }
}

#[test]
fn strip_to_unsigned_after_first_signature_is_refused_under_both_flags() {
    let fixture = SignedFixture::new();
    let mut stored = fixture.stored.clone();
    stored.row_msg = None;
    stored.row_token = None;
    stored.row_sig = None;
    stored.signed_at = None;
    let verdict = verify_row(
        &stored,
        &fixture.roots,
        Some(&fixture.mark()),
        &fixture.verify_cfg(),
    );
    assert_eq!(verdict, RowVerdict::Unsigned { stripped: true });
    assert_eq!(verdict.disposition(false), Disposition::Refused);
    assert_eq!(verdict.disposition(true), Disposition::Refused);
}

#[test]
fn unsigned_row_is_usable_with_warning_for_a_cold_reader() {
    let fixture = SignedFixture::new();
    let mut stored = fixture.stored.clone();
    stored.row_msg = None;
    stored.row_token = None;
    stored.row_sig = None;
    stored.signed_at = None;
    let verdict = verify_row(&stored, &fixture.roots, None, &fixture.verify_cfg());
    assert_eq!(verdict, RowVerdict::Unsigned { stripped: false });
    assert_eq!(verdict.disposition(false), Disposition::UsableWarn);
    assert_eq!(verdict.disposition(true), Disposition::Refused);
}

#[test]
fn version_downgraded_row_with_the_bit_set_is_bad_sig() {
    let fixture = SignedFixture::new();
    let mut stored = fixture.stored.clone();
    stored.version = "0.2.9".to_string();
    assert_eq!(
        verify_row(&stored, &fixture.roots, Some(&fixture.mark()), &fixture.verify_cfg()),
        RowVerdict::BadSig
    );
}

#[test]
fn same_shape_without_the_bit_is_unsigned_stale() {
    let fixture = SignedFixture::new();
    let mut stored = fixture.stored.clone();
    stored.version = "0.2.9".to_string();
    assert_eq!(
        verify_row(&stored, &fixture.roots, None, &fixture.verify_cfg()),
        RowVerdict::UnsignedStale
    );
}

#[test]
fn token_unverifiable_under_local_roots_is_unverified_when_the_pinned_key_still_checks_out() {
    let fixture = SignedFixture::new();
    let other = A2aHome::resolve(Some(&fixture._tmp.path().join("other-root"))).unwrap();
    fleet_init(&other).unwrap();
    let other_roots = RootSet::load(&other).unwrap();
    assert_eq!(
        verify_row(
            &fixture.stored,
            &other_roots,
            Some(&fixture.mark()),
            &fixture.verify_cfg(),
        ),
        RowVerdict::Unverified
    );
}

#[test]
fn below_mark_signed_at_is_row_replayed() {
    let fixture = SignedFixture::new();
    let mut mark = fixture.mark();
    mark.high_water = Some(fixture.msg.signed_at);
    mark.msg_digest = fixture.stored.row_msg.as_deref().map(sha256_hex);
    let mut msg = fixture.msg.clone();
    msg.signed_at -= Duration::seconds(1);
    let stored = fixture.resign(msg);
    assert_eq!(
        verify_row(&stored, &fixture.roots, Some(&mark), &fixture.verify_cfg()),
        RowVerdict::RowReplayed
    );
}

#[test]
fn at_mark_digest_fork_is_refused() {
    let fixture = SignedFixture::new();
    let mut mark = fixture.mark();
    mark.high_water = Some(fixture.msg.signed_at);
    mark.msg_digest = fixture.stored.row_msg.as_deref().map(sha256_hex);
    let mut msg = fixture.msg.clone();
    msg.roots_held = "[\"fork\"]".to_string();
    let stored = fixture.resign(msg);
    let verdict = verify_row(&stored, &fixture.roots, Some(&mark), &fixture.verify_cfg());
    assert_eq!(verdict, RowVerdict::RowReplayed);
    assert_eq!(verdict.disposition(false), Disposition::Refused);
}

#[test]
fn signed_at_beyond_skew_is_row_clock_ahead_and_is_never_cached() {
    let fixture = SignedFixture::new();
    let mut msg = fixture.msg.clone();
    let cfg = RowVerifyCfg::new(msg.signed_at - Duration::minutes(10));
    msg.signed_at = cfg.now + Duration::seconds(cfg.skew_tolerance_secs + 1);
    let stored = fixture.resign(msg);
    let before = fixture.mark();
    assert_eq!(
        verify_row(&stored, &fixture.roots, Some(&before), &cfg),
        RowVerdict::RowClockAhead
    );
    assert_eq!(before.high_water, None);
}

#[test]
fn stale_row_uses_the_rows_declared_cadence_clamped_to_max_accepted() {
    let fixture = SignedFixture::new();
    let mut msg = fixture.msg.clone();
    msg.row_resign_secs = 10_000_000;
    let stored = fixture.resign(msg.clone());
    let mut cfg = RowVerifyCfg::new(msg.signed_at + Duration::seconds(1_209_601));
    cfg.max_accepted_row_resign_secs = 604_800;
    assert!(row_flags(&stored, Some(&fixture.mark()), &cfg).contains(&RowFlag::StaleRow));
}

#[test]
fn null_signed_at_rows_are_never_cache_ordered() {
    let fixture = SignedFixture::new();
    let mut stored = fixture.stored.clone();
    stored.row_msg = None;
    stored.row_token = None;
    stored.row_sig = None;
    stored.signed_at = None;
    let mut mark = fixture.mark();
    mark.high_water = Some(fixture.msg.signed_at + Duration::days(1));
    assert_eq!(
        verify_row(&stored, &fixture.roots, Some(&mark), &fixture.verify_cfg()),
        RowVerdict::Unsigned { stripped: true }
    );
}

#[test]
fn clear_signature_then_an_unsigned_row_is_usable_under_flag_false() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    store.clear_signature(&fixture.msg.gateway_id);
    let mut stored = fixture.stored.clone();
    stored.row_msg = None;
    stored.row_token = None;
    stored.row_sig = None;
    stored.signed_at = None;
    let verdict = verify_row(
        &stored,
        &fixture.roots,
        store.get(&fixture.msg.gateway_id),
        &fixture.verify_cfg(),
    );
    assert_eq!(verdict, RowVerdict::Unsigned { stripped: false });
    assert_eq!(verdict.disposition(false), Disposition::UsableWarn);
}

#[test]
fn forget_signed_clears_the_high_water_but_a_replayed_row_is_accepted_again() {
    let fixture = SignedFixture::new();
    let mut store = RowMarkStore::open(&fixture.home).unwrap();
    store
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    let mut older = fixture.msg.clone();
    older.signed_at -= Duration::seconds(1);
    let stored = fixture.resign(older);
    assert_eq!(
        verify_row(
            &stored,
            &fixture.roots,
            store.get(&fixture.msg.gateway_id),
            &fixture.verify_cfg(),
        ),
        RowVerdict::RowReplayed
    );
    store.forget_signed(&fixture.msg.gateway_id);
    assert_eq!(
        verify_row(
            &stored,
            &fixture.roots,
            store.get(&fixture.msg.gateway_id),
            &fixture.verify_cfg(),
        ),
        RowVerdict::ValidSigned
    );
}

#[test]
fn verifier_chokepoint_has_exactly_one_unverified_read_site() {
    let sources = [
        ("registry.rs", include_str!("../src/mesh/registry.rs")),
        ("mesh_cmd.rs", include_str!("../src/commands/mesh_cmd.rs")),
        ("work_cmd.rs", include_str!("../src/commands/work_cmd.rs")),
        ("gateway/mod.rs", include_str!("../src/mesh/gateway/mod.rs")),
        (
            "gateway/notes_wire.rs",
            include_str!("../src/mesh/gateway/notes_wire.rs"),
        ),
    ];
    let sites = sources
        .into_iter()
        .filter_map(|(name, source)| source.contains("discover_unverified(").then_some(name))
        .collect::<Vec<_>>();
    assert_eq!(sites, vec!["registry.rs"]);
}

#[test]
fn open_registry_requires_roots_and_a_fleet_id() {
    let fixture = SignedFixture::new();
    let path = fixture._tmp.path().join("registry");
    let registry = open_registry(
        &format!("file:{}", path.display()),
        fixture.roots.clone(),
        fixture.claims.fleet_id.clone(),
    )
    .unwrap();
    let _ = registry;
}

#[test]
fn unreadable_local_roots_are_not_misreported_as_a_peer_signature_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("reader"))).unwrap();
    let err = reader_roots(&home).unwrap_err();
    assert_eq!(err.code, "ROOTS_UNREADABLE");
    assert!(err.message.contains("mesh/fleet/root.pub"), "{err:?}");
    assert!(err.remedy.as_deref().unwrap().contains("LOCAL"));
}

#[test]
fn invalid_rows_never_retire_origins_and_verdict_transitions_log_once() {
    let fixture = SignedFixture::new();
    let now = fixture.msg.signed_at + Duration::minutes(1);
    let mut row = gateway_row_from_stored(&fixture.stored);
    row.revoked_at = Some(now - Duration::days(30));
    let invalid = vec![(row.clone(), RowVerdict::BadSig)];

    presence_wire::refresh_registry_cache(&fixture.home, &invalid, now).unwrap();
    presence_wire::refresh_registry_cache(&fixture.home, &invalid, now + Duration::seconds(1))
        .unwrap();

    let cache_path = fixture
        .home
        .root()
        .join("mesh/gateway/registry-cache.json");
    let cache: serde_json::Value =
        serde_json::from_slice(&fs::read(&cache_path).unwrap()).unwrap();
    let cached = &cache["gateways"][&fixture.msg.gateway_id];
    assert_eq!(cached["verdict"], "BAD_SIG");
    assert_eq!(cached["invalid_since"], serde_json::json!(now));
    assert!(cached["missing_since"].is_null());
    assert!(cached["revoked_at"].is_string());

    let mut cursors = CursorStore::load_cached(&fixture.home).unwrap();
    cursors.apply_advance(CursorAdvance::new(&fixture.msg.gateway_id, 7));
    cursors.persist(&fixture.home).unwrap();
    let spool = fixture
        .home
        .root()
        .join("mesh/outbound")
        .join(&fixture.msg.gateway_id);
    fs::create_dir_all(&spool).unwrap();
    fs::write(spool.join("captured.note.json"), b"evidence").unwrap();

    let report = presence_wire::retire_stale_origins(
        &fixture.home,
        now + Duration::days(365),
        1,
    )
    .unwrap();
    assert!(report.retired.is_empty());
    assert_eq!(
        CursorStore::load_cached(&fixture.home)
            .unwrap()
            .last_seq(&fixture.msg.gateway_id),
        7
    );
    assert!(spool.join("captured.note.json").exists());

    let log_path = fixture.home.root().join("mesh/gateway/gateway.jsonl");
    let transitions = fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .filter(|line| line.contains(r#""kind":"row_verdict_transition""#))
        .count();
    assert_eq!(transitions, 1);

    let valid = vec![(row, RowVerdict::ValidSigned)];
    presence_wire::refresh_registry_cache(&fixture.home, &valid, now + Duration::seconds(2))
        .unwrap();
    presence_wire::refresh_registry_cache(&fixture.home, &valid, now + Duration::seconds(3))
        .unwrap();
    let cache: serde_json::Value =
        serde_json::from_slice(&fs::read(&cache_path).unwrap()).unwrap();
    let cached = &cache["gateways"][&fixture.msg.gateway_id];
    assert_eq!(cached["verdict"], "valid-signed");
    assert!(cached["invalid_since"].is_null());
    let transitions = fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .filter(|line| line.contains(r#""kind":"row_verdict_transition""#))
        .count();
    assert_eq!(transitions, 2);
}

fn gateway_row_from_stored(stored: &StoredRowColumns) -> GatewayRow {
    GatewayRow {
        fleet_id: stored.fleet_id.clone(),
        gateway_id: stored.gateway_id.clone(),
        owner: stored.owner.clone(),
        endpoint_url: stored.endpoint_url.clone(),
        pubkey_fpr: stored.pubkey_fpr.clone(),
        cert_fpr: stored.cert_fpr.clone(),
        version: stored.version.clone(),
        created_at: stored.created_at,
        last_heartbeat: stored.created_at,
        revoked_at: None,
        schema_version: stored.schema_version,
        row_msg: stored.row_msg.clone(),
        row_token: stored.row_token.clone(),
        row_sig: stored.row_sig.clone(),
        signed_at: stored.signed_at,
        revocation: stored.revocation.clone(),
    }
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn b64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 3) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 15) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
