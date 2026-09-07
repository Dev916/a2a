use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, enroll_gateway_token_with, ensure_gateway_token, fleet_init, gateway_identity,
    fleet_root_fingerprint, fpr_of_pubkey_b64, gateway_public_key_b64,
    gateway_public_key_fingerprint, install_gateway_token, mint_ulid, sign_token,
    stage_root, unverified_claims, verify_announcement, verify_token, verify_token_no_clock,
    verify_token_with_issuer, Clock, RootRole, RootSet, RootSigner, TestClock, TokenClaims,
    SKEW_TOLERANCE_SECS,
};
use chrono::{TimeZone, Utc};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn home_in(tmp: &tempfile::TempDir) -> A2aHome {
    A2aHome::resolve(Some(tmp.path())).unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn claims(exp: i64, issued_at: i64) -> TokenClaims {
    TokenClaims {
        fleet_id: "fleet-1234abcd".to_string(),
        gateway_id: "gw-testhost-abcdef".to_string(),
        pubkey_b64: "AQIDBA==".to_string(),
        key_epoch: 0,
        issued_at,
        exp,
    }
}

#[test]
fn fleet_and_gateway_material_have_stable_ids_modes_and_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);

    let fleet = fleet_init(&home).unwrap();
    assert!(
        fleet.fleet_id.starts_with("fleet-") && fleet.fleet_id.len() == "fleet-12345678".len(),
        "{}",
        fleet.fleet_id
    );
    assert!(fleet.fleet_id["fleet-".len()..]
        .chars()
        .all(|c| c.is_ascii_hexdigit()));

    let gw = gateway_identity(&home, false).unwrap();
    let again = gateway_identity(&home, false).unwrap();
    assert_eq!(gw.gateway_id, again.gateway_id);
    assert_eq!(gw.cert_fpr, again.cert_fpr);
    assert!(gw.gateway_id.starts_with("gw-"), "{}", gw.gateway_id);
    assert_eq!(gw.cert_fpr.len(), 64);
    assert!(gw
        .cert_fpr
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    assert_eq!(gw.cert_fpr(), gw.cert_fpr);

    let fresh = gateway_identity(&home, true).unwrap();
    assert_ne!(fresh.gateway_id, gw.gateway_id);

    let mesh = tmp.path().join("mesh");
    assert_eq!(mode(&mesh.join("fleet/root.key")), 0o600);
    assert_eq!(mode(&mesh.join("fleet/root.pub")), 0o644);
    assert_eq!(mode(&mesh.join("gateway/gw.key")), 0o600);
    assert_eq!(mode(&mesh.join("gateway/gw.tls.key")), 0o600);
}

#[test]
fn sign_verify_roundtrip_accepts_the_current_root() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    let root = fleet_init(&home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = sign_token(&claims(1_700_001_000, 1_699_999_000), &home).unwrap();
    let verified = verify_token(&token, &RootSet::load(&home).unwrap(), &clock).unwrap();
    assert_eq!(verified.gateway_id, "gw-testhost-abcdef");
    assert_eq!(verified.fleet_id, root.fleet_id);

}

#[test]
fn an_announcement_verifies_under_the_current_root_and_is_never_a_token() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let staged = stage_root(&home, &clock, false).unwrap();
    let roots = RootSet::load(&home).unwrap();

    let (announcement, issuer) = verify_announcement(&staged.announcement, &roots).unwrap();
    assert_eq!(announcement.fleet_id, staged.fleet_id);
    assert_eq!(announcement.next_fpr, staged.next_fingerprint);
    assert_eq!(issuer, staged.current_fingerprint);
    assert_eq!(roots.role_of(&issuer), Some(RootRole::Current));

    let err = verify_token(&staged.announcement, &roots, &clock).unwrap_err();
    assert_eq!(err.code, "TOKEN_CLAIMS_INVALID");

    let real_token = sign_token(&claims(1_700_001_000, 1_699_999_000), &home).unwrap();
    let err = verify_announcement(&real_token, &roots).unwrap_err();
    assert_eq!(err.code, "ROOT_ANNOUNCEMENT_INVALID");
}

#[test]
fn root_set_refuses_a_next_root_from_another_fleet() {
    let first_tmp = tempfile::tempdir().unwrap();
    let first_home = home_in(&first_tmp);
    let first = fleet_init(&first_home).unwrap();
    let second_tmp = tempfile::tempdir().unwrap();
    let second_home = home_in(&second_tmp);
    let second = fleet_init(&second_home).unwrap();
    fs::copy(
        second_tmp.path().join("mesh/fleet/root.pub"),
        first_tmp.path().join("mesh/fleet/root.pub.next"),
    )
    .unwrap();

    let err = RootSet::load(&first_home).unwrap_err();
    assert_eq!(err.code, "ROOT_NEXT_FLEET_MISMATCH");
    assert!(err.message.contains(&first.fleet_id), "{}", err.message);
    assert!(err.message.contains(&second.fleet_id), "{}", err.message);
}

#[test]
fn verify_token_with_issuer_names_the_root_that_verified() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let identity = gateway_identity(&home, false).unwrap();
    let pubkey = gateway_public_key_b64(&home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    stage_root(&home, &clock, false).unwrap();
    let current_token = enroll_gateway_token(&home, &identity.gateway_id, &pubkey, &clock).unwrap();
    let (next_token, next_issuer) = enroll_gateway_token_with(
        &home,
        &identity.gateway_id,
        &pubkey,
        RootSigner::Next,
        &clock,
    )
    .unwrap();
    let roots = RootSet::load(&home).unwrap();
    let (_claims, current_issuer) =
        verify_token_with_issuer(&current_token, &roots, &clock).unwrap();
    let (_claims, verified_next_issuer) =
        verify_token_with_issuer(&next_token, &roots, &clock).unwrap();
    assert_eq!(roots.role_of(&current_issuer), Some(RootRole::Current));
    assert_eq!(verified_next_issuer, next_issuer);
    assert_eq!(roots.role_of(&verified_next_issuer), Some(RootRole::Next));
}

#[test]
fn self_enrollment_token_is_minted_once_from_existing_fleet_root() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    let root = fleet_init(&home).unwrap();
    let gateway = gateway_identity(&home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

    assert!(ensure_gateway_token(&home, &clock).unwrap());
    let token_path = tmp.path().join("mesh/gateway/gw.token");
    assert_eq!(mode(&token_path), 0o600);
    let token = fs::read_to_string(&token_path).unwrap();
    let verified = verify_token(token.trim(), &RootSet::load(&home).unwrap(), &clock).unwrap();
    assert_eq!(verified.fleet_id, root.fleet_id);
    assert_eq!(verified.gateway_id, gateway.gateway_id);
    assert_eq!(verified.exp - verified.issued_at, 90 * 24 * 60 * 60);
    assert_eq!(gateway_public_key_fingerprint(&home).unwrap().len(), 64);

    let before = fs::read_to_string(&token_path).unwrap();
    assert!(!ensure_gateway_token(&home, &clock).unwrap());
    assert_eq!(fs::read_to_string(&token_path).unwrap(), before);
}

#[test]
fn self_enrollment_without_root_key_returns_gateway_not_enrolled() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    gateway_identity(&home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

    let err = ensure_gateway_token(&home, &clock).unwrap_err();
    assert_eq!(err.code, "GATEWAY_NOT_ENROLLED");
    assert!(err.remedy.unwrap().contains("a2a mesh fleet init"));
}

#[test]
fn remote_enrollment_token_round_trips_with_root_pub_on_joiner() {
    let root_tmp = tempfile::tempdir().unwrap();
    let root_home = home_in(&root_tmp);
    let join_tmp = tempfile::tempdir().unwrap();
    let join_home = home_in(&join_tmp);
    let fleet = fleet_init(&root_home).unwrap();
    let identity = gateway_identity(&join_home, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(&join_home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

    fs::create_dir_all(join_tmp.path().join("mesh/fleet")).unwrap();
    fs::copy(
        root_tmp.path().join("mesh/fleet/root.pub"),
        join_tmp.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();

    let token =
        enroll_gateway_token(&root_home, &identity.gateway_id, &pubkey_b64, &clock).unwrap();
    let claims = install_gateway_token(&join_home, &token, &clock).unwrap();

    assert_eq!(claims.fleet_id, fleet.fleet_id);
    assert_eq!(claims.gateway_id, identity.gateway_id);
    assert_eq!(claims.pubkey_b64, pubkey_b64);
    assert_eq!(claims.exp - claims.issued_at, 90 * 24 * 60 * 60);
    let token_path = join_tmp.path().join("mesh/gateway/gw.token");
    assert_eq!(mode(&token_path), 0o600);
    assert_eq!(fs::read_to_string(&token_path).unwrap().trim(), token);
    let verified = verify_token(&token, &RootSet::load(&join_home).unwrap(), &clock).unwrap();
    assert_eq!(verified.gateway_id, identity.gateway_id);
}

#[test]
fn remote_install_rejects_pubkey_mismatch() {
    let root_tmp = tempfile::tempdir().unwrap();
    let root_home = home_in(&root_tmp);
    let join_tmp = tempfile::tempdir().unwrap();
    let join_home = home_in(&join_tmp);
    let wrong_tmp = tempfile::tempdir().unwrap();
    let wrong_home = home_in(&wrong_tmp);
    fleet_init(&root_home).unwrap();
    let identity = gateway_identity(&join_home, false).unwrap();
    gateway_identity(&wrong_home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

    fs::create_dir_all(join_tmp.path().join("mesh/fleet")).unwrap();
    fs::copy(
        root_tmp.path().join("mesh/fleet/root.pub"),
        join_tmp.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();

    let token = enroll_gateway_token(
        &root_home,
        &identity.gateway_id,
        &gateway_public_key_b64(&wrong_home).unwrap(),
        &clock,
    )
    .unwrap();
    let err = install_gateway_token(&join_home, &token, &clock).unwrap_err();
    assert_eq!(err.code, "TOKEN_MISMATCH");
    assert!(err.message.contains("pubkey_b64"), "{}", err.message);
}

#[test]
fn remote_install_requires_root_pub_before_writing_token() {
    let root_tmp = tempfile::tempdir().unwrap();
    let root_home = home_in(&root_tmp);
    let join_tmp = tempfile::tempdir().unwrap();
    let join_home = home_in(&join_tmp);
    fleet_init(&root_home).unwrap();
    let identity = gateway_identity(&join_home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = enroll_gateway_token(
        &root_home,
        &identity.gateway_id,
        &gateway_public_key_b64(&join_home).unwrap(),
        &clock,
    )
    .unwrap();

    let err = install_gateway_token(&join_home, &token, &clock).unwrap_err();
    assert_eq!(err.code, "TOKEN_UNVERIFIABLE");
    assert!(err.remedy.unwrap().contains("mesh/fleet/root.pub"));
    assert!(!join_tmp.path().join("mesh/gateway/gw.token").exists());
}

#[test]
fn remote_install_rejects_expired_tokens() {
    let root_tmp = tempfile::tempdir().unwrap();
    let root_home = home_in(&root_tmp);
    let join_tmp = tempfile::tempdir().unwrap();
    let join_home = home_in(&join_tmp);
    let fleet = fleet_init(&root_home).unwrap();
    let identity = gateway_identity(&join_home, false).unwrap();
    let old_now = 1_700_000_000;
    let old_claims = TokenClaims {
        fleet_id: fleet.fleet_id,
        gateway_id: identity.gateway_id,
        pubkey_b64: gateway_public_key_b64(&join_home).unwrap(),
        key_epoch: 0,
        issued_at: old_now,
        exp: old_now + 10,
    };
    let token = sign_token(&old_claims, &root_home).unwrap();

    fs::create_dir_all(join_tmp.path().join("mesh/fleet")).unwrap();
    fs::copy(
        root_tmp.path().join("mesh/fleet/root.pub"),
        join_tmp.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();

    let clock = TestClock::new(Utc.timestamp_opt(old_now + 1_000, 0).unwrap());
    let err = install_gateway_token(&join_home, &token, &clock).unwrap_err();
    assert_eq!(err.code, "TOKEN_EXPIRED");
}

#[test]
fn remote_enroll_rejects_invalid_gateway_public_key() {
    let root_tmp = tempfile::tempdir().unwrap();
    let root_home = home_in(&root_tmp);
    fleet_init(&root_home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());

    let err =
        enroll_gateway_token(&root_home, "gw-joiner-abcdef", "not-a-key", &clock).unwrap_err();
    assert_eq!(err.code, "INVALID_PUBKEY");
}

#[test]
fn clock_skew_boundaries_are_injected_and_expired_error_names_both_timestamps() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let roots = RootSet::load(&home).unwrap();

    let now = 1_700_000_000;
    let token = sign_token(&claims(now + 10, now - 10), &home).unwrap();

    let ahead_ok = TestClock::new(
        Utc.timestamp_opt(now + 10 + SKEW_TOLERANCE_SECS - 1, 0)
            .unwrap(),
    );
    verify_token(&token, &roots, &ahead_ok).unwrap();
    let behind_ok = TestClock::new(
        Utc.timestamp_opt(now - 10 - SKEW_TOLERANCE_SECS + 1, 0)
            .unwrap(),
    );
    verify_token(&token, &roots, &behind_ok).unwrap();

    let ahead_bad = TestClock::new(
        Utc.timestamp_opt(now + 10 + SKEW_TOLERANCE_SECS + 1, 0)
            .unwrap(),
    );
    let err = verify_token(&token, &roots, &ahead_bad).unwrap_err();
    assert_eq!(err.code, "TOKEN_EXPIRED");
    assert!(
        err.message.contains(&(now + 10).to_string()),
        "{}",
        err.message
    );
    assert!(
        err.message
            .contains(&(now + 10 + SKEW_TOLERANCE_SECS + 1).to_string()),
        "{}",
        err.message
    );

    let future_token = sign_token(&claims(now + 10, now + 10), &home).unwrap();
    let behind_bad = TestClock::new(
        Utc.timestamp_opt(now + 10 - SKEW_TOLERANCE_SECS - 1, 0)
            .unwrap(),
    );
    let err = verify_token(&future_token, &roots, &behind_bad).unwrap_err();
    assert_eq!(err.code, "TOKEN_NOT_YET_VALID");
}

#[test]
fn test_clock_advances_and_ulids_follow_it() {
    let clock = TestClock::new(Utc.timestamp_millis_opt(1_700_000_000_123).unwrap());
    let a = mint_ulid(&clock);
    clock.advance(chrono::Duration::milliseconds(1));
    let b = mint_ulid(&clock);
    assert!(a < b, "{a} !< {b}");
}

#[test]
fn forced_errors_do_not_leak_private_key_material() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    gateway_identity(&home, false).unwrap();

    let secret = "PRIVATE-KEY-MATERIAL-SENTINEL";
    fs::write(tmp.path().join("mesh/fleet/root.key"), secret).unwrap();
    fs::write(tmp.path().join("mesh/gateway/gw.key"), secret).unwrap();

    let mut corpus = Vec::new();
    corpus.push(format!(
        "{:?}",
        sign_token(&claims(1_700_001_000, 1_699_999_000), &home).unwrap_err()
    ));
    corpus.push(format!("{:?}", gateway_identity(&home, false).unwrap_err()));
    corpus.push(format!(
        "{:?}",
        TokenClaims {
            pubkey_b64: "public".to_string(),
            ..claims(1, 0)
        }
    ));

    for entry in corpus {
        assert!(!entry.contains(secret), "{entry}");
    }
}

#[test]
fn pre_l4_token_without_key_epoch_verifies_as_epoch_zero_and_stays_canonical() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let gateway = gateway_identity(&home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = enroll_gateway_token(
        &home,
        &gateway.gateway_id,
        &gateway_public_key_b64(&home).unwrap(),
        &clock,
    )
    .unwrap();

    let claims = verify_token(&token, &RootSet::load(&home).unwrap(), &clock).unwrap();
    assert_eq!(claims.key_epoch, 0);
    assert_eq!(unverified_claims(&token).unwrap().key_epoch, 0);
    assert_eq!(
        fpr_of_pubkey_b64(&gateway_public_key_b64(&home).unwrap()).unwrap(),
        gateway_public_key_fingerprint(&home).unwrap()
    );
    assert!(verify_token(&token, &RootSet::load(&home).unwrap(), &clock).is_ok());
}

#[test]
fn verify_token_no_clock_accepts_an_expired_token_and_names_the_issuing_root() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let gateway = gateway_identity(&home, false).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = enroll_gateway_token(
        &home,
        &gateway.gateway_id,
        &gateway_public_key_b64(&home).unwrap(),
        &clock,
    )
    .unwrap();
    let roots = RootSet::load(&home).unwrap();
    let claims = verify_token(&token, &roots, &clock).unwrap();
    let expected_root_fpr = fleet_root_fingerprint(&home).unwrap();
    assert_eq!(roots.fingerprints(), vec![expected_root_fpr.clone()]);

    clock.advance(chrono::Duration::seconds(
        claims.exp - clock.now().timestamp() + SKEW_TOLERANCE_SECS + 1,
    ));
    assert_eq!(verify_token(&token, &roots, &clock).unwrap_err().code, "TOKEN_EXPIRED");

    let (without_clock, issuing_root_fpr) = verify_token_no_clock(&token, &roots).unwrap();
    assert_eq!(without_clock, claims);
    assert_eq!(issuing_root_fpr, expected_root_fpr);
}
