use a2a::home::A2aHome;
use a2a::mesh::keys::{
    fleet_init, gateway_identity, mint_ulid, sign_token, verify_token, RootSet, TestClock,
    TokenClaims, SKEW_TOLERANCE_SECS,
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
    assert!(fleet.fleet_id["fleet-".len()..].chars().all(|c| c.is_ascii_hexdigit()));

    let gw = gateway_identity(&home, false).unwrap();
    let again = gateway_identity(&home, false).unwrap();
    assert_eq!(gw.gateway_id, again.gateway_id);
    assert_eq!(gw.cert_fpr, again.cert_fpr);
    assert!(gw.gateway_id.starts_with("gw-"), "{}", gw.gateway_id);
    assert_eq!(gw.cert_fpr.len(), 64);
    assert!(gw.cert_fpr.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
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
fn sign_verify_roundtrip_and_root_rotation_accepts_current_or_next() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    let root = fleet_init(&home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = sign_token(&claims(1_700_001_000, 1_699_999_000), &home).unwrap();
    let verified = verify_token(&token, &RootSet::load(&home).unwrap(), &clock).unwrap();
    assert_eq!(verified.gateway_id, "gw-testhost-abcdef");
    assert_eq!(verified.fleet_id, root.fleet_id);

    let next_tmp = tempfile::tempdir().unwrap();
    let next_home = home_in(&next_tmp);
    fleet_init(&next_home).unwrap();
    let next_token = sign_token(&claims(1_700_001_000, 1_699_999_000), &next_home).unwrap();
    fs::copy(
        next_tmp.path().join("mesh/fleet/root.pub"),
        tmp.path().join("mesh/fleet/root.pub.next"),
    )
    .unwrap();
    let verified_next = verify_token(&next_token, &RootSet::load(&home).unwrap(), &clock).unwrap();
    assert_eq!(verified_next.gateway_id, "gw-testhost-abcdef");
}

#[test]
fn clock_skew_boundaries_are_injected_and_expired_error_names_both_timestamps() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let roots = RootSet::load(&home).unwrap();

    let now = 1_700_000_000;
    let token = sign_token(&claims(now + 10, now - 10), &home).unwrap();

    let ahead_ok = TestClock::new(Utc.timestamp_opt(now + 10 + SKEW_TOLERANCE_SECS - 1, 0).unwrap());
    verify_token(&token, &roots, &ahead_ok).unwrap();
    let behind_ok =
        TestClock::new(Utc.timestamp_opt(now - 10 - SKEW_TOLERANCE_SECS + 1, 0).unwrap());
    verify_token(&token, &roots, &behind_ok).unwrap();

    let ahead_bad =
        TestClock::new(Utc.timestamp_opt(now + 10 + SKEW_TOLERANCE_SECS + 1, 0).unwrap());
    let err = verify_token(&token, &roots, &ahead_bad).unwrap_err();
    assert_eq!(err.code, "TOKEN_EXPIRED");
    assert!(err.message.contains(&(now + 10).to_string()), "{}", err.message);
    assert!(
        err.message
            .contains(&(now + 10 + SKEW_TOLERANCE_SECS + 1).to_string()),
        "{}",
        err.message
    );

    let future_token = sign_token(&claims(now + 10, now + 10), &home).unwrap();
    let behind_bad =
        TestClock::new(Utc.timestamp_opt(now + 10 - SKEW_TOLERANCE_SECS - 1, 0).unwrap());
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
    corpus.push(format!("{:?}", TokenClaims {
        pubkey_b64: "public".to_string(),
        ..claims(1, 0)
    }));

    for entry in corpus {
        assert!(!entry.contains(secret), "{entry}");
    }
}
