use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64, gateway_signing_key,
    install_gateway_token, TestClock,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, validate_envelope_hygiene, UnsignedWorkEnvelope,
    WorkAddressing, WorkRepo,
};
use chrono::{Duration, TimeZone, Utc};
use ed25519_dalek::Signer;
use std::fs;
use std::path::Path;

const MAX_DIRECTED_BYTES: usize = 262_144;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &Path, joiner: &Path) {
    fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome, clock: &TestClock) {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, clock).unwrap();
    install_gateway_token(joiner, &token, clock).unwrap();
}

fn fixture() -> (tempfile::TempDir, A2aHome, A2aHome, TestClock) {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    enroll_joiner(&root, &sender, &clock);
    (tmp, root, sender, clock)
}

fn unsigned() -> UnsignedWorkEnvelope {
    UnsignedWorkEnvelope {
        work_id: "01HZ7P7Q0V0V7G01J4J8R8M2Q0".to_string(),
        session: "codex:session-123".to_string(),
        addressing: WorkAddressing::Pool,
        repo: WorkRepo {
            remote_url: "https://github.com/example/project.git".to_string(),
            base_ref: "main".to_string(),
        },
        title: "Implement mesh handoff".to_string(),
        brief_text: "---\nlane: impl\n---\nDo the task.".to_string(),
        acceptance: "focused test passes".to_string(),
        ttl_days: 7,
        max_bytes: MAX_DIRECTED_BYTES,
    }
}

fn assert_code<T: std::fmt::Debug>(result: Result<T, a2a::output::AppError>, code: &str) {
    let err = result.unwrap_err();
    assert_eq!(err.code, code, "{err:?}");
}

#[test]
fn signed_envelope_round_trips_and_rejects_verification_matrix() {
    let (tmp, root, sender, clock) = fixture();
    let receiver = home_at(&tmp.path().join("receiver"));

    let env = sign_envelope(&sender, unsigned(), &clock).unwrap();
    validate_envelope_hygiene(&env, MAX_DIRECTED_BYTES).unwrap();
    assert_eq!(serde_json::to_value(&env).unwrap()["addressing"], "pool");
    assert_eq!(
        serde_json::from_value::<WorkAddressing>(serde_json::json!("directed:gw-target"))
            .unwrap(),
        WorkAddressing::Directed("gw-target".to_string())
    );
    let verified = verify_envelope(&receiver, &env, &clock).unwrap();
    assert_eq!(verified.envelope, env);
    assert_eq!(verified.sender_claims.gateway_id, env.sender.gateway_id);

    let mut tampered = env.clone();
    tampered.title.push('!');
    assert_code(
        verify_envelope(&receiver, &tampered, &clock),
        "WORK_SIGNATURE_INVALID",
    );

    let other = home_at(&tmp.path().join("other-sender"));
    copy_root_pub(root.root(), other.root());
    enroll_joiner(&root, &other, &clock);
    let mut wrong_key_sig = env.clone();
    wrong_key_sig.sig_b64 = sign_envelope(&other, unsigned(), &clock).unwrap().sig_b64;
    assert_code(
        verify_envelope(&receiver, &wrong_key_sig, &clock),
        "WORK_SIGNATURE_INVALID",
    );

    let foreign_root = home_at(&tmp.path().join("foreign-root"));
    let foreign_sender = home_at(&tmp.path().join("foreign-sender"));
    fleet_init(&foreign_root).unwrap();
    copy_root_pub(foreign_root.root(), foreign_sender.root());
    enroll_joiner(&foreign_root, &foreign_sender, &clock);
    let foreign_env = sign_envelope(&foreign_sender, unsigned(), &clock).unwrap();
    assert_code(
        verify_envelope(&receiver, &foreign_env, &clock),
        "WORK_SENDER_UNKNOWN",
    );

    fs::copy(
        foreign_root.root().join("mesh/fleet/root.pub"),
        receiver.root().join("mesh/fleet/root.pub.next"),
    )
    .unwrap();
    assert_code(
        verify_envelope(&receiver, &foreign_env, &clock),
        "ROOT_NEXT_FLEET_MISMATCH",
    );
    // A foreign next root poisons the whole RootSet (by design) — remove it
    // so the remaining matrix legs verify under a clean set again.
    fs::remove_file(receiver.root().join("mesh/fleet/root.pub.next")).unwrap();

    let mut binding_mismatch = env.clone();
    binding_mismatch.sender.gateway_id.push_str("-renamed");
    assert_code(
        verify_envelope(&receiver, &binding_mismatch, &clock),
        "WORK_TOKEN_MISMATCH",
    );

    let old_clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let stale_root = home_at(&tmp.path().join("stale-root"));
    let stale_sender = home_at(&tmp.path().join("stale-sender"));
    let stale_receiver = home_at(&tmp.path().join("stale-receiver"));
    fleet_init(&stale_root).unwrap();
    copy_root_pub(stale_root.root(), stale_sender.root());
    copy_root_pub(stale_root.root(), stale_receiver.root());
    enroll_joiner(&stale_root, &stale_sender, &old_clock);
    let stale_env = sign_envelope(&stale_sender, unsigned(), &old_clock).unwrap();
    old_clock.advance(Duration::days(91));
    assert_code(
        verify_envelope(&stale_receiver, &stale_env, &old_clock),
        "WORK_SENDER_UNKNOWN",
    );

    let mut expires_now = unsigned();
    expires_now.ttl_days = 0;
    let expired_env = sign_envelope(&sender, expires_now, &clock).unwrap();
    assert_code(
        verify_envelope(&receiver, &expired_env, &clock),
        "WORK_EXPIRED",
    );

}

#[test]
fn envelope_signatures_are_domain_separated_from_raw_canonical_json() {
    let (tmp, _root, sender, clock) = fixture();
    let receiver = home_at(&tmp.path().join("receiver"));
    let mut env = sign_envelope(&sender, unsigned(), &clock).unwrap();
    let canonical = a2a::mesh::work::canonical_envelope_without_sig(&env).unwrap();
    let signing = gateway_signing_key(&sender).unwrap();
    env.sig_b64 = b64_encode(&signing.sign(&canonical).to_bytes());

    assert_code(
        verify_envelope(&receiver, &env, &clock),
        "WORK_SIGNATURE_INVALID",
    );
}

#[test]
fn envelope_hygiene_is_rejected_at_construction() {
    let (_tmp, _root, sender, clock) = fixture();

    let mut oversized = unsigned();
    oversized.brief_text = "x".repeat(300_000);
    assert_code(
        sign_envelope(&sender, oversized, &clock),
        "WORK_TOO_LARGE",
    );

    let mut bad_url = unsigned();
    bad_url.repo.remote_url = "-oProxyCommand=sh".to_string();
    assert_code(
        sign_envelope(&sender, bad_url, &clock),
        "WORK_BAD_REPO_URL",
    );

    let mut ansi_title = unsigned();
    ansi_title.title = "approve\u{1b}[31m".to_string();
    assert_code(
        sign_envelope(&sender, ansi_title, &clock),
        "WORK_CONTROL_CHARS",
    );
}

#[test]
fn base_ref_hygiene_refuses_control_chars_leading_dash_and_bad_ref_shape() {
    let (_tmp, _root, sender, clock) = fixture();
    let bad_refs = [
        "-x".to_string(),
        "a\nb".to_string(),
        "refs//x".to_string(),
        "x..y".to_string(),
        "x".repeat(300),
    ];
    for bad_ref in &bad_refs {
        let mut value = unsigned();
        value.repo.base_ref = bad_ref.clone();
        assert_code(sign_envelope(&sender, value, &clock), "WORK_BAD_BASE_REF");
    }

    for valid_ref in ["main", "release/1.2"] {
        let mut value = unsigned();
        value.repo.base_ref = valid_ref.to_string();
        let envelope = sign_envelope(&sender, value, &clock).unwrap();
        validate_envelope_hygiene(&envelope, MAX_DIRECTED_BYTES).unwrap();

        for bad_ref in &bad_refs {
            let mut received = envelope.clone();
            received.repo.base_ref = bad_ref.clone();
            assert_code(
                validate_envelope_hygiene(&received, MAX_DIRECTED_BYTES),
                "WORK_BAD_BASE_REF",
            );
        }
    }
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
