use a2a::mesh::row_sig::{
    canonical_row_msg, parse_row_msg_strict, row_signature_digest, row_signature_prefix, sign_row,
    verify_row_bytes, Disposition, RowMsg, RowSigFault, RowVerdict, ROW_MSG_VER,
};
use a2a::mesh::render::{escape_untrusted, fence_untrusted, Rendered};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};

const GOLDEN_ROW_MSG: &str = r#"{"cert_fpr":"c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2","created_at":"2026-01-02T03:04:05.678901Z","endpoint_url":"wss://gw.example.test:7787","enforce_auto_push":false,"enforce_require_signed_rows":true,"fleet_id":"fleet-golden","gateway_id":"gw-golden-0123456789abcdef","key_epoch":1,"msg_ver":1,"owner":"mike","pubkey_fpr":"a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","registry_heartbeat_secs":180,"roots_held":"[\"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4\"]","row_resign_secs":86400,"row_token_sha256":"d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3","schema_version":1,"signed_at":"2026-03-04T05:06:07.891234Z","version":"0.3.0"}"#;

// These three values are filled from the first signer run exactly once. Never regenerate them:
// any later change is the regression this complete five-literal tuple is meant to expose.
const GOLDEN_DIGEST_HEX: &str =
    "2f8ee18ee876a1c3b506ec84cb2ce89e867bb195efdecd542515f7daf3ee5a9d";
const GOLDEN_SIG_B64: &str =
    "ZFAOhsuNne1GXAeWGNH0kVxjaEKSEY3pqbk5zAae2FlHuUqSXBnhvmhesTV/hVzGMnS0oM31FnrCS9pGZpZgDw==";
const GOLDEN_PUBKEY_B64: &str = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=";

fn golden_fixture() -> RowMsg {
    RowMsg {
        msg_ver: ROW_MSG_VER,
        fleet_id: "fleet-golden".to_string(),
        gateway_id: "gw-golden-0123456789abcdef".to_string(),
        endpoint_url: Some("wss://gw.example.test:7787".to_string()),
        pubkey_fpr:
            "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
                .to_string(),
        cert_fpr: Some(
            "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2"
                .to_string(),
        ),
        version: "0.3.0".to_string(),
        owner: "mike".to_string(),
        created_at: "2026-01-02T03:04:05.678901Z"
            .parse::<DateTime<Utc>>()
            .unwrap(),
        schema_version: 1,
        key_epoch: 1,
        roots_held:
            "[\"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4\"]"
                .to_string(),
        row_token_sha256:
            "d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3"
                .to_string(),
        row_resign_secs: 86_400,
        registry_heartbeat_secs: 180,
        enforce_require_signed_rows: true,
        enforce_auto_push: false,
        signed_at: "2026-03-04T05:06:07.891234Z"
            .parse::<DateTime<Utc>>()
            .unwrap(),
    }
}

#[test]
fn golden_row_vector_is_pinned_as_a_tuple() {
    let key = SigningKey::from_bytes(&[7_u8; 32]);
    let (row_msg, row_sig_b64) = sign_row(&golden_fixture(), &key).unwrap();
    let digest_hex = hex(&row_signature_digest(ROW_MSG_VER, row_msg.as_bytes()));
    let pubkey_b64 = b64_encode(&key.verifying_key().to_bytes());

    assert_eq!(row_msg, GOLDEN_ROW_MSG);
    assert_eq!(
        row_signature_prefix(ROW_MSG_VER),
        b"a2a/registry-row/v1\0".to_vec()
    );
    assert_eq!(digest_hex, GOLDEN_DIGEST_HEX);
    assert_eq!(row_sig_b64, GOLDEN_SIG_B64);
    assert_eq!(pubkey_b64, GOLDEN_PUBKEY_B64);
}

#[test]
fn row_signature_version_is_unpadded_decimal() {
    assert_eq!(
        row_signature_prefix(10),
        b"a2a/registry-row/v10\0".to_vec()
    );
}

#[test]
fn duplicate_key_row_msg_is_refused_even_when_the_signature_verifies() {
    let key = SigningKey::from_bytes(&[7_u8; 32]);
    let duplicated = format!(
        "{},\"owner\":\"eve\"}}",
        GOLDEN_ROW_MSG.strip_suffix('}').unwrap()
    );
    let signature = sign_bytes(&duplicated, &key, ROW_MSG_VER);

    assert_eq!(
        verify_row_bytes(&duplicated, &signature, GOLDEN_PUBKEY_B64),
        Err(RowSigFault::Malformed("duplicate key"))
    );
}

#[test]
fn unknown_msg_ver_is_reported_before_any_signature_check() {
    let unknown = GOLDEN_ROW_MSG.replacen("\"msg_ver\":1", "\"msg_ver\":2", 1);

    assert_eq!(
        verify_row_bytes(&unknown, "not-base64", "also-not-a-public-key"),
        Err(RowSigFault::UnknownVersion(2))
    );
}

#[test]
fn unknown_and_missing_fields_are_malformations() {
    let unknown = format!(
        "{},\"surprise\":1}}",
        GOLDEN_ROW_MSG.strip_suffix('}').unwrap()
    );
    assert_eq!(
        parse_row_msg_strict(&unknown),
        Err(RowSigFault::Malformed("unknown field"))
    );

    let missing = GOLDEN_ROW_MSG.replacen(",\"owner\":\"mike\"", "", 1);
    assert_eq!(
        parse_row_msg_strict(&missing),
        Err(RowSigFault::Malformed("missing field"))
    );
}

#[test]
fn non_object_row_msg_is_a_malformation() {
    assert_eq!(
        parse_row_msg_strict("[]"),
        Err(RowSigFault::Malformed(
            "row_msg is not a flat JSON object"
        ))
    );
}

#[test]
fn every_signed_field_tamper_breaks_the_row_signature() {
    let key = SigningKey::from_bytes(&[7_u8; 32]);
    let (golden_json, golden_sig) = sign_row(&golden_fixture(), &key).unwrap();
    let pubkey = b64_encode(&key.verifying_key().to_bytes());
    let mut cases = Vec::new();

    // A semantic msg_ver change is the separately-tested UnknownVersion precedence case. This
    // byte-level mutation is localized to msg_ver while keeping it readable as supported v1.
    cases.push((
        "msg_ver",
        golden_json.replacen("\"msg_ver\":1", "\"msg_ver\": 1", 1),
    ));

    let mut msg = golden_fixture();
    msg.fleet_id.push('x');
    cases.push(("fleet_id", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.gateway_id.push('x');
    cases.push(("gateway_id", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.endpoint_url.as_mut().unwrap().push('x');
    cases.push(("endpoint_url", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.pubkey_fpr.push('x');
    cases.push(("pubkey_fpr", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.cert_fpr.as_mut().unwrap().push('x');
    cases.push(("cert_fpr", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.version.push('x');
    cases.push(("version", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.owner.push('x');
    cases.push(("owner", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.created_at += chrono::Duration::seconds(1);
    cases.push(("created_at", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.schema_version += 1;
    cases.push(("schema_version", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.key_epoch += 1;
    cases.push(("key_epoch", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.roots_held.insert_str(
        msg.roots_held.len() - 1,
        ",\"e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5\"",
    );
    cases.push(("roots_held", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.row_token_sha256.push('x');
    cases.push(("row_token_sha256", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.row_resign_secs += 1;
    cases.push(("row_resign_secs", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.registry_heartbeat_secs += 1;
    cases.push((
        "registry_heartbeat_secs",
        canonical_row_msg(&msg).unwrap(),
    ));

    let mut msg = golden_fixture();
    msg.enforce_require_signed_rows = !msg.enforce_require_signed_rows;
    cases.push((
        "enforce_require_signed_rows",
        canonical_row_msg(&msg).unwrap(),
    ));

    let mut msg = golden_fixture();
    msg.enforce_auto_push = !msg.enforce_auto_push;
    cases.push(("enforce_auto_push", canonical_row_msg(&msg).unwrap()));

    let mut msg = golden_fixture();
    msg.signed_at += chrono::Duration::seconds(1);
    cases.push(("signed_at", canonical_row_msg(&msg).unwrap()));

    assert_eq!(cases.len(), 18);
    for (field, tampered_json) in cases {
        assert_eq!(
            verify_row_bytes(&tampered_json, &golden_sig, &pubkey),
            Err(RowSigFault::SignatureMismatch),
            "tampering {field} must invalidate the row signature"
        );
    }
}

#[test]
fn non_canonical_but_validly_signed_row_msg_is_accepted_and_bound_from_the_parse() {
    const REVERSE_ORDER_ROW_MSG: &str = r#"{"version":  "0.3.0","signed_at":  "2026-03-04T05:06:07.891234Z","schema_version":  1,"row_token_sha256":  "d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3","row_resign_secs":  86400,"roots_held":  "[\"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4\"]","registry_heartbeat_secs":  180,"pubkey_fpr":  "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","owner":  "mike","msg_ver":  1,"key_epoch":  1,"gateway_id":  "gw-golden-0123456789abcdef","fleet_id":  "fleet-golden","enforce_require_signed_rows":  true,"enforce_auto_push":  false,"endpoint_url":  "wss://gw.example.test:7787","created_at":  "2026-01-02T03:04:05.678901Z","cert_fpr":  "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2"}"#;
    let key = SigningKey::from_bytes(&[7_u8; 32]);
    let signature = sign_bytes(REVERSE_ORDER_ROW_MSG, &key, ROW_MSG_VER);
    let verified = verify_row_bytes(REVERSE_ORDER_ROW_MSG, &signature, GOLDEN_PUBKEY_B64).unwrap();

    assert_eq!(verified.owner, "mike");
    assert_eq!(verified.signed_at, golden_fixture().signed_at);
    assert_eq!(verified, golden_fixture());
}

#[test]
fn sub_microsecond_precision_is_truncated_before_signing() {
    let mut msg = golden_fixture();
    msg.signed_at = "2026-03-04T05:06:05.678901999Z"
        .parse::<DateTime<Utc>>()
        .unwrap();
    let key = SigningKey::from_bytes(&[7_u8; 32]);
    let (canonical, signature) = sign_row(&msg, &key).unwrap();

    assert!(canonical.contains("\"signed_at\":\"2026-03-04T05:06:05.678901Z\""));
    let parsed = verify_row_bytes(&canonical, &signature, GOLDEN_PUBKEY_B64).unwrap();
    assert_eq!(canonical_row_msg(&parsed).unwrap(), canonical);
}

#[test]
fn disposition_table_matches_the_spec_flag_table() {
    let cases = [
        (
            RowVerdict::ValidSigned,
            Disposition::Usable,
            Disposition::Usable,
        ),
        (
            RowVerdict::Unsigned { stripped: false },
            Disposition::UsableWarn,
            Disposition::Refused,
        ),
        (
            RowVerdict::Unsigned { stripped: true },
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::UnsignedStale,
            Disposition::UsableWarn,
            Disposition::Refused,
        ),
        (
            RowVerdict::Unverified,
            Disposition::UsableWarn,
            Disposition::Refused,
        ),
        (
            RowVerdict::BadSig,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowSigIncomplete,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowSigUnknownVersion,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowKeySuperseded,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowReplayed,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowClockAhead,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::Revoked,
            Disposition::Refused,
            Disposition::Refused,
        ),
        (
            RowVerdict::RowRevocationMisplaced,
            Disposition::Refused,
            Disposition::Refused,
        ),
    ];

    for (verdict, without_flag, with_flag) in cases {
        assert_eq!(
            verdict.disposition(false),
            without_flag,
            "{} without require_signed_rows",
            verdict.label()
        );
        assert_eq!(
            verdict.disposition(true),
            with_flag,
            "{} with require_signed_rows",
            verdict.label()
        );
    }
}

#[test]
fn verdict_labels_are_the_spec_strings() {
    let cases = [
        (RowVerdict::ValidSigned, "valid-signed"),
        (RowVerdict::Unsigned { stripped: false }, "UNSIGNED"),
        (
            RowVerdict::Unsigned { stripped: true },
            "UNSIGNED (stripped)",
        ),
        (RowVerdict::UnsignedStale, "UNSIGNED-STALE"),
        (RowVerdict::Unverified, "UNVERIFIED"),
        (RowVerdict::BadSig, "BAD_SIG"),
        (RowVerdict::RowSigIncomplete, "ROW_SIG_INCOMPLETE"),
        (
            RowVerdict::RowSigUnknownVersion,
            "ROW_SIG_UNKNOWN_VERSION",
        ),
        (RowVerdict::RowKeySuperseded, "ROW_KEY_SUPERSEDED"),
        (RowVerdict::RowReplayed, "ROW_REPLAYED"),
        (RowVerdict::RowClockAhead, "ROW_CLOCK_AHEAD"),
        (RowVerdict::Revoked, "REVOKED"),
        (
            RowVerdict::RowRevocationMisplaced,
            "ROW_REVOCATION_MISPLACED",
        ),
    ];

    for (verdict, label) in cases {
        assert_eq!(verdict.label(), label);
    }
}

#[test]
fn untrusted_rendering_escapes_all_terminal_controls_and_reports_flags() {
    assert_eq!(
        escape_untrusted("\u{1b}\r\u{202e}\u{200b}ok", 16),
        Rendered {
            text: "\\u001B\\u000D\\u202E\\u200Bok".to_string(),
            non_ascii: true,
            clamped: false,
        }
    );

    assert_eq!(
        escape_untrusted("abé", 2),
        Rendered {
            text: "ab".to_string(),
            non_ascii: true,
            clamped: true,
        }
    );
}

#[test]
fn untrusted_fence_is_emitted_from_fixed_a2a_markers() {
    assert_eq!(
        fence_untrusted("safe escaped text"),
        "----- BEGIN UNTRUSTED CONTENT -----\nsafe escaped text\n----- END UNTRUSTED CONTENT -----"
    );
}

fn sign_bytes(text: &str, key: &SigningKey, msg_ver: u64) -> String {
    let digest = row_signature_digest(msg_ver, text.as_bytes());
    b64_encode(&key.sign(&digest).to_bytes())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
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
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}
