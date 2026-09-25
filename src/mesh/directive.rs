use crate::mesh::keys::{self, RootSet, TokenClaims};
use crate::mesh::row_sig::gateway_id_binds_key;
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DirectiveClass {
    Session,
    Harness,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directive {
    pub directive_id: String,
    pub from: String,
    pub target: String,
    pub ts: DateTime<Utc>,
    pub class: DirectiveClass,
    pub instruction: String,
    pub sig: String,
}

impl Directive {
    pub fn new(
        from: impl Into<String>,
        target: impl Into<String>,
        class: DirectiveClass,
        instruction: impl Into<String>,
    ) -> Self {
        let clock = keys::SystemClock;
        Self {
            directive_id: keys::mint_ulid(&clock),
            from: from.into(),
            target: target.into(),
            ts: Utc::now(),
            class,
            instruction: instruction.into(),
            sig: String::new(),
        }
    }
}

pub fn canonical_bytes(directive: &Directive) -> Result<String, AppError> {
    let fields: BTreeMap<&str, Value> = BTreeMap::from([
        ("class", json!(&directive.class)),
        ("directive_id", json!(&directive.directive_id)),
        ("from", json!(&directive.from)),
        ("instruction", json!(&directive.instruction)),
        ("target", json!(&directive.target)),
        ("ts", json!(&directive.ts)),
    ]);
    String::from_utf8(keys::canonical_json_fields(fields)?).map_err(|error| {
        AppError::new(
            "SERIALIZE",
            ExitCode::Internal,
            format!("canonical directive was not UTF-8: {error}"),
        )
    })
}

pub fn sign(directive: &mut Directive, key: &SigningKey) -> Result<(), AppError> {
    let canonical = canonical_bytes(directive)?;
    let signature = key.sign(canonical.as_bytes());
    directive.sig = keys::b64_encode(&signature.to_bytes());
    Ok(())
}

pub fn verify(directive: &Directive, roots: &RootSet) -> Result<String, AppError> {
    let signature_bytes = keys::b64_decode(&directive.sig)
        .map_err(|_| signature_invalid("directive signature is not base64"))?;
    let signature_bytes: [u8; 64] = signature_bytes
        .try_into()
        .map_err(|_| signature_invalid("directive signature has invalid length"))?;
    let signature = Signature::from_bytes(&signature_bytes);
    let canonical = canonical_bytes(directive)?;
    let mut sender_is_bound = false;

    for root in roots.entries() {
        let claims = TokenClaims {
            fleet_id: root.fleet_id.clone(),
            gateway_id: directive.from.clone(),
            pubkey_b64: root.pubkey_b64.clone(),
            key_epoch: 0,
            issued_at: 0,
            exp: 0,
        };
        if !gateway_id_binds_key(&directive.from, &claims) {
            continue;
        }
        sender_is_bound = true;
        let key = keys::verifying_key_from_b64(&claims.pubkey_b64)
            .map_err(|_| signature_invalid("trusted directive public key is invalid"))?;
        if key.verify(canonical.as_bytes(), &signature).is_ok() {
            return Ok(directive.from.clone());
        }
    }

    if sender_is_bound {
        Err(signature_invalid(
            "directive signature does not verify against the bound trusted key",
        ))
    } else {
        Err(signature_invalid(
            "directive sender is not bound to a trusted key",
        ))
    }
}

fn signature_invalid(message: &'static str) -> AppError {
    AppError::new(
        "DIRECTIVE_SIGNATURE_INVALID",
        ExitCode::Precondition,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::A2aHome;
    use crate::mesh::keys::{self, RootSet, TestClock};
    use chrono::{TimeZone, Utc};
    use ed25519_dalek::SigningKey;
    use serde_json::json;
    use std::fs;

    fn trusted_key() -> (tempfile::TempDir, SigningKey, RootSet, String) {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let key = SigningKey::from_bytes(&[7_u8; 32]);
        let pubkey_b64 = keys::b64_encode(&key.verifying_key().to_bytes());
        let fingerprint = keys::fpr_of_pubkey_b64(&pubkey_b64).unwrap();
        let root_dir = home.root().join("mesh/fleet");
        fs::create_dir_all(&root_dir).unwrap();
        fs::write(
            root_dir.join("root.pub"),
            serde_json::to_vec(&json!({
                "fleet_id": "fleet-directive-test",
                "pubkey_b64": pubkey_b64,
            }))
            .unwrap(),
        )
        .unwrap();
        let roots = RootSet::load(&home).unwrap();
        let from = format!("gw-directive-test-{}", &fingerprint[..16]);
        (tmp, key, roots, from)
    }

    fn fixture(from: String) -> Directive {
        let ts = Utc
            .with_ymd_and_hms(2026, 9, 25, 16, 21, 0)
            .single()
            .unwrap();
        Directive {
            directive_id: keys::mint_ulid(&TestClock::new(ts)),
            from,
            target: "gw-target-0123456789abcdef".to_string(),
            ts,
            class: DirectiveClass::Session,
            instruction: "start a brainstorm on signed directives".to_string(),
            sig: String::new(),
        }
    }

    #[test]
    fn signed_directive_verifies_against_a_bound_trusted_key() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from.clone());

        sign(&mut directive, &key).unwrap();

        assert!(!directive.sig.is_empty());
        assert_eq!(verify(&directive, &roots).unwrap(), from);
        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
    }

    #[test]
    fn new_directive_mints_a_ulid_and_starts_unsigned() {
        let directive = Directive::new(
            "gw-from-0123456789abcdef",
            "gw-target-0123456789abcdef",
            DirectiveClass::Session,
            "start the requested work",
        );

        assert!(ulid::Ulid::from_string(&directive.directive_id).is_ok());
        assert!(directive.sig.is_empty());
    }

    #[test]
    fn tampered_instruction_is_rejected() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from);
        sign(&mut directive, &key).unwrap();

        directive.instruction.push('!');

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn tampered_sender_is_rejected() {
        let (_tmp, key, roots, from) = trusted_key();
        let mut directive = fixture(from);
        sign(&mut directive, &key).unwrap();

        directive.from.push_str("-tampered");

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn a_valid_signature_from_an_unbound_key_is_rejected() {
        let (_tmp, key, roots, _from) = trusted_key();
        let mut directive = fixture("gw-directive-test-deadbeef".to_string());

        sign(&mut directive, &key).unwrap();

        assert!(verify(&directive, &roots).is_err());
    }

    #[test]
    fn canonical_bytes_are_stable_across_struct_field_order() {
        let (_tmp, _key, _roots, from) = trusted_key();
        let first = fixture(from.clone());
        let second = Directive {
            sig: "ignored-signature".to_string(),
            instruction: first.instruction.clone(),
            class: DirectiveClass::Session,
            ts: first.ts,
            target: first.target.clone(),
            from,
            directive_id: first.directive_id.clone(),
        };

        assert_eq!(canonical_bytes(&first).unwrap(), canonical_bytes(&second).unwrap());
    }

    #[test]
    fn unknown_directive_classes_deserialize_forward_compatibly() {
        assert_eq!(
            serde_json::from_str::<DirectiveClass>("\"future\"").unwrap(),
            DirectiveClass::Unknown
        );
    }
}
