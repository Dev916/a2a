//! Answers the ten server→client requests (spec §4.3).
//!
//! `codex app-server` does not only stream notifications: mid-turn it asks the
//! client questions that carry an `id` and therefore **owe an answer**
//! ([`crate::protocol::SERVER_REQUEST_METHODS`]). `a2a` is an unattended
//! orchestrator — there is no human at the other end of an approval prompt — so
//! every one of them is refused, immediately and by table.
//!
//! Two properties are load-bearing:
//!
//! * **Every request is answered, and fast.** A dropped request is not a slow
//!   turn, it is a *permanent hang*: the server blocks on the response forever
//!   and the worker never completes. [`respond_to`] is therefore a pure,
//!   allocation-light table lookup with no I/O and no fallible path — an unknown
//!   method still produces a JSON-RPC error rather than nothing (the 100ms
//!   budget in the plan's acceptance criteria is three orders of magnitude more
//!   than this needs).
//! * **The payloads are schema-exact, per family.** The deny spellings are *not*
//!   uniform across methods, and guessing produces a response the server rejects
//!   — which is indistinguishable from not answering at all. `ReviewDecision`
//!   (the older `execCommandApproval` / `applyPatchApproval` family) uses the
//!   tagged `{"denied":{"rejection":…}}` shape; the newer `item/*` approval
//!   family spells it `"decline"`; elicitation answers on `action`, not
//!   `decision`; and a dynamic tool call wants a *successful-shaped* response
//!   carrying `success:false`.
//!
//! **Four methods have no deny shape at all.** Their response schemas require
//! fields `a2a` cannot fabricate — granted permissions, user answers, ChatGPT
//! access tokens, an attestation token — so the only correct answer is a
//! JSON-RPC **error**. That is a feature, not a fallback:
//! `account/chatgptAuthTokens/refresh` erroring is what makes the server fall
//! back to refreshing its own credentials. Answering it "allowed" with an empty
//! token would break auth silently. `tests/protocol_canary.rs` byte-diffs those
//! response schemas against the live binary, and `protocol.rs` pins the fact
//! that they still require the impossible fields, so if a deny variant is ever
//! added upstream this table gets a failing test rather than staying wrong.
//!
//! No approval response carries a `reason` field. The legacy review family now
//! requires a nested `rejection` string, for which a2a sends a fixed generic
//! explanation. [`Answer::raw`] remains only the `{method, disposition}` pair
//! the caller logs as an [`crate::events::EventKind::ServerRequest`] lifecycle
//! envelope. Routing that envelope to the owning worker is the broker demux's
//! job, not this module's.

use crate::rpc::ServerRequestFrame;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The fake-fidelity helper, shared with the integration tests rather than
/// duplicated: the canned payloads below are validated against the very same
/// `schema/v2-snapshot/` files the fake's transcripts are held to. Declared at
/// file scope because `#[path]` inside an inline `mod` would resolve against a
/// `src/responder/tests/` directory that does not exist.
#[cfg(test)]
#[path = "../tests/common/fidelity.rs"]
mod fidelity;

/// JSON-RPC's standard "Method not found", used for requests outside
/// [`crate::protocol::SERVER_REQUEST_METHODS`].
pub const METHOD_NOT_FOUND: i64 = -32601;

/// Implementation-defined server error (the -32000..=-32099 range) for a known
/// method whose response schema `a2a` cannot satisfy. Distinct from
/// [`METHOD_NOT_FOUND`] on purpose: "I know what you asked and I refuse" and "I
/// have never heard of that" are different operational events.
pub const NO_DENY_SHAPE: i64 = -32000;

/// Required by the tagged `ReviewDecision::Denied` payload in codex 0.152.1.
const APPROVAL_REJECTION: &str = "a2a is unattended and denies interactive approval requests";

/// What `a2a` did with a server→client request, in the vocabulary the worker's
/// event log uses. Four values, because the four wire families are genuinely
/// different — collapsing them would make the log unable to distinguish "the
/// agent was told no" from "a2a could not answer at all".
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    /// `ReviewDecision` family: answered with a tagged `decision.denied`.
    Denied,
    /// `item/*` approval and elicitation families: answered `"decline"`.
    Declined,
    /// `item/tool/call`: answered with a `success:false` tool result.
    Unsupported,
    /// A known method with no deny shape: answered with a JSON-RPC error.
    Errored,
    /// A method not in [`crate::protocol::SERVER_REQUEST_METHODS`]: answered
    /// with `METHOD_NOT_FOUND` so the far side unblocks instead of hanging.
    UnknownMethod,
}

impl Disposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Disposition::Denied => "denied",
            Disposition::Declined => "declined",
            Disposition::Unsupported => "unsupported",
            Disposition::Errored => "errored",
            Disposition::UnknownMethod => "unknown_method",
        }
    }
}

impl fmt::Display for Disposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One canned answer: the payload to hand to [`crate::rpc::Client::respond`],
/// plus the `{method, disposition}` pair the caller logs.
#[derive(Clone, Debug, PartialEq)]
pub struct Answer {
    pub method: String,
    pub disposition: Disposition,
    /// Exactly the shape `Client::respond` takes: `Ok` becomes a result frame,
    /// `Err((code, message))` a JSON-RPC error frame.
    pub payload: Result<serde_json::Value, (i64, String)>,
}

impl Answer {
    /// The `{method, disposition}` object that becomes the `raw` of a
    /// [`crate::events::EventKind::ServerRequest`] lifecycle envelope. The
    /// payload deliberately does not appear: an approval request's params can
    /// carry a command line, and the event log is not the place for it.
    pub fn raw(&self) -> serde_json::Value {
        serde_json::json!({
            "method": self.method,
            "disposition": self.disposition.as_str(),
        })
    }
}

/// The table. Total over every possible method — there is no `None` arm and no
/// error return, because every path out of here must be an answer.
pub fn respond_to(frame: &ServerRequestFrame) -> Answer {
    let method = frame.method.as_str();
    let (disposition, payload) = match method {
        // --- ReviewDecision family (snake_case values) ---------------------
        "execCommandApproval" | "applyPatchApproval" => (
            Disposition::Denied,
            Ok(serde_json::json!({
                "decision": {
                    "denied": { "rejection": APPROVAL_REJECTION }
                }
            })),
        ),

        // --- item/* approval family (camelCase values) ---------------------
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => (
            Disposition::Declined,
            Ok(serde_json::json!({ "decision": "decline" })),
        ),

        // --- elicitation: answers on `action`, not `decision` --------------
        "mcpServer/elicitation/request" => (
            Disposition::Declined,
            Ok(serde_json::json!({ "action": "decline" })),
        ),

        // --- dynamic tool calls: a well-formed *failed* tool result ---------
        //
        // There is no decline shape here; the refusal rides in `success:false`
        // and the agent reads the text as the tool's output.
        "item/tool/call" => (
            Disposition::Unsupported,
            Ok(serde_json::json!({
                "success": false,
                "contentItems": [
                    { "type": "inputText", "text": "dynamic tool calls unsupported" }
                ]
            })),
        ),

        // --- no deny shape exists: JSON-RPC error is the only honest answer -
        "item/permissions/requestApproval" => (
            Disposition::Errored,
            Err((
                NO_DENY_SHAPE,
                "a2a grants no permissions: the response schema requires a granted \
                 permission profile and has no deny variant"
                    .to_string(),
            )),
        ),
        "item/tool/requestUserInput" => (
            Disposition::Errored,
            Err((
                NO_DENY_SHAPE,
                "a2a answers no user-input requests: the response schema requires \
                 answers and has no decline variant"
                    .to_string(),
            )),
        ),
        // Erroring here is deliberate and is NOT a degraded mode: it is what
        // lets the server run its own token refresh. Never answer "allowed".
        "account/chatgptAuthTokens/refresh" => (
            Disposition::Errored,
            Err((
                NO_DENY_SHAPE,
                "a2a holds no ChatGPT auth tokens: refresh on the server side".to_string(),
            )),
        ),
        // Unreachable in practice — `a2a` never sets `requestAttestation:true`
        // — and answered anyway, because "unreachable" is a claim about today's
        // handshake, not a guarantee about the wire.
        "attestation/generate" => (
            Disposition::Errored,
            Err((
                NO_DENY_SHAPE,
                "a2a issues no attestation tokens (requestAttestation is never set)".to_string(),
            )),
        ),

        // --- anything else -------------------------------------------------
        other => (
            Disposition::UnknownMethod,
            Err((
                METHOD_NOT_FOUND,
                format!("a2a does not answer server request method {other}"),
            )),
        ),
    };
    Answer {
        method: method.to_string(),
        disposition,
        payload,
    }
}

#[cfg(test)]
mod tests {
    use super::fidelity;
    use super::*;
    use crate::protocol::SERVER_REQUEST_METHODS;
    use serde_json::json;
    use std::time::{Duration, Instant};

    /// method → the snapshot schema its *payload* must satisfy. The four
    /// error-answered methods are absent by construction: they have no payload.
    const PAYLOAD_SCHEMAS: [(&str, &str); 6] = [
        ("execCommandApproval", "ExecCommandApprovalResponse.json"),
        ("applyPatchApproval", "ApplyPatchApprovalResponse.json"),
        (
            "item/commandExecution/requestApproval",
            "CommandExecutionRequestApprovalResponse.json",
        ),
        (
            "item/fileChange/requestApproval",
            "FileChangeRequestApprovalResponse.json",
        ),
        (
            "mcpServer/elicitation/request",
            "McpServerElicitationRequestResponse.json",
        ),
        ("item/tool/call", "DynamicToolCallResponse.json"),
    ];

    /// The four methods whose only correct answer is a JSON-RPC error.
    const ERRORED_METHODS: [&str; 4] = [
        "item/permissions/requestApproval",
        "item/tool/requestUserInput",
        "account/chatgptAuthTokens/refresh",
        "attestation/generate",
    ];

    fn frame(method: &str) -> ServerRequestFrame {
        ServerRequestFrame {
            id: json!(1),
            method: method.to_string(),
            params: json!({}),
        }
    }

    fn answer(method: &str) -> Answer {
        respond_to(&frame(method))
    }

    // --- the table, verbatim ------------------------------------------------

    #[test]
    fn the_payload_table_is_verbatim() {
        for method in ["execCommandApproval", "applyPatchApproval"] {
            let a = answer(method);
            assert_eq!(a.method, method);
            assert_eq!(a.disposition, Disposition::Denied);
            assert_eq!(
                a.payload,
                Ok(json!({
                    "decision": {
                        "denied": {"rejection": APPROVAL_REJECTION}
                    }
                })),
                "{method}"
            );
        }
        for method in [
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        ] {
            let a = answer(method);
            assert_eq!(a.disposition, Disposition::Declined);
            assert_eq!(a.payload, Ok(json!({"decision": "decline"})), "{method}");
        }
        let a = answer("mcpServer/elicitation/request");
        assert_eq!(a.disposition, Disposition::Declined);
        assert_eq!(a.payload, Ok(json!({"action": "decline"})));

        let a = answer("item/tool/call");
        assert_eq!(a.disposition, Disposition::Unsupported);
        assert_eq!(
            a.payload,
            Ok(json!({
                "success": false,
                "contentItems": [{"type": "inputText", "text": "dynamic tool calls unsupported"}]
            }))
        );
    }

    /// The two families spell refusal differently. Crossing them produces a
    /// response the server rejects, which reads exactly like a dropped answer.
    #[test]
    fn the_two_approval_families_do_not_share_a_spelling() {
        let old = answer("execCommandApproval").payload.unwrap();
        let new = answer("item/commandExecution/requestApproval")
            .payload
            .unwrap();
        assert_eq!(
            old["decision"]["denied"]["rejection"],
            APPROVAL_REJECTION
        );
        assert_eq!(new["decision"], "decline");
        assert_ne!(old, new);
    }

    #[test]
    fn no_answer_carries_a_reason_field() {
        // Reasons live in the worker's event log, never on the wire.
        for method in SERVER_REQUEST_METHODS {
            if let Ok(payload) = answer(method).payload {
                assert!(
                    payload.get("reason").is_none(),
                    "{method} invented a reason field: {payload}"
                );
            }
        }
    }

    // --- schema fidelity ----------------------------------------------------

    #[test]
    fn every_canned_payload_validates_against_its_snapshot_response_schema() {
        for (method, schema) in PAYLOAD_SCHEMAS {
            let payload = answer(method)
                .payload
                .unwrap_or_else(|e| panic!("{method} must answer with a payload, got {e:?}"));
            fidelity::assert_valid(&payload, schema, &format!("responder answer to {method}"));
        }
    }

    /// A guard on the guard: the validator must actually reject a wrong
    /// spelling, otherwise the test above would pass on anything.
    #[test]
    fn the_schema_check_rejects_the_wrong_familys_spelling() {
        let v = fidelity::validator("ExecCommandApprovalResponse.json");
        assert!(
            v.iter_errors(&json!({"decision": "decline"}))
                .next()
                .is_some(),
            "ReviewDecision must not accept the item/* spelling"
        );
        let v = fidelity::validator("CommandExecutionRequestApprovalResponse.json");
        assert!(
            v.iter_errors(&json!({"decision": "denied"}))
                .next()
                .is_some(),
            "the item/* family must not accept the ReviewDecision spelling"
        );
    }

    // --- the four with no deny shape ---------------------------------------

    #[test]
    fn methods_without_a_deny_shape_are_errored_never_answered() {
        for method in ERRORED_METHODS {
            let a = answer(method);
            assert_eq!(a.disposition, Disposition::Errored, "{method}");
            let (code, message) = a
                .payload
                .expect_err("no deny shape exists: the answer must be a JSON-RPC error");
            assert_eq!(code, NO_DENY_SHAPE, "{method}");
            assert!(!message.is_empty(), "{method} errored with no message");
        }
    }

    /// The one that would fail silently and expensively if it were ever
    /// "allowed": an empty-token answer would break auth instead of letting the
    /// server refresh itself.
    #[test]
    fn chatgpt_auth_token_refresh_is_errored_so_the_server_refreshes_itself() {
        let a = answer("account/chatgptAuthTokens/refresh");
        assert_eq!(a.disposition, Disposition::Errored);
        let (code, message) = a.payload.expect_err("must be an error, never a token");
        assert_eq!(code, NO_DENY_SHAPE);
        assert!(
            !message.to_ascii_lowercase().contains("allow"),
            "an 'allowed' answer here breaks auth: {message}"
        );
    }

    // --- totality + unknown methods ----------------------------------------

    #[test]
    fn every_pinned_server_request_method_has_an_answer() {
        for method in SERVER_REQUEST_METHODS {
            let a = answer(method);
            assert_eq!(a.method, method);
            assert_ne!(
                a.disposition,
                Disposition::UnknownMethod,
                "{method} is in SERVER_REQUEST_METHODS but fell through the table"
            );
        }
        // Exactly six payloads and four errors — the split is the point.
        assert_eq!(PAYLOAD_SCHEMAS.len() + ERRORED_METHODS.len(), 10);
        assert_eq!(SERVER_REQUEST_METHODS.len(), 10);
    }

    #[test]
    fn an_unknown_method_is_errored_and_names_itself() {
        let a = answer("sorcery/requestUnicorn");
        assert_eq!(a.disposition, Disposition::UnknownMethod);
        let (code, message) = a.payload.expect_err("unknown methods must be errored");
        assert_eq!(code, METHOD_NOT_FOUND);
        assert!(message.contains("sorcery/requestUnicorn"), "got {message}");
    }

    /// The 100ms budget, with three orders of magnitude of headroom. This is a
    /// pure table lookup precisely so that no future edit can put I/O between a
    /// request and its answer.
    #[test]
    fn answering_is_far_inside_the_100ms_budget() {
        let mut methods: Vec<&str> = SERVER_REQUEST_METHODS.to_vec();
        methods.push("sorcery/requestUnicorn");
        for method in methods {
            let t0 = Instant::now();
            let a = answer(method);
            let elapsed = t0.elapsed();
            assert!(
                elapsed < Duration::from_millis(100),
                "{method} took {elapsed:?}"
            );
            // And it really did produce something to send.
            assert!(a.payload.is_ok() || a.payload.is_err());
        }
    }

    // --- what the caller logs ----------------------------------------------

    #[test]
    fn raw_is_the_method_disposition_pair_and_nothing_else() {
        let a = answer("execCommandApproval");
        assert_eq!(
            a.raw(),
            json!({"method": "execCommandApproval", "disposition": "denied"})
        );
        assert_eq!(
            answer("sorcery/requestUnicorn").raw()["disposition"],
            "unknown_method"
        );
        // The params of an approval request can carry a command line; the
        // logged envelope must not.
        let mut f = frame("execCommandApproval");
        f.params = json!({"command": ["rm", "-rf", "/"]});
        let raw = respond_to(&f).raw();
        assert_eq!(raw.as_object().unwrap().len(), 2);
        assert!(!raw.to_string().contains("rm"));
    }

    #[test]
    fn dispositions_round_trip_through_their_wire_spelling() {
        for d in [
            Disposition::Denied,
            Disposition::Declined,
            Disposition::Unsupported,
            Disposition::Errored,
            Disposition::UnknownMethod,
        ] {
            assert_eq!(serde_json::to_value(d).unwrap(), json!(d.as_str()));
            assert_eq!(d.to_string(), d.as_str());
        }
    }
}
