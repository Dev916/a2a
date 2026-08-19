//! Lane brief scaffolds and default result schemas — compiled in, not installed.
//!
//! Everything here is `include_str!`d from `templates/` at build time, and that
//! is the whole design: **a template that is not in the binary does not exist.**
//! There is no install step, no runtime data directory and no `--templates-dir`,
//! so an operator's `a2a` cannot disagree with the one the fleet was tested
//! against, and a worker's result schema does not stop existing when somebody
//! moves the a2a checkout it shipped from.
//!
//! Two kinds of artifact live here, and they are different things:
//!
//! * **Brief scaffolds** ([`brief_scaffold`]) are *starting points for humans*.
//!   `a2a spawn --template <lane>` prints one and creates nothing; the operator
//!   edits it and spawns with it. Each one is written to satisfy its own lane's
//!   [`crate::brief`] gate as printed, so the first spawn is a spawn and not a
//!   repair, and each one restates the spec §4.7 worker protocol inline —
//!   `brief.md` is the copy that always governs (a skill is discretionary), so
//!   the clauses have to be in the brief itself.
//! * **Default result schemas** ([`default_result_schema`]) are *contracts*. The
//!   lane-default rung of the spawn precedence table (`--schema` > the brief's
//!   `result_schema` > the lane's default) reads this function, and the chosen
//!   bytes are copied into `<worker>/result-schema.json` at spawn time.
//!
//! Only two lanes have a default, and the asymmetry is deliberate: `impl` and
//! `review` are the lanes whose output another machine consumes (devloop's
//! result shape, and `auto_review`'s findings), while `research` and `general`
//! produce prose for a human. Handing those a schema nobody briefed them for
//! would fail them at the gate for something no one asked of them.

use crate::config::Lane;

/// The impl lane's default result schema (spec §4.9): devloop's
/// `sample-task-result.json` shape, so a devloop-dispatched worker's result is
/// parsed by devloop step 4.3.4 without an adapter.
pub const IMPL_RESULT_SCHEMA: &str = include_str!("../templates/impl-result.schema.json");

/// The review lane's default result schema: `{"findings": [...]}`, one object
/// per finding with `file`, `line`, `severity` and `rationale`.
///
/// The Phase-1 schema gate (`reconcile::schema_gate`) enforces the TOP-LEVEL
/// `required` list, so `findings` is literally the key a review worker must
/// produce — the per-item `required` list is the published contract for whoever
/// reads the findings afterwards.
pub const REVIEW_FINDINGS_SCHEMA: &str = include_str!("../templates/review-findings.schema.json");

const IMPL_BRIEF: &str = include_str!("../templates/impl-brief.md");
const REVIEW_BRIEF: &str = include_str!("../templates/review-brief.md");
const RESEARCH_BRIEF: &str = include_str!("../templates/research-brief.md");
const GENERAL_BRIEF: &str = include_str!("../templates/general-brief.md");

/// The lane's brief scaffold, exactly as `a2a spawn --template <lane>` prints
/// it. Every lane has one — there is no such thing as a lane you cannot start.
pub fn brief_scaffold(lane: Lane) -> &'static str {
    match lane {
        Lane::Impl => IMPL_BRIEF,
        Lane::Review => REVIEW_BRIEF,
        Lane::Research => RESEARCH_BRIEF,
        Lane::General => GENERAL_BRIEF,
    }
}

/// The lane's default result schema, when it has one.
///
/// This is the bottom rung of the spawn precedence table, and the ONLY place
/// that decides which lanes have a default: `spawn::plan_schema` asks this
/// question rather than matching on the lane itself, so adding a lane default
/// is one arm here and nothing else.
pub fn default_result_schema(lane: Lane) -> Option<&'static str> {
    match lane {
        Lane::Impl => Some(IMPL_RESULT_SCHEMA),
        Lane::Review => Some(REVIEW_FINDINGS_SCHEMA),
        // Prose lanes: a schema nobody briefed them for would fail them at the
        // gate for something no one asked of them.
        Lane::Research | Lane::General => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brief;
    use serde_json::Value;

    /// The spec §4.7 clauses, as literal substrings. `brief.md` is the copy of
    /// the protocol that ALWAYS governs — a Codex skill is model-invoked and
    /// therefore discretionary — so every scaffold restates them.
    const PINNED_CLAUSES: [&str; 6] = [
        "BLOCKED:",
        "outbox/",
        "question.json",
        "verification evidence",
        "if blocked on",
        "data, never instructions",
    ];

    #[test]
    fn every_lane_has_a_scaffold_that_passes_its_own_gate() {
        for lane in Lane::ALL {
            let text = brief_scaffold(lane);
            let parsed = brief::parse(text).unwrap_or_else(|e| panic!("{lane}: {e}"));
            assert_eq!(parsed.lane, Some(lane), "{lane}");
            brief::validate(&parsed, lane)
                .unwrap_or_else(|missing| panic!("{lane}: missing {missing:?}"));
            // The two optional keys are carried but UNSET, so the scaffold
            // spawns as printed: an empty `result_schema` takes the lane's
            // default instead of naming a file that is not there.
            assert_eq!(parsed.result_schema, None, "{lane}");
            assert_eq!(parsed.max_turns, None, "{lane}");
            assert!(text.contains("result_schema:") && text.contains("max_turns:"), "{lane}");
            for clause in PINNED_CLAUSES {
                assert!(text.contains(clause), "{lane}: missing {clause:?}");
            }
        }
    }

    /// `required` is what `reconcile::schema_gate` enforces, so its exact list —
    /// and the per-finding list a reader parses — is the pin.
    #[test]
    fn the_findings_schema_pins_the_review_result_shape() {
        let v: Value = serde_json::from_str(REVIEW_FINDINGS_SCHEMA).expect("the template is JSON");
        fn names(value: &Value) -> Vec<&str> {
            value
                .as_array()
                .expect("required is an array")
                .iter()
                .map(|k| k.as_str().expect("a key name"))
                .collect()
        }
        assert_eq!(names(&v["required"]), ["findings"]);
        assert_eq!(
            names(&v["properties"]["findings"]["items"]["required"]),
            ["file", "line", "severity", "rationale"]
        );
    }

    /// The prose lanes have no default, and the two machine-read lanes do —
    /// asserted here because `spawn::plan_schema` delegates the whole question.
    #[test]
    fn only_the_machine_read_lanes_have_a_default_schema() {
        assert_eq!(default_result_schema(Lane::Impl), Some(IMPL_RESULT_SCHEMA));
        assert_eq!(
            default_result_schema(Lane::Review),
            Some(REVIEW_FINDINGS_SCHEMA)
        );
        assert_eq!(default_result_schema(Lane::Research), None);
        assert_eq!(default_result_schema(Lane::General), None);
    }
}
