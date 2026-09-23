//! Versioned events emitted by the observability plane.
//!
//! `kind` and the envelope itself stay open so older consumers can continue
//! reading additive producers. [`Plane::Unknown`] provides the same guarantee
//! when a producer introduces a new plane.

use serde::{Deserialize, Serialize};

/// A source-normalized observation with enough provenance to trace it back to disk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub v: u32,
    pub ts: String,
    pub agent_id: String,
    pub plane: Plane,
    /// Open taxonomy: producers may add kinds without changing this contract.
    pub kind: String,
    pub subject: Subject,
    pub payload: serde_json::Value,
    pub src: Src,
}

/// Stable top-level event categories; unknown future categories remain readable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Plane {
    Mesh,
    Work,
    Worker,
    Presence,
    Health,
    #[serde(other)]
    Unknown,
}

/// Optional identifiers that let consumers associate an event with a resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Subject {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
}

/// On-disk origin of an event, retained so normalized output remains auditable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Src {
    pub artifact: String,
    #[serde(rename = "ref")]
    pub reference: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trips_and_tolerates_unknown_fields() {
        let e = Event {
            v: 1,
            ts: "2026-09-17T18:22:01.412Z".into(),
            agent_id: "gw-host-c726ba".into(),
            plane: Plane::Mesh,
            kind: "feed_entry".into(),
            subject: Subject {
                worker_id: None,
                work_id: None,
                peer: None,
            },
            payload: serde_json::json!({"origin_seq": 42}),
            src: Src {
                artifact: "mesh/feed.jsonl".into(),
                reference: "origin_seq=42".into(),
            },
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);
        assert_eq!(back.plane, Plane::Mesh);

        // Forward compatibility: unknown planes and fields do not reject an event.
        let future = r#"{"v":2,"ts":"t","agent_id":"a","plane":"quantum","kind":"k",
            "subject":{},"payload":{},"src":{"artifact":"x","ref":"y"},"future_field":true}"#;
        let f: Event = serde_json::from_str(future).unwrap();
        assert_eq!(f.plane, Plane::Unknown);
    }
}
