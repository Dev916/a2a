//! Stable contract types shared by the observability plane's source readers,
//! fold, replay cursor, and serve layers.
//!
//! These types keep source-specific parsing behind one additive serde boundary
//! so later consumers do not need to understand the home directory's artifacts.

pub mod event;
pub mod snapshot;
pub mod cursor;
pub mod serve;
mod fold;
mod source;
mod tail;
pub mod normalize;

use std::path::PathBuf;

pub use cursor::Cursor;
pub use event::{Event, Plane, Src, Subject};
pub use serve::{router, AppState};
pub use snapshot::Snapshot;
pub use source::{read_json, read_jsonl, read_jsonl_from};

/// Read-only entry point for observing one a2a home.
#[derive(Clone, Debug)]
pub struct Observer {
    home: PathBuf,
}

impl Observer {
    pub fn open(home: impl Into<PathBuf>) -> Observer {
        Observer { home: home.into() }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = fold::snapshot(&self.home);
        tail::stamp_line_generations(&self.home, &mut snapshot.cursor);
        snapshot
    }

    /// Return normalized events after `cursor` and the cursor advanced past them.
    pub fn events_since(&self, cursor: &Cursor) -> (Vec<Event>, Cursor) {
        tail::events_since(&self.home, cursor)
    }
}
