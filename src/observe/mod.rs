//! Stable contract types shared by the observability plane's source readers,
//! fold, replay cursor, and serve layers.
//!
//! These types keep source-specific parsing behind one additive serde boundary
//! so later consumers do not need to understand the home directory's artifacts.

pub mod event;
pub mod snapshot;
pub mod cursor;

pub use cursor::Cursor;
pub use event::{Event, Plane, Src, Subject};
pub use snapshot::Snapshot;
