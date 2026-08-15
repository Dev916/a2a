//! One module per CLI subcommand; each `run(...)` composes the pure core with IO
//! and returns the machine payload that `main` wraps in the output envelope.

pub mod config_show;
pub mod ls;
pub mod result;
pub mod send;
pub mod spawn;
pub mod status;
pub mod tail;
pub mod wait;
