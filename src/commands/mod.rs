//! One module per CLI subcommand; each `run(...)` composes the pure core with IO
//! and returns the machine payload that `main` wraps in the output envelope.

pub mod broker_cmd;
pub mod config_show;
pub mod doctor;
pub mod gc;
pub mod interrupt;
pub mod kill;
pub mod ls;
pub mod result;
pub mod resume;
pub mod send;
pub mod spawn;
pub mod status;
pub mod tail;
pub mod wait;
