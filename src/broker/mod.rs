//! The broker daemon's actor tree (spec §4.2).
//!
//! Everything under this module is async, and everything under this module
//! obeys the actor discipline the rest of `a2a` is built to allow:
//!
//! * **One owner per piece of state.** `Arc<Mutex<…>>`/`Arc<RwLock<…>>` are
//!   forbidden here (source-scan-tested): state that two tasks need is reached
//!   by sending a message to the task that owns it, never by sharing a lock.
//! * **Bounded channels only.** Per-worker mailboxes are bounded at
//!   [`worker_actor::MAILBOX_CAPACITY`] (spec §4.5); a producer that outruns a
//!   consumer gets backpressure, and the one stream that cannot apply
//!   backpressure — the trace tail — drops the OLDEST and says so in the log.
//! * **Cancellation safety.** No `write_all` inside a `select!`, no
//!   `std::thread::sleep`: writes go through dedicated writer tasks, and
//!   blocking file work goes through `spawn_blocking`.
//!
//! [`worker_actor::WorkerActor`] is the sole writer to one worker's directory —
//! but only *alongside* Phase 1's CLI paths, which are still perfectly entitled
//! to reconcile the same worker. What makes "sole writer" true across both is
//! that every actor write takes the same `state.lock` a reconcile takes and
//! re-opens the `EventLog` inside it.

pub mod worker_actor;
