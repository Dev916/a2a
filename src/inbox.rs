//! Operator → worker messaging: the `inbox/` queue and its turn-boundary
//! delivery (spec §4.3, §4.5).
//!
//! The exec transport has no mid-turn channel, so steering is a **queue plus a
//! watermark**, not a pipe:
//!
//! ```text
//! a2a send ──▶ inbox/<seq>-<ms>-<rand>.md        (durable, ordered, 0600)
//!                     │  turn boundary
//!                     ▼
//!            prompt.md ──stdin──▶ codex exec resume <session>
//!                     │
//!                     ▼
//!            inbox/delivered/<same name>          + last_delivered_seq := seq
//! ```
//!
//! Three properties this module owns:
//!
//! * **Ordering is in the filename.** `<seq:06>-<epoch_ms>-<rand8>` sorts
//!   lexicographically in seq order, so two rapid sends are delivered in the
//!   order they were queued no matter how the directory enumerates.
//! * **At-least-once, deduped by watermark.** The child is spawned FIRST, the
//!   files are moved to `delivered/` second, and the caller records the new
//!   watermark last. A crash anywhere in that sequence can only re-deliver a
//!   message the worker already saw (which `last_delivered_seq` then filters),
//!   never silently drop one.
//! * **The message never enters argv.** Delivery writes the concatenated batch
//!   to the worker's 0600 `prompt.md` and hands that file to the child as
//!   stdin — the same rule `spawn` follows, so a message is never visible in
//!   `ps` (spec §5).
//!
//! Delivery reuses the **current** epoch on purpose: answering a question is a
//! continuation of the same run, not a new one, so it appends to the epoch's
//! existing `exec.<epoch>.jsonl` (the shim opens it `O_APPEND`) and the ingest
//! watermark keeps indexing into one stream. `a2a resume` — a deliberate
//! restart — is the operation that bumps the epoch (Task 17).
//!
//! Delivery-env policy: `spawn_detached` inherits the *calling* process's
//! environment (after the §4.2 scrub). Spawn-time environment is deliberately
//! not persisted — a2a never stores a worker's env, so a resume carries
//! whatever the operator's shell carries.

use crate::config::Config;
use crate::exec::{self, SpawnSpec};
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::output::{AppError, ExitCode};
use crate::state::WorkerSnapshot;
use crate::store::write_atomic;
use chrono::Utc;
use rand::Rng;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Queue directory under a worker dir, and the acknowledgement subdirectory a
/// consumed message is moved into.
pub const INBOX: &str = "inbox";
pub const DELIVERED: &str = "delivered";

/// What a queued message is for. `steer` is advisory mid-flight guidance;
/// `answer` responds to an `outbox/<seq>.question.json`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MsgKind {
    Steer,
    Answer,
}

impl MsgKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MsgKind::Steer => "steer",
            MsgKind::Answer => "answer",
        }
    }
}

/// What one delivery did, for the caller's WAL record.
///
/// The pid matters: a delivery replaces the worker's process while the epoch
/// stays the same, so a snapshot that kept the *spawn's* pid would aim a later
/// `a2a kill` at a corpse. The witness makes that safe but inert; recording the
/// new pair makes it correct.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// New `last_delivered_seq`.
    pub watermark: u64,
    /// Seqs carried by this delivery, in the order they were concatenated.
    pub seqs: Vec<u64>,
    pub pid: u32,
    pub start_witness: String,
}

/// Write `inbox/<seq:06>-<epoch_ms>-<rand8 hex>.md` with YAML front matter
/// `{from, ts, kind}` and return the allocated seq.
///
/// The seq is allocated across BOTH `inbox/` and `inbox/delivered/`, so an
/// acknowledged message never has its number reused — the watermark would
/// otherwise swallow a new message the moment it was queued.
pub fn queue_message(
    worker_dir: &Path,
    from: &str,
    kind: MsgKind,
    body: &str,
) -> std::io::Result<u64> {
    let inbox = worker_dir.join(INBOX);
    create_dir_0700(&inbox.join(DELIVERED))?; // recursive: creates inbox/ too
    let seq = next_seq(&inbox);
    let now = Utc::now();
    let name = format!(
        "{seq:06}-{}-{}.md",
        now.timestamp_millis().max(0),
        rand8_hex()
    );
    let mut f = write_new_0600(&inbox.join(name))?;
    write!(
        f,
        "---\nfrom: {from}\nts: {}\nkind: {}\n---\n\n{}\n",
        now.to_rfc3339(),
        kind.as_str(),
        body.trim_end()
    )?;
    f.sync_all()
        // The message is the whole point of the command: it must be on disk
        // before `send` claims to have queued it.
        .map(|_| seq)
}

/// Every queued-but-undelivered message with `seq > watermark`, in seq order,
/// as `(seq, path, body)` — body is the text after the front matter.
pub fn undelivered(
    worker_dir: &Path,
    watermark: u64,
) -> std::io::Result<Vec<(u64, PathBuf, String)>> {
    let inbox = worker_dir.join(INBOX);
    let entries = match std::fs::read_dir(&inbox) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        // `delivered/` is a directory and is skipped here by construction.
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(seq) = seq_of(&entry.file_name().to_string_lossy()) else {
            continue; // not one of ours
        };
        if seq <= watermark {
            continue; // already acknowledged
        }
        let text = std::fs::read_to_string(entry.path())?;
        out.push((seq, entry.path(), strip_front_matter(&text)));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Deliver every undelivered message as ONE resumed turn; returns the new
/// watermark. See [`deliver`] for the full contract — this is the signature the
/// plan pins, and it is a thin wrapper.
pub fn deliver_pending(
    home: &A2aHome,
    cfg: &Config,
    snap: &WorkerSnapshot,
) -> Result<u64, AppError> {
    deliver(&home.worker_dir(&snap.id), cfg, snap).map(|d| d.watermark)
}

/// Concatenate every undelivered message (seq order) into `<worker>/prompt.md`,
/// spawn a detached `codex exec resume <session-id>` in the CURRENT epoch, move
/// each consumed file into `inbox/delivered/`, and report what was delivered.
///
/// Precondition: the worker has a recorded session id. Without one there is
/// nothing to resume, so this is a typed `SESSION_UNKNOWN` (exit 9) rather than
/// a spawn that would silently start a *new* conversation.
///
/// A caller with nothing pending gets `Ok` with an empty [`Delivery::seqs`] and
/// the watermark unchanged — delivery is idempotent, never an error.
pub fn deliver(
    worker_dir: &Path,
    cfg: &Config,
    snap: &WorkerSnapshot,
) -> Result<Delivery, AppError> {
    let pending = undelivered(worker_dir, snap.last_delivered_seq)
        .map_err(|e| io_err("reading the inbox", e))?;
    if pending.is_empty() {
        return Ok(Delivery {
            watermark: snap.last_delivered_seq,
            seqs: Vec::new(),
            pid: snap.pid.unwrap_or(0),
            start_witness: snap.pid_start_witness.clone().unwrap_or_default(),
        });
    }
    let session = snap.session_id.as_deref().ok_or_else(session_unknown)?;

    // The batch travels as a FILE, wired to the child's stdin — never argv.
    let prompt_file = worker_dir.join("prompt.md");
    write_atomic(&prompt_file, render(&pending).as_bytes())
        .map_err(|e| io_err("writing prompt.md", e))?;

    let codex_bin = cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    let spawned = exec::spawn_detached(&SpawnSpec {
        codex_bin: &codex_bin,
        cwd: &snap.cwd,
        worker_dir,
        // Same-epoch continuation: the shim appends to this epoch's stream.
        epoch: snap.epoch,
        sandbox: snap.sandbox,
        network: snap.network,
        model: &snap.model,
        effort: &snap.effort,
        schema_path: snap.schema_path.as_deref(),
        prompt_file: &prompt_file,
        resume_session: Some(session),
        env: &[],
    })?;

    // Acknowledge only what we actually handed over. Moving AFTER the spawn is
    // what makes the queue at-least-once: a crash here re-delivers, and the
    // watermark the caller records is what dedupes.
    let delivered_dir = worker_dir.join(INBOX).join(DELIVERED);
    create_dir_0700(&delivered_dir).map_err(|e| io_err("creating inbox/delivered", e))?;
    let mut watermark = snap.last_delivered_seq;
    let mut seqs = Vec::with_capacity(pending.len());
    for (seq, path, _) in &pending {
        if let Some(name) = path.file_name() {
            std::fs::rename(path, delivered_dir.join(name))
                .map_err(|e| io_err("acknowledging a delivered message", e))?;
        }
        watermark = watermark.max(*seq);
        seqs.push(*seq);
    }
    Ok(Delivery {
        watermark,
        seqs,
        pid: spawned.pid,
        start_witness: spawned.start_witness,
    })
}

fn session_unknown() -> AppError {
    AppError::new(
        "SESSION_UNKNOWN",
        ExitCode::Precondition,
        "no codex session id was ever recorded for this worker, so there is nothing to resume \
         — the message stays queued and will be delivered once a session exists",
    )
    .remedy("check `a2a tail <id>` for a session event; a worker that never started must be re-spawned")
}

/// One block per message, tagged with its seq so the worker can tell a batch of
/// two apart from one long message.
///
/// `pub(crate)` because the app-server transport batches the same way: `send`
/// hands the broker the rendered backlog rather than only the newest message,
/// so `--queue-only` staging means the same thing on both transports.
pub(crate) fn render(pending: &[(u64, PathBuf, String)]) -> String {
    let mut out = String::new();
    for (i, (seq, _, body)) in pending.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("[a2a message {seq}]\n{}\n", body.trim_end()));
    }
    out
}

/// Next free seq across the queue AND the acknowledged messages (seqs are
/// never reused). Unreadable directories yield 1 — a worker whose inbox we
/// cannot enumerate still gets its first message.
fn next_seq(inbox: &Path) -> u64 {
    let highest = |dir: PathBuf| -> u64 {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| seq_of(&e.file_name().to_string_lossy()))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    };
    highest(inbox.to_path_buf()).max(highest(inbox.join(DELIVERED))) + 1
}

/// The leading zero-padded seq of `<seq>-<ms>-<rand>.md`.
fn seq_of(name: &str) -> Option<u64> {
    if !name.ends_with(".md") {
        return None;
    }
    name.split('-').next()?.parse::<u64>().ok()
}

/// The body after a leading `---` YAML front-matter block. A file without one
/// is all body — a hand-dropped message must still be deliverable.
fn strip_front_matter(text: &str) -> String {
    let rest = match text.strip_prefix("---\n") {
        Some(r) => r,
        None => return text.to_string(),
    };
    match rest.split_once("\n---\n") {
        Some((_fm, body)) => body.trim_start_matches('\n').to_string(),
        None => text.to_string(),
    }
}

fn rand8_hex() -> String {
    format!("{:08x}", rand::thread_rng().gen::<u32>())
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn queued_names_sort_lexicographically_in_seq_order() {
        let tmp = tempfile::tempdir().unwrap();
        let a = queue_message(tmp.path(), "orchestrator", MsgKind::Answer, "one").unwrap();
        let b = queue_message(tmp.path(), "orchestrator", MsgKind::Steer, "two").unwrap();
        assert_eq!((a, b), (1, 2));
        let mut names: Vec<String> = std::fs::read_dir(tmp.path().join(INBOX))
            .unwrap()
            .flatten()
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(seq_of(&names[0]), Some(1));
        assert_eq!(seq_of(&names[1]), Some(2));
        // and every message is 0600 like every other worker file (spec §4.5)
        for n in &names {
            let mode = std::fs::metadata(tmp.path().join(INBOX).join(n))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "{n}");
        }
    }

    #[test]
    fn front_matter_records_from_ts_and_kind_and_is_stripped_from_the_body() {
        let tmp = tempfile::tempdir().unwrap();
        queue_message(tmp.path(), "mike", MsgKind::Answer, "use option B").unwrap();
        let pending = undelivered(tmp.path(), 0).unwrap();
        assert_eq!(pending.len(), 1);
        let raw = std::fs::read_to_string(&pending[0].1).unwrap();
        assert!(raw.contains("from: mike"), "{raw}");
        assert!(raw.contains("kind: answer"), "{raw}");
        assert!(raw.contains("ts: "), "{raw}");
        assert_eq!(pending[0].2.trim(), "use option B", "body only");
    }

    #[test]
    fn undelivered_filters_by_watermark_and_orders_by_seq() {
        let tmp = tempfile::tempdir().unwrap();
        for body in ["one", "two", "three"] {
            queue_message(tmp.path(), "o", MsgKind::Steer, body).unwrap();
        }
        let all = undelivered(tmp.path(), 0).unwrap();
        assert_eq!(
            all.iter().map(|(s, _, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(all[0].2.trim(), "one");
        let tail = undelivered(tmp.path(), 2).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].0, 3);
        assert!(undelivered(tmp.path(), 99).unwrap().is_empty());
    }

    #[test]
    fn seqs_are_never_reused_once_acknowledged() {
        let tmp = tempfile::tempdir().unwrap();
        queue_message(tmp.path(), "o", MsgKind::Answer, "one").unwrap();
        // simulate an acknowledged delivery
        let (_, path, _) = undelivered(tmp.path(), 0).unwrap().remove(0);
        let name = path.file_name().unwrap().to_owned();
        std::fs::rename(&path, tmp.path().join(INBOX).join(DELIVERED).join(name)).unwrap();
        assert_eq!(
            queue_message(tmp.path(), "o", MsgKind::Answer, "two").unwrap(),
            2,
            "an acknowledged seq must not be handed out again"
        );
    }

    #[test]
    fn a_batch_renders_in_seq_order_with_every_body_present() {
        let tmp = tempfile::tempdir().unwrap();
        queue_message(tmp.path(), "o", MsgKind::Answer, "part one").unwrap();
        queue_message(tmp.path(), "o", MsgKind::Answer, "part two").unwrap();
        let text = render(&undelivered(tmp.path(), 0).unwrap());
        let (p1, p2) = (
            text.find("part one").expect("first message"),
            text.find("part two").expect("second message"),
        );
        assert!(p1 < p2, "{text}");
    }

    #[test]
    fn a_message_without_front_matter_is_all_body() {
        assert_eq!(strip_front_matter("just text\n"), "just text\n");
        assert_eq!(
            strip_front_matter("---\nfrom: o\n---\n\nhello\n"),
            "hello\n"
        );
        // an unterminated block is not front matter; nothing is dropped
        assert_eq!(strip_front_matter("---\nfrom: o\n"), "---\nfrom: o\n");
    }

    #[test]
    fn only_our_filenames_are_treated_as_messages() {
        assert_eq!(seq_of("000007-1755000000000-deadbeef.md"), Some(7));
        assert_eq!(seq_of("000007-1755000000000-deadbeef.txt"), None);
        assert_eq!(seq_of("notes.md"), None);
        assert_eq!(seq_of("delivered"), None);
    }

    #[test]
    fn delivering_an_empty_queue_is_a_no_op_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let (cfg, _, _) = crate::config::load(&home).unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.last_delivered_seq = 4;
        snap.session_id = None; // no session, but also nothing to deliver
        let d = deliver(tmp.path(), &cfg, &snap).unwrap();
        assert_eq!(d.watermark, 4);
        assert!(d.seqs.is_empty());
    }

    #[test]
    fn a_worker_with_no_session_cannot_be_delivered_to() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let (cfg, _, _) = crate::config::load(&home).unwrap();
        let snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        queue_message(tmp.path(), "o", MsgKind::Answer, "hello").unwrap();
        let err = deliver(tmp.path(), &cfg, &snap).unwrap_err();
        assert_eq!(err.code, "SESSION_UNKNOWN");
        assert_eq!(err.exit, ExitCode::Precondition);
        // and the message is still queued — a failed delivery loses nothing
        assert_eq!(undelivered(tmp.path(), 0).unwrap().len(), 1);
    }
}
