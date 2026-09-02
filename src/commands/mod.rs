//! One module per CLI subcommand; each `run(...)` composes the pure core with IO
//! and returns the machine payload that `main` wraps in the output envelope.

/// Owner scoping (spec §4.8) — the one policy every command shares, so it lives
/// at the command layer's root rather than in whichever command needed it first.
///
/// Two orchestrators on one machine is the ordinary case, and the rule that
/// keeps them apart is deliberately narrow:
///
/// * **Reads are never scoped.** `status`/`tail`/`result`/`wait` answer
///   questions about a worker; refusing to answer would buy nothing and cost
///   every cross-account debugging session.
/// * **Writes are scoped, and `--force` opts out.** A guard with no override is
///   a guard people work around by editing `state.json`.
/// * **`owner: None` matches everyone.** Every worker spawned before owners
///   existed has no owner on record, and treating "unknown" as "someone else's"
///   would turn the upgrade that adds this feature into a fleet-wide lockout.
///   That asymmetry is the whole reason the owner is `Option<String>` on the
///   snapshot rather than a `String` with an empty default.
pub mod owner {
    use crate::output::{AppError, ExitCode};
    use serde_json::json;

    /// Who this process is acting as: `A2A_OWNER` when it is set to something
    /// non-blank, `anon-<uid>` otherwise.
    ///
    /// `anon-<uid>` rather than `None` on purpose — an unset `A2A_OWNER` still
    /// produces a *stable* identity (every shell of the same user agrees), which
    /// is what makes the default useful rather than merely permissive.
    pub fn caller() -> String {
        std::env::var("A2A_OWNER")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| format!("anon-{}", nix::unistd::getuid().as_raw()))
    }

    /// Spawn-time precedence: `--owner` > `A2A_OWNER` > `anon-<uid>`.
    pub fn resolve(flag: Option<&str>) -> String {
        flag.map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .unwrap_or_else(caller)
    }

    /// May `caller` act on a worker owned by `owner`? Unowned means yes.
    pub fn permitted(owner: Option<&str>, caller: &str) -> bool {
        match owner {
            None => true, // spawned before owners existed: everybody's
            Some(o) => o == caller,
        }
    }

    /// The guard every mutating command runs before it touches anything.
    ///
    /// The refusal is a *static* precondition (exit 9): it is decided from the
    /// worker's own record, and no broker appearing or disappearing changes it.
    pub fn guard(id: &str, owner: Option<&str>, force: bool) -> Result<(), AppError> {
        guard_as(id, owner, &caller(), force)
    }

    /// [`guard`] for callers that already resolved their identity (`ls`/`gc`
    /// classify a whole fleet against one caller, and must not re-read the
    /// environment per worker).
    pub fn guard_as(
        id: &str,
        owner: Option<&str>,
        caller: &str,
        force: bool,
    ) -> Result<(), AppError> {
        if force || permitted(owner, caller) {
            return Ok(());
        }
        let owner = owner.unwrap_or_default();
        Err(AppError::new(
            "NOT_OWNER",
            ExitCode::Precondition,
            format!("worker {id} belongs to `{owner}`, and this caller is `{caller}`"),
        )
        .remedy(
            "re-run as that owner (`A2A_OWNER=<owner> a2a …`), or pass `--force` to act \
             across owners",
        )
        .data(json!({"id": id, "owner": owner, "caller": caller})))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn an_unowned_worker_is_open_to_everyone() {
            assert!(permitted(None, "alice"));
            assert!(permitted(None, "anon-501"));
            assert!(permitted(Some("alice"), "alice"));
            assert!(!permitted(Some("alice"), "bob"));
        }

        #[test]
        fn the_flag_outranks_the_environment_and_blanks_do_not_count() {
            assert_eq!(resolve(Some("alice")), "alice");
            assert_eq!(resolve(Some("  alice  ")), "alice");
            // A blank flag is not an owner; it falls through to the next layer.
            assert_eq!(resolve(Some("   ")), caller());
            assert_eq!(resolve(None), caller());
        }

        #[test]
        fn the_refusal_carries_branchable_detail_and_force_skips_it() {
            let err = guard_as("impl-0814t1300-aaaa", Some("alice"), "bob", false)
                .expect_err("bob may not touch alice's worker");
            assert_eq!(err.code, "NOT_OWNER");
            assert_eq!(err.exit, ExitCode::Precondition);
            let data = err.data.expect("machine-readable detail");
            assert_eq!(data["owner"], "alice");
            assert_eq!(data["caller"], "bob");
            assert!(guard_as("impl-0814t1300-aaaa", Some("alice"), "bob", true).is_ok());
            assert!(guard_as("impl-0814t1300-aaaa", None, "bob", false).is_ok());
        }
    }
}

pub mod broker_cmd;
pub mod config_roots;
pub mod config_show;
pub mod doctor;
pub mod gc;
pub mod gen_skills;
pub mod interrupt;
pub mod kill;
pub mod ls;
pub mod mesh_cmd;
pub mod respawn;
pub mod result;
pub mod resume;
pub mod send;
pub mod spawn;
pub mod status;
pub mod sync_cmd;
pub mod tail;
pub mod update;
pub mod verify;
pub mod wait;
