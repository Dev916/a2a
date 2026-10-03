//! Find the pid of the agent process (Claude Code or Codex) that this `a2a`
//! invocation is running under, by walking up the process tree. Recorded in
//! presence so an operator can correlate a mesh session with a real process.

use std::process::Command;

const MAX_DEPTH: usize = 16;

/// One line of `ps -o ppid=,comm= -p <pid>`: the parent pid, then the command.
pub fn parse_ps_line(line: &str) -> Option<(u32, String)> {
    let line = line.trim();
    let (ppid, comm) = line.split_once(char::is_whitespace)?;
    let comm = comm.trim();
    if comm.is_empty() {
        return None;
    }
    Some((ppid.parse().ok()?, comm.to_string()))
}

/// Is `comm` the agent binary for this provider? Matches the basename exactly.
/// `_claude` is the real Claude Code binary when it runs behind the
/// claude-acct shim (`~/.local/bin/claude` is a wrapper script there).
pub fn is_agent(comm: &str, provider: &str) -> bool {
    let names: &[&str] = match provider {
        "claude" => &["claude", "_claude"],
        "codex" => &["codex"],
        _ => return false,
    };
    comm.rsplit('/').next().is_some_and(|base| names.contains(&base))
}

/// Walk ancestors starting at `start`; return the first one that is the agent.
/// `lookup(pid)` returns that pid's (parent pid, command).
pub fn find_from(
    start: u32,
    provider: &str,
    lookup: impl Fn(u32) -> Option<(u32, String)>,
) -> Option<u32> {
    let mut pid = start;
    for _ in 0..MAX_DEPTH {
        if pid <= 1 {
            return None;
        }
        let (ppid, comm) = lookup(pid)?;
        if is_agent(&comm, provider) {
            return Some(pid);
        }
        if ppid == pid {
            return None;
        }
        pid = ppid;
    }
    None
}

fn ps_lookup(pid: u32) -> Option<(u32, String)> {
    let out = Command::new("ps")
        .args(["-o", "ppid=,comm=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_ps_line(String::from_utf8_lossy(&out.stdout).lines().next()?)
}

/// The agent process this invocation runs under, if any. `A2A_SESSION_PID`
/// overrides the walk (for harnesses whose binary has another name). Returns
/// None where `ps` is unavailable (e.g. inside the codex sandbox) — callers
/// must treat the pid as optional.
pub fn find_session_pid(provider: &str) -> Option<u32> {
    if let Ok(explicit) = std::env::var("A2A_SESSION_PID") {
        return explicit.trim().parse().ok();
    }
    find_from(std::os::unix::process::parent_id(), provider, ps_lookup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ps_output() {
        assert_eq!(parse_ps_line("  812 /Users/x/.local/bin/claude"), Some((812, "/Users/x/.local/bin/claude".to_string())));
        assert_eq!(parse_ps_line("1 /sbin/launchd"), Some((1, "/sbin/launchd".to_string())));
        assert_eq!(parse_ps_line("  77 /Applications/My App/bin/codex"), Some((77, "/Applications/My App/bin/codex".to_string())));
        assert_eq!(parse_ps_line(""), None);
        assert_eq!(parse_ps_line("notapid zsh"), None);
    }

    #[test]
    fn matches_the_agent_binary_by_basename() {
        assert!(is_agent("/Users/x/.local/bin/claude", "claude"));
        assert!(is_agent("claude", "claude"));
        assert!(is_agent("/opt/homebrew/bin/codex", "codex"));
        assert!(!is_agent("/bin/zsh", "claude"));
        assert!(!is_agent("/usr/bin/claude-helper-not", "codex"));
        assert!(!is_agent("claude", "user"), "bare-terminal users have no agent process");
    }

    #[test]
    fn matches_the_shimmed_claude_binary_exactly() {
        // Observed on this machine: the claude-acct shim execs `_claude`.
        assert!(is_agent("/Users/x/.local/bin/_claude", "claude"));
        assert!(!is_agent("/Users/x/.local/bin/_claude", "codex"));
        assert!(!is_agent("/usr/bin/claude-helper", "claude"), "no substring matches");
        assert!(!is_agent("/usr/bin/__claude", "claude"));
        assert!(!is_agent("/usr/bin/_codex", "codex"));
    }

    #[test]
    fn walks_up_to_the_nearest_agent_ancestor() {
        // 500 (a2a's parent: sh) -> 400 (claude) -> 300 (zsh) -> 1
        let tree = |pid: u32| match pid {
            500 => Some((400, "/bin/sh".to_string())),
            400 => Some((300, "/Users/x/.local/bin/claude".to_string())),
            300 => Some((1, "/bin/zsh".to_string())),
            _ => None,
        };
        assert_eq!(find_from(500, "claude", tree), Some(400));
        assert_eq!(find_from(300, "claude", tree), None, "no agent above the shell");
        assert_eq!(find_from(500, "codex", tree), None);
    }

    #[test]
    fn a_cycle_or_deep_tree_terminates() {
        let looped = |pid: u32| Some((pid, "/bin/sh".to_string()));
        assert_eq!(find_from(42, "claude", looped), None);
    }
}
