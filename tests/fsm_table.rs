use a2a::fsm::*;

fn all_states() -> Vec<WorkerState> {
    use WorkerState::*;
    vec![
        Spawned,
        Queued,
        Running,
        AwaitingInput,
        Escalated,
        Done,
        Failed,
        Cancelled,
        Orphaned,
    ]
}
fn all_events() -> Vec<FsmEvent> {
    use FsmEvent::*;
    vec![
        Spawn {
            id: "impl-0814t1300-aaaa".into(),
        },
        Admit,
        ProcessStarted {
            pid: 1,
            start_witness: "w".into(),
        },
        SessionIdentified {
            session_id: "s".into(),
            rollout_path: None,
        },
        TurnEnded {
            has_result: true,
            has_question: false,
        },
        TurnEnded {
            has_result: false,
            has_question: true,
        },
        TurnEnded {
            has_result: false,
            has_question: false,
        },
        ResultInvalid { detail: "d".into() },
        ProcessExited { code: Some(1) },
        SessionMissing,
        Timeout {
            kind: TimeoutKind::Start,
        },
        Timeout {
            kind: TimeoutKind::Task,
        },
        Timeout {
            kind: TimeoutKind::Blocked,
        },
        Interrupt,
        Kill,
        OrphanDetected,
        Resume {
            answer_queued: false,
        },
        Send,
    ]
}

#[test]
fn totality_no_catch_all_arm_in_source() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/fsm.rs"),
    )
    .unwrap();
    let body = src.split("pub fn step").nth(1).expect("step fn present");
    for forbidden in ["(_, _)", "_ =>"] {
        assert!(
            !body.contains(forbidden),
            "step() must enumerate every (state, event) pair explicitly; found `{forbidden}`"
        );
    }
}

#[test]
fn every_pair_is_decided_without_panicking() {
    for s in all_states() {
        for e in all_events() {
            let _ = step(s, None, &e); // Ok or Rejected; a panic fails the test
        }
    }
}

#[test]
fn terminal_states_absorb_everything_but_resume() {
    for s in [
        WorkerState::Done,
        WorkerState::Failed,
        WorkerState::Cancelled,
    ] {
        for e in all_events() {
            let r = step(s, None, &e);
            match e {
                FsmEvent::Resume { .. } => {
                    let t = r.expect("resume must be legal on terminal");
                    assert_eq!(t.state, WorkerState::Running);
                    assert!(t.epoch_bump);
                }
                _ => assert!(r.is_err(), "{s:?} must reject {e:?}"),
            }
        }
    }
}

#[test]
fn spawn_lifecycle_rows() {
    let t = step(
        WorkerState::Spawned,
        None,
        &FsmEvent::ProcessStarted {
            pid: 9,
            start_witness: "w".into(),
        },
    )
    .unwrap();
    assert_eq!(t.state, WorkerState::Running);
    let t = step(
        WorkerState::Spawned,
        None,
        &FsmEvent::Timeout {
            kind: TimeoutKind::Start,
        },
    )
    .unwrap();
    assert_eq!(
        (t.state, t.failure),
        (WorkerState::Failed, Some(FailureClass::NeverStarted))
    );
    let t = step(
        WorkerState::Spawned,
        None,
        &FsmEvent::ProcessExited { code: Some(1) },
    )
    .unwrap();
    assert_eq!(
        (t.state, t.failure),
        (WorkerState::Failed, Some(FailureClass::NeverStarted))
    );
}

#[test]
fn kill_is_cancelled_never_failed_and_send_queues_while_running() {
    for s in [
        WorkerState::Running,
        WorkerState::AwaitingInput,
        WorkerState::Orphaned,
    ] {
        let t = step(s, None, &FsmEvent::Kill).unwrap();
        assert_eq!(t.state, WorkerState::Cancelled);
        assert!(t.failure.is_none());
    }
    let t = step(WorkerState::Running, None, &FsmEvent::Send).unwrap();
    assert_eq!(t.state, WorkerState::Running);
    assert!(
        t.actions.is_empty(),
        "send-while-running queues; no delivery action"
    );
    let t = step(
        WorkerState::AwaitingInput,
        Some(StateReason::Blocked),
        &FsmEvent::Send,
    )
    .unwrap();
    assert!(t.actions.contains(&Action::DeliverInbox));
}

#[test]
fn blocked_timeout_escalates_and_session_missing_is_distinct() {
    let t = step(
        WorkerState::AwaitingInput,
        Some(StateReason::Blocked),
        &FsmEvent::Timeout {
            kind: TimeoutKind::Blocked,
        },
    )
    .unwrap();
    assert_eq!(t.state, WorkerState::Escalated);
    let t = step(WorkerState::Running, None, &FsmEvent::SessionMissing).unwrap();
    assert_eq!(
        (t.state, t.failure),
        (WorkerState::Failed, Some(FailureClass::SessionMissing))
    );
}

#[test]
fn every_state_reaches_terminal() {
    use std::collections::HashSet;
    for start in all_states() {
        let mut seen = HashSet::new();
        let mut stack = vec![start];
        let mut reached_terminal = false;
        while let Some(s) = stack.pop() {
            if !seen.insert(s) {
                continue;
            }
            if s.is_terminal() {
                reached_terminal = true;
                break;
            }
            for e in all_events() {
                if let Ok(t) = step(s, None, &e) {
                    stack.push(t.state);
                }
            }
        }
        assert!(reached_terminal, "{start:?} cannot reach a terminal state");
    }
}
