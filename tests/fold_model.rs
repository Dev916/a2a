use a2a::{events::*, fold::*, fsm::*, state::*};
use chrono::Utc;

fn lifecycle(kind: EventKind, fsm_payload: serde_json::Value, epoch: u32, seq: u64) -> Envelope {
    let mut e = Envelope::new(
        epoch,
        Transport::Exec,
        EventClass::Lifecycle,
        kind,
        fsm_payload,
        Utc::now(),
    );
    e.seq = seq;
    e
}
fn base(dir: &std::path::Path) -> WorkerSnapshot {
    WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir)
}

#[test]
fn fold_prefix_equals_incremental_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let seq = vec![
        lifecycle(
            EventKind::StateChange,
            serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}),
            1,
            1,
        ),
        lifecycle(
            EventKind::StateChange,
            serde_json::json!({"fsm":"TurnEnded","has_result":false,"has_question":true}),
            1,
            2,
        ),
        lifecycle(
            EventKind::StateChange,
            serde_json::json!({"fsm":"Send"}),
            1,
            3,
        ),
        lifecycle(
            EventKind::StateChange,
            serde_json::json!({"fsm":"TurnEnded","has_result":true,"has_question":false}),
            1,
            4,
        ),
    ];
    for k in 0..=seq.len() {
        let all_at_once = fold(base(tmp.path()), &seq[..k], Utc::now());
        let mut inc = base(tmp.path());
        for e in &seq[..k] {
            inc = fold(inc, std::slice::from_ref(e), Utc::now());
        }
        assert_eq!(all_at_once.state, inc.state, "prefix {k}");
        assert_eq!(
            all_at_once.last_applied_seq, inc.last_applied_seq,
            "prefix {k}"
        );
    }
}

#[test]
fn stale_epoch_events_are_fenced() {
    let tmp = tempfile::tempdir().unwrap();
    let mut snap = base(tmp.path());
    snap.epoch = 2;
    snap.state = WorkerState::Running;
    let stale = lifecycle(
        EventKind::StateChange,
        serde_json::json!({"fsm":"TurnEnded","has_result":true,"has_question":false}),
        1,
        snap.last_applied_seq + 1,
    );
    let after = fold(snap.clone(), &[stale], Utc::now());
    assert_eq!(
        after.state,
        WorkerState::Running,
        "stale-epoch completion must not apply"
    );
    assert_eq!(
        after.last_applied_seq,
        snap.last_applied_seq + 1,
        "seq watermark still advances"
    );
}

#[test]
fn reapplying_applied_seq_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let e = lifecycle(
        EventKind::StateChange,
        serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}),
        1,
        1,
    );
    let s1 = fold(base(tmp.path()), &[e.clone()], Utc::now());
    let s2 = fold(s1.clone(), &[e], Utc::now());
    assert_eq!(
        (s1.state, s1.last_applied_seq),
        (s2.state, s2.last_applied_seq)
    );
}

#[test]
fn snapshot_roundtrip_and_too_new() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("w");
    std::fs::create_dir_all(&dir).unwrap();
    let snap = base(&dir);
    save_snapshot(&dir, &snap).unwrap();
    let loaded = load_snapshot(&dir).unwrap();
    assert_eq!(loaded.schema_version, 1);
    assert_eq!(loaded.id, snap.id);
    // newer schema is read-only
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("state.json")).unwrap()).unwrap();
    v["schema_version"] = 2.into();
    std::fs::write(dir.join("state.json"), v.to_string()).unwrap();
    assert!(matches!(load_snapshot(&dir), Err(SnapshotError::TooNew(2))));
}

#[test]
fn recovery_table_cases() {
    let now = Utc::now();
    let tmp = tempfile::tempdir().unwrap();
    // (a) empty dir → Failed{NeverStarted}
    let d1 = tmp.path().join("impl-0814t1300-bbbb");
    std::fs::create_dir_all(&d1).unwrap();
    let r = recover(
        &d1,
        LivenessInput {
            process_alive: false,
        },
        now,
    )
    .unwrap();
    assert_eq!(
        (r.state, r.failure_class),
        (WorkerState::Failed, Some(FailureClass::NeverStarted))
    );
    // (b) running snapshot + dead process → Orphaned
    let d2 = tmp.path().join("impl-0814t1300-cccc");
    std::fs::create_dir_all(&d2).unwrap();
    let mut s = WorkerSnapshot::new_for_test("impl-0814t1300-cccc", &d2);
    s.state = WorkerState::Running;
    save_snapshot(&d2, &s).unwrap();
    let r = recover(
        &d2,
        LivenessInput {
            process_alive: false,
        },
        now,
    )
    .unwrap();
    assert_eq!(r.state, WorkerState::Orphaned);
    // (c) corrupt snapshot + events → rebuilt from the log
    let d3 = tmp.path().join("impl-0814t1300-dddd");
    std::fs::create_dir_all(&d3).unwrap();
    std::fs::write(d3.join("state.json"), b"{not json").unwrap();
    let mut log = a2a::store::EventLog::open(&d3).unwrap();
    let mut g = Envelope::new(
        1,
        Transport::Exec,
        EventClass::Lifecycle,
        EventKind::Spawned,
        serde_json::json!({"fsm":"Spawn","id":"impl-0814t1300-dddd","lane":"impl",
            "cwd":d3.to_str().unwrap(),"brief_sha":"x","sandbox":"workspace-write",
            "network":false,"model":"gpt-5.5","effort":"high","transport":"exec"}),
        Utc::now(),
    );
    log.append(&mut g).unwrap();
    let mut p = Envelope::new(
        1,
        Transport::Exec,
        EventClass::Lifecycle,
        EventKind::StateChange,
        serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}),
        Utc::now(),
    );
    log.append(&mut p).unwrap();
    let r = recover(
        &d3,
        LivenessInput {
            process_alive: true,
        },
        now,
    )
    .unwrap();
    assert_eq!(
        r.state,
        WorkerState::Running,
        "rebuilt from the log, snapshot discarded"
    );
    assert_eq!(
        r.lane,
        a2a::config::Lane::Impl,
        "identity seeded from the genesis event"
    );
}
