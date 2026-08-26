use a2a::store;

#[test]
fn write_atomic_never_exposes_partial_content() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("state.json");
    store::write_atomic(&p, br#"{"v":1}"#).unwrap();
    store::write_atomic(&p, br#"{"v":2,"more":"data"}"#).unwrap();
    assert_eq!(
        std::fs::read_to_string(&p).unwrap(),
        r#"{"v":2,"more":"data"}"#
    );
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name() != "state.json")
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp files left behind: {leftovers:?}"
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn event_log_seq_monotonic_and_torn_tail_tolerated() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = store::EventLog::open(tmp.path()).unwrap();
    for _ in 0..3 {
        let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
        log.append(&mut e).unwrap();
    }
    let path = tmp.path().join("events.jsonl");
    let full = std::fs::read(&path).unwrap();
    let last_line_start = full[..full.len() - 1]
        .iter()
        .rposition(|&b| b == b'\n')
        .map(|i| i + 1)
        .unwrap();
    // torn strictly inside the final line (cut == full.len()-1 merely strips the trailing
    // newline, leaving a COMPLETE parseable line — asserted separately below)
    for cut in last_line_start + 1..full.len() - 1 {
        std::fs::write(&path, &full[..cut]).unwrap();
        let evs = store::read_events(tmp.path(), 0).unwrap();
        assert_eq!(
            evs.len(),
            2,
            "torn tail at {cut} must drop only the torn line"
        );
        assert_eq!(evs.last().unwrap().seq, 2);
    }
    // newline-less but complete final line IS returned
    std::fs::write(&path, &full[..full.len() - 1]).unwrap();
    assert_eq!(store::read_events(tmp.path(), 0).unwrap().len(), 3);
}

#[test]
fn open_repairs_torn_tail_so_later_appends_stay_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = store::EventLog::open(tmp.path()).unwrap();
    for _ in 0..3 {
        let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
        log.append(&mut e).unwrap();
    }
    drop(log);
    let path = tmp.path().join("events.jsonl");
    let full = std::fs::read(&path).unwrap();
    std::fs::write(&path, &full[..full.len() - 3]).unwrap(); // torn mid-line-3
    let mut log = store::EventLog::open(tmp.path()).unwrap(); // must repair: truncate to end of line 2
    let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
    assert_eq!(log.append(&mut e).unwrap(), 3); // seq continues from last COMPLETE line
    let evs = store::read_events(tmp.path(), 0).unwrap();
    assert_eq!(
        evs.len(),
        3,
        "post-repair append must be readable, not glued to the fragment"
    );
    assert_eq!(evs.last().unwrap().seq, 3);
}
