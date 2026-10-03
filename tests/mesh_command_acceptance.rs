// tests/mesh_command_acceptance.rs
//
// Task 9 of the Agent Command Channel (a2a-byd): the end-to-end acceptance for
// the whole channel — grant, sign, route, evaluate, render, report.
//
// Two legs, on purpose:
//
//   * HERMETIC (`#[test]`): two same-fleet homes wired through the file-layer
//     note transport. This is the leg a sandboxed worker can run, and it pins
//     the security invariant the spec is organized around — an imperative block
//     appears in the target's inject digest ONLY for a granted+verified sender,
//     while ambient notes in the SAME digest keep their "data, not
//     instructions" rendering.
//
//   * LIVE SOCKET (`#[tokio::test]`): the same directive over a real
//     `127.0.0.1:0` TCP socket, pinned TLS, a mutual mesh handshake and two
//     `run_link_session` gateways — the transport the file-layer leg stands in
//     for. It binds a port, so it is orchestrator/subagent-run (codex workers
//     cannot bind). It degrades to a printed SKIP rather than a failure when
//     the ambient sandbox denies the bind.

use a2a::config::Config;
use a2a::home::A2aHome;
use a2a::mesh::directive::{self, Directive, DirectiveOutcome, OutcomeVerdict};
use a2a::mesh::gateway::link::{self, LinkDirection, LinkSessionParams, LinkStateHandle};
use a2a::mesh::gateway::notes_wire::{self, NoteDeliveryTarget, SpoolPassReport};
use a2a::mesh::keys::{self, RootSet, SystemClock};
use a2a::mesh::wire::{
    accept_pinned, connect_pinned, handshake_acceptor, handshake_dialer, HandshakeIdentity,
    TlsIdentity,
};
use a2a::mesh::Note;
use assert_cmd::Command;
use chrono::Utc;
use rand::{rngs::StdRng, SeedableRng};
use serde_json::json;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// The one rendering the plane ever treats as an instruction (`src/mesh/notes.rs`).
const IMPERATIVE_HEADING: &str = "## AUTHENTICATED DIRECTIVE";
/// The unchanged ambient rendering every other observed message keeps.
const AMBIENT_MARKER: &str = "data, not instructions";
const INSTRUCTION: &str = "open the release checklist and report what is left";
const AMBIENT_TEXT: &str = "ordinary peer gossip that must stay ambient";
const A_SESSION: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const B_SESSION: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const AMBIENT_NOTE_ID: &str = "01K3KV00000000000000009001";

struct Fleet {
    _tmp: tempfile::TempDir,
    a: A2aHome,
    b: A2aHome,
    gw_a: String,
    gw_b: String,
}

/// One fleet, two enrolled gateway homes: A commands, B receives.
fn fleet() -> Fleet {
    let tmp = tempfile::tempdir().unwrap();
    let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
    let a = A2aHome::resolve(Some(&tmp.path().join("a"))).unwrap();
    let b = A2aHome::resolve(Some(&tmp.path().join("b"))).unwrap();
    keys::fleet_init(&root).unwrap();
    for home in [&a, &b] {
        fs::create_dir_all(home.root().join("mesh/fleet")).unwrap();
        fs::copy(
            root.root().join("mesh/fleet/root.pub"),
            home.root().join("mesh/fleet/root.pub"),
        )
        .unwrap();
    }
    let gw_a = enroll(&root, &a);
    let gw_b = enroll(&root, &b);
    Fleet {
        _tmp: tmp,
        a,
        b,
        gw_a,
        gw_b,
    }
}

fn enroll(root: &A2aHome, joiner: &A2aHome) -> String {
    let identity = keys::gateway_identity(joiner, false).unwrap();
    let token = keys::enroll_gateway_token(
        root,
        &identity.gateway_id,
        &keys::gateway_public_key_b64(joiner).unwrap(),
        &SystemClock,
    )
    .unwrap();
    keys::install_gateway_token(joiner, &token, &SystemClock).unwrap();
    identity.gateway_id
}

fn cfg_for(home: &A2aHome) -> Config {
    a2a::config::load(home).unwrap().0
}

fn cli(home: &A2aHome) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command
        .env("A2A_HOME", home.root())
        .env("A2A_BROKER_DISABLE", "1")
        .env_remove("A2A_JSON")
        .env_remove("CLAUDE_SESSION_ID")
        .env_remove("BEADS_ACTOR");
    command
}

fn run(home: &A2aHome, args: &[&str]) -> String {
    let stdout = cli(home)
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(stdout).unwrap()
}

/// `a2a mesh command grant` — the receiving operator's explicit consent.
fn grant(home: &A2aHome, peer: &str) {
    run(home, &["mesh", "command", "grant", peer]);
}

/// `a2a mesh command send` — mints, signs and spools; returns what was signed.
fn send_directive(home: &A2aHome, to: &str, instruction: &str) -> Directive {
    let path = home.root().join("instruction.txt");
    fs::write(&path, instruction).unwrap();
    cli(home)
        .args(["mesh", "command", "send", "--to", to])
        .arg("--instruction-file")
        .arg(&path)
        .assert()
        .success();
    spooled_directive(home, to)
}

fn spooled_directive(home: &A2aHome, to: &str) -> Directive {
    let mut found = Vec::new();
    for path in note_files(&home.root().join("mesh/outbound").join(to)) {
        let spooled = notes_wire::read_spooled(&path).unwrap();
        if spooled.note.kind == "directive" {
            found.push(
                serde_json::from_value::<Directive>(spooled.note.refs["directive"].clone())
                    .unwrap(),
            );
        }
    }
    assert_eq!(found.len(), 1, "exactly one spooled directive for {to}");
    assert!(
        !found[0].sig.is_empty(),
        "the routed directive carries a signature"
    );
    found.remove(0)
}

/// An ordinary peer note riding the same spool, so one digest carries both
/// renderings and the ambient default is proved unchanged.
fn spool_ambient_note(fleet: &Fleet, cfg: &Config) {
    let note = Note {
        note_id: Some(AMBIENT_NOTE_ID.to_string()),
        from: format!("claude:{A_SESSION}"),
        to: format!("claude:{B_SESSION}"),
        ts: Utc::now(),
        kind: "note".to_string(),
        text: AMBIENT_TEXT.to_string(),
        refs: json!({}),
        extra: serde_json::Map::new(),
    };
    notes_wire::spool_note(&fleet.a, cfg, &fleet.gw_a, &fleet.gw_b, note).unwrap();
}

fn deliver(
    from: &A2aHome,
    from_cfg: &Config,
    to_gateway: &str,
    to: &A2aHome,
    seed: u64,
) -> SpoolPassReport {
    notes_wire::deliver_spooled_once(
        from_cfg,
        from,
        NoteDeliveryTarget::Gateway {
            to_gateway,
            home: Some(to),
        },
        Utc::now(),
        &mut StdRng::seed_from_u64(seed),
    )
    .unwrap()
}

/// The orchestrator's next-turn digest, taken through the real CLI.
fn inject_digest(home: &A2aHome, session: &str) -> String {
    run(
        home,
        &["mesh", "inject", "--provider", "claude", "--session", session],
    )
}

fn note_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = match fs::read_dir(dir) {
        Ok(entries) => entries.flatten().map(|entry| entry.path()).collect(),
        Err(_) => Vec::<PathBuf>::new(),
    };
    files.retain(|path| {
        path.file_name()
            .map(|name| name.to_string_lossy().ends_with(".note.json"))
            .unwrap_or(false)
    });
    files.sort();
    files
}

/// Every directive-outcome note that reached this home, however it was routed.
fn outcome_notes(home: &A2aHome) -> Vec<DirectiveOutcome> {
    let mut outcomes = Vec::new();
    let Ok(peers) = fs::read_dir(home.root().join("mesh/peers")) else {
        return outcomes;
    };
    for peer in peers.flatten() {
        let inbox = peer.path().join("inbox");
        for dir in [inbox.clone(), inbox.join("read")] {
            for path in note_files(&dir) {
                let note: Note = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                if let Some(raw) = note.refs.get("directive_outcome") {
                    outcomes.push(serde_json::from_value(raw.clone()).unwrap());
                }
            }
        }
    }
    outcomes
}

#[test]
fn a_granted_directive_is_imperative_at_the_target_and_its_outcome_routes_back() {
    let fleet = fleet();
    let cfg_a = cfg_for(&fleet.a);
    let cfg_b = cfg_for(&fleet.b);

    // Gate 1: B's operator consents to being commanded by A.
    grant(&fleet.b, &fleet.gw_a);
    assert!(run(&fleet.b, &["mesh", "command", "list"]).contains(&fleet.gw_a));

    let directive = send_directive(&fleet.a, &fleet.gw_b, INSTRUCTION);
    spool_ambient_note(&fleet, &cfg_a);

    let routed = deliver(&fleet.a, &cfg_a, &fleet.gw_b, &fleet.b, 101);
    assert_eq!(routed.delivered, 2, "directive and ambient note both routed");
    assert_eq!(routed.rejected, 0);
    let pending = directive::pending(&fleet.b).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].directive_id, directive.directive_id);

    let digest = inject_digest(&fleet.b, B_SESSION);
    assert!(digest.contains(IMPERATIVE_HEADING), "{digest}");
    assert!(digest.contains(&fleet.gw_a), "{digest}");
    assert!(digest.contains(INSTRUCTION), "{digest}");
    // The narrow exception is narrow: the ambient block in the SAME digest is
    // untouched, and the imperative block leads it.
    assert!(digest.contains(AMBIENT_MARKER), "{digest}");
    assert!(digest.contains(AMBIENT_TEXT), "{digest}");
    assert!(
        digest.find(IMPERATIVE_HEADING) < digest.find(AMBIENT_MARKER),
        "{digest}"
    );

    let audit = run(&fleet.b, &["mesh", "command", "log"]);
    assert!(audit.contains(&directive.directive_id), "{audit}");
    assert!(audit.contains("\"verdict\":\"imperative\""), "{audit}");
    assert!(audit.contains(&fleet.gw_a), "{audit}");

    let reported = deliver(&fleet.b, &cfg_b, &fleet.gw_a, &fleet.a, 102);
    assert_eq!(reported.delivered, 1, "one outcome note routes back to A");
    let outcomes = outcome_notes(&fleet.a);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].directive_id, directive.directive_id);
    assert_eq!(outcomes[0].verdict, OutcomeVerdict::Acted);
}

#[test]
fn an_ungranted_directive_renders_no_imperative_content_and_is_audited_refused() {
    let fleet = fleet();
    let cfg_a = cfg_for(&fleet.a);
    let cfg_b = cfg_for(&fleet.b);

    // No grant: everything else about this directive is identical and valid.
    assert!(run(&fleet.b, &["mesh", "command", "list"]).trim().is_empty());

    let directive = send_directive(&fleet.a, &fleet.gw_b, INSTRUCTION);
    spool_ambient_note(&fleet, &cfg_a);
    assert_eq!(deliver(&fleet.a, &cfg_a, &fleet.gw_b, &fleet.b, 201).delivered, 2);

    let digest = inject_digest(&fleet.b, B_SESSION);
    // Ambient traffic still renders; the directive contributes nothing at all.
    assert!(digest.contains(AMBIENT_MARKER), "{digest}");
    assert!(digest.contains(AMBIENT_TEXT), "{digest}");
    assert!(!digest.contains(IMPERATIVE_HEADING), "{digest}");
    assert!(!digest.contains("AUTHENTICATED"), "{digest}");
    assert!(!digest.contains(INSTRUCTION), "{digest}");
    assert!(!digest.contains(&directive.directive_id), "{digest}");

    let audit = run(&fleet.b, &["mesh", "command", "log"]);
    assert!(audit.contains(&directive.directive_id), "{audit}");
    assert!(audit.contains("\"verdict\":\"refused\""), "{audit}");
    assert!(audit.contains("DIRECTIVE_UNGRANTED"), "{audit}");
    assert!(!audit.contains("\"verdict\":\"imperative\""), "{audit}");

    assert_eq!(deliver(&fleet.b, &cfg_b, &fleet.gw_a, &fleet.a, 202).delivered, 1);
    let outcomes = outcome_notes(&fleet.a);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].directive_id, directive.directive_id);
    assert_eq!(outcomes[0].verdict, OutcomeVerdict::Refused);
    assert_eq!(outcomes[0].reason.as_deref(), Some("DIRECTIVE_UNGRANTED"));
}

async fn wait_for(limit: Duration, label: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if predicate() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} did not happen within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_live_loopback_link_carries_a_granted_directive_and_routes_its_outcome_back() {
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            eprintln!("SKIP: sandbox denied the loopback bind for mesh_command_acceptance");
            return;
        }
        Err(error) => panic!("binding the loopback listener: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_fpr = tls.spki_fpr().to_string();

    let fleet = fleet();
    let cfg_a = cfg_for(&fleet.a);
    let cfg_b = cfg_for(&fleet.b);
    grant(&fleet.b, &fleet.gw_a);

    // B accepts on the real socket; A dials it. Both ends complete the mutual
    // mesh handshake against the fleet RootSet before any directive moves.
    let b_identity = HandshakeIdentity::new(
        keys::gateway_token(&fleet.b).unwrap(),
        keys::gateway_signing_key(&fleet.b).unwrap(),
    );
    let b_roots = RootSet::load(&fleet.b).unwrap();
    let acceptor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut side = accept_pinned(stream, tls).await.unwrap();
        let peer = handshake_acceptor(&mut side, &b_identity, &b_roots, &SystemClock)
            .await
            .unwrap();
        (side, peer)
    });

    let a_identity = HandshakeIdentity::new(
        keys::gateway_token(&fleet.a).unwrap(),
        keys::gateway_signing_key(&fleet.a).unwrap(),
    );
    let a_roots = RootSet::load(&fleet.a).unwrap();
    let mut dialed = connect_pinned(&format!("wss://{addr}/"), &cert_fpr)
        .await
        .unwrap();
    let seen_b = handshake_dialer(&mut dialed, &a_identity, &a_roots, &SystemClock)
        .await
        .unwrap();
    let (accepted, seen_a) = acceptor.await.unwrap();
    assert_eq!(seen_a.gateway_id, fleet.gw_a);
    assert_eq!(seen_b.gateway_id, fleet.gw_b);

    let token = CancellationToken::new();
    let a_state = LinkStateHandle::new(&fleet.a, Duration::from_secs(60)).unwrap();
    let b_state = LinkStateHandle::new(&fleet.b, Duration::from_secs(60)).unwrap();
    let a_task = tokio::spawn(link::run_link_session(
        dialed,
        LinkSessionParams::new(
            fleet.a.clone(),
            cfg_a.clone(),
            fleet.gw_a.clone(),
            fleet.gw_b.clone(),
            LinkDirection::Outbound,
            a_state.clone(),
            token.child_token(),
        ),
    ));
    let b_task = tokio::spawn(link::run_link_session(
        accepted,
        LinkSessionParams::new(
            fleet.b.clone(),
            cfg_b.clone(),
            fleet.gw_b.clone(),
            fleet.gw_a.clone(),
            LinkDirection::Inbound,
            b_state.clone(),
            token.child_token(),
        ),
    ));

    let live_a = a_state.clone();
    let live_b = b_state.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if live_a.is_live(&fleet.gw_b).await && live_b.is_live(&fleet.gw_a).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the live loopback link never registered both sessions"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Mint + sign through the real CLI, then push the frame across the socket.
    // Nothing but that socket joins the two homes: B has no pending directive
    // until the frame crosses it.
    assert!(addr.ip().is_loopback() && addr.port() != 0);
    assert!(directive::pending(&fleet.b).unwrap().is_empty());
    let directive = send_directive(&fleet.a, &fleet.gw_b, INSTRUCTION);
    let pushed = link::push_spooled_notes(
        &fleet.a,
        &cfg_a,
        &a_state,
        Utc::now(),
        &mut StdRng::seed_from_u64(301),
    )
    .await
    .unwrap();
    assert_eq!(pushed, 1, "the directive frame went onto the live link");

    wait_for(Duration::from_secs(5), "directive delivery across the link", || {
        directive::pending(&fleet.b).is_ok_and(|pending| pending.len() == 1)
    })
    .await;
    assert_eq!(
        directive::pending(&fleet.b).unwrap()[0].directive_id,
        directive.directive_id
    );

    let digest = inject_digest(&fleet.b, B_SESSION);
    assert!(digest.contains(IMPERATIVE_HEADING), "{digest}");
    assert!(digest.contains(&fleet.gw_a), "{digest}");
    assert!(digest.contains(INSTRUCTION), "{digest}");
    let audit = run(&fleet.b, &["mesh", "command", "log"]);
    assert!(audit.contains(&directive.directive_id), "{audit}");
    assert!(audit.contains("\"verdict\":\"imperative\""), "{audit}");

    let reported = link::push_spooled_notes(
        &fleet.b,
        &cfg_b,
        &b_state,
        Utc::now(),
        &mut StdRng::seed_from_u64(302),
    )
    .await
    .unwrap();
    assert_eq!(reported, 1, "the outcome note went back over the same link");
    wait_for(Duration::from_secs(5), "outcome delivery back to A", || {
        outcome_notes(&fleet.a).len() == 1
    })
    .await;
    let outcomes = outcome_notes(&fleet.a);
    assert_eq!(outcomes[0].directive_id, directive.directive_id);
    assert_eq!(outcomes[0].verdict, OutcomeVerdict::Acted);

    token.cancel();
    a_task.await.unwrap().unwrap();
    b_task.await.unwrap().unwrap();
}
