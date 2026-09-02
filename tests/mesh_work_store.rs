mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::work_store::{self, WorkSource};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &Path, joiner: &Path) {
    fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome, clock: &SystemClock) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, clock).unwrap();
    install_gateway_token(joiner, &token, clock).unwrap();
    identity.gateway_id
}

fn unsigned(work_id: &str, addressing: WorkAddressing) -> UnsignedWorkEnvelope {
    UnsignedWorkEnvelope {
        work_id: work_id.to_string(),
        session: "codex:session-123".to_string(),
        addressing,
        repo: WorkRepo {
            remote_url: "https://github.com/example/project.git".to_string(),
            base_ref: "main".to_string(),
        },
        title: "Implement mesh handoff".to_string(),
        brief_text: "---\nlane: impl\n---\nDo the task.".to_string(),
        acceptance: "focused test passes".to_string(),
        ttl_days: 7,
        max_bytes: 262_144,
    }
}

fn verified(
    work_id: &str,
    source: WorkSource,
) -> (
    tempfile::TempDir,
    A2aHome,
    a2a::mesh::work::VerifiedEnvelope,
) {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let clock = SystemClock;
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    enroll_joiner(&root, &sender, &clock);
    let receiver_gateway = enroll_joiner(&root, &receiver, &clock);
    let addressing = match source {
        WorkSource::Directed => WorkAddressing::Directed(receiver_gateway),
        WorkSource::Pool => WorkAddressing::Pool,
    };
    let env = sign_envelope(&sender, unsigned(work_id, addressing), &clock).unwrap();
    let verified = verify_envelope(&receiver, &env, &clock).unwrap();
    (tmp, receiver, verified)
}

#[test]
fn store_offer_lists_loads_and_removes_directed_and_pool_work_with_0600_files() {
    let (_tmp, home, directed) = verified("01HZ7P7Q0V0V7G01J4J8R8M2Q0", WorkSource::Directed);
    let directed_path =
        work_store::store_offer(&home, directed.clone(), WorkSource::Directed).unwrap();
    assert_eq!(
        directed_path,
        home.root()
            .join("work")
            .join("inbox")
            .join("01HZ7P7Q0V0V7G01J4J8R8M2Q0.json")
    );
    assert_eq!(
        fs::metadata(&directed_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let loaded = work_store::load(&home, "01HZ7P7Q0V0V7G01J4J8R8M2Q0")
        .unwrap()
        .expect("directed work is loadable");
    assert_eq!(loaded.source, WorkSource::Directed);
    assert_eq!(loaded.verified.envelope.work_id, "01HZ7P7Q0V0V7G01J4J8R8M2Q0");

    let (_pool_tmp, pool_home, pool) = verified("01HZ7P7Q0V0V7G01J4J8R8M2Q1", WorkSource::Pool);
    let pool_path = work_store::store_offer(&pool_home, pool, WorkSource::Pool).unwrap();
    assert_eq!(
        pool_path,
        pool_home
            .root()
            .join("work")
            .join("pool")
            .join("01HZ7P7Q0V0V7G01J4J8R8M2Q1.json")
    );

    let listed = work_store::list(&pool_home).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].source, WorkSource::Pool);

    assert!(work_store::remove(&pool_home, "01HZ7P7Q0V0V7G01J4J8R8M2Q1").unwrap());
    assert!(work_store::load(&pool_home, "01HZ7P7Q0V0V7G01J4J8R8M2Q1")
        .unwrap()
        .is_none());
}
