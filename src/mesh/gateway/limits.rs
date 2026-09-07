use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::wire::Frame;
use chrono::Utc;
use serde_json::json;

pub const MIN_DEFERRED_PEER_VERSION: &str = "0.4.0";

pub fn peer_supports_deferred(peer_version: Option<&str>) -> bool {
    peer_version.is_some_and(|version| {
        crate::commands::work_cmd::version_at_least(version, MIN_DEFERRED_PEER_VERSION)
    })
}

pub fn deferred_frame(code: &str, retry_after_secs: Option<u64>) -> Frame {
    Frame::Deferred {
        code: code.to_string(),
        retry_after_secs,
    }
}

pub(crate) fn log_deferral_received(
    home: &A2aHome,
    peer_gateway_id: &str,
    code: &str,
    retry_after_secs: Option<u64>,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut file) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "link",
            "peer_gateway_id": peer_gateway_id,
            "action": "deferral_received",
            "code": code,
            "retry_after_secs": retry_after_secs,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut file, &bytes);
        }
    }
}
