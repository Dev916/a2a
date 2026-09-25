use crate::home::{create_dir_0700, A2aHome};
use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

const COMMAND_GRANTS_FILE: &str = "command-grants.json";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandGrants {
    #[serde(default)]
    granted: BTreeSet<String>,
}

impl CommandGrants {
    pub fn load(home: &A2aHome) -> Result<Self, AppError> {
        let path = command_grants_path(home);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(io_error("reading command grants", error)),
        };
        serde_json::from_slice(&bytes).map_err(|error| {
            AppError::new(
                "DATA",
                ExitCode::Precondition,
                format!("command grants are invalid: {error}"),
            )
            .remedy("restore or remove mesh/command-grants.json, then grant trusted peers again")
        })
    }

    pub fn grant(&mut self, peer: &str) {
        self.granted.insert(peer.to_string());
    }

    pub fn revoke(&mut self, peer: &str) {
        self.granted.remove(peer);
    }

    pub fn save(&self, home: &A2aHome) -> Result<(), AppError> {
        let path = command_grants_path(home);
        let parent = path.parent().expect("command grants path has a parent");
        create_dir_0700(parent)
            .map_err(|error| io_error("creating command grants directory", error))?;
        let mut bytes = serde_json::to_vec_pretty(self).map_err(|error| {
            AppError::new("SERIALIZE", ExitCode::Internal, error.to_string())
        })?;
        bytes.push(b'\n');
        crate::store::write_atomic(&path, &bytes)
            .map_err(|error| io_error("writing command grants", error))
    }

    pub fn list(&self) -> Vec<String> {
        self.granted.iter().cloned().collect()
    }

    pub fn is_granted(&self, peer: &str, self_gateway_id: &str) -> bool {
        peer == self_gateway_id || self.granted.contains(peer)
    }
}

fn command_grants_path(home: &A2aHome) -> PathBuf {
    home.root().join("mesh").join(COMMAND_GRANTS_FILE)
}

fn io_error(context: &str, error: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::A2aHome;
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn test_home() -> (tempfile::TempDir, A2aHome) {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        (tmp, home)
    }

    #[test]
    fn grant_and_revoke_change_authorization() {
        let mut grants = CommandGrants::default();

        grants.grant("gw-peer");
        assert!(grants.is_granted("gw-peer", "gw-self"));

        grants.revoke("gw-peer");
        assert!(!grants.is_granted("gw-peer", "gw-self"));
    }

    #[test]
    fn list_is_sorted() {
        let mut grants = CommandGrants::default();
        grants.grant("gw-z");
        grants.grant("gw-a");
        grants.grant("gw-m");

        assert_eq!(
            grants.list(),
            vec![
                "gw-a".to_string(),
                "gw-m".to_string(),
                "gw-z".to_string(),
            ]
        );
    }

    #[test]
    fn self_is_always_granted() {
        let grants = CommandGrants::default();

        assert!(grants.is_granted("gw-self", "gw-self"));
        assert!(grants.list().is_empty());
    }

    #[test]
    fn missing_file_loads_as_empty() {
        let (_tmp, home) = test_home();

        let grants = CommandGrants::load(&home).unwrap();

        assert!(grants.list().is_empty());
    }

    #[test]
    fn save_load_round_trip_has_stable_shape() {
        let (_tmp, home) = test_home();
        let mut grants = CommandGrants::default();
        grants.grant("gw-b");
        grants.grant("gw-a");

        grants.save(&home).unwrap();
        let loaded = CommandGrants::load(&home).unwrap();

        assert_eq!(loaded.list(), vec!["gw-a".to_string(), "gw-b".to_string()]);
        let path = home.root().join("mesh").join("command-grants.json");
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted, json!({"granted": ["gw-a", "gw-b"]}));
    }

    #[test]
    fn load_ignores_future_fields() {
        let (_tmp, home) = test_home();
        let mesh = home.root().join("mesh");
        fs::create_dir_all(&mesh).unwrap();
        fs::write(
            mesh.join("command-grants.json"),
            br#"{"granted":["gw-peer"],"future_field":{"enabled":true}}"#,
        )
        .unwrap();

        let grants = CommandGrants::load(&home).unwrap();

        assert_eq!(grants.list(), vec!["gw-peer".to_string()]);
    }

    #[test]
    fn saved_file_is_owner_only() {
        let (_tmp, home) = test_home();
        let grants = CommandGrants::default();

        grants.save(&home).unwrap();

        let path = home.root().join("mesh").join("command-grants.json");
        let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
