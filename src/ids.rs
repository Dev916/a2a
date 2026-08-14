use crate::config::Lane;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use rand::Rng;

const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

pub fn generate(lane: Lane, now: chrono::DateTime<chrono::Utc>) -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (0..4).map(|_| B32[rng.gen_range(0..32)] as char).collect();
    format!("{}-{}-{}", lane, now.format("%m%dt%H%M"), suffix)
}

pub fn is_valid(id: &str) -> bool {
    regex::Regex::new(r"^(impl|review|research|general)-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$")
        .unwrap()
        .is_match(id)
}

pub fn resolve(home: &A2aHome, prefix: &str) -> Result<String, AppError> {
    let ids = home
        .list_worker_ids()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, e.to_string()))?;
    let matches: Vec<_> = ids.into_iter().filter(|i| i.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches.into_iter().next().unwrap()),
        0 => Err(AppError::new(
            "NO_SUCH_WORKER",
            ExitCode::NoSuchWorker,
            format!("no worker matches '{prefix}'"),
        )),
        _ => Err(AppError::new(
            "AMBIGUOUS_WORKER",
            ExitCode::NoSuchWorker,
            format!("'{prefix}' is ambiguous: {}", matches.join(", ")),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn generate_matches_contract_shape() {
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 13, 18, 0).unwrap();
        let id = generate(crate::config::Lane::Impl, now);
        let re = regex::Regex::new(r"^impl-0814t1318-[a-z2-7]{4}$").unwrap();
        assert!(re.is_match(&id), "got {id}");
        assert!(is_valid(&id));
    }

    #[test]
    fn resolve_prefix_unique_ambiguous_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let home = crate::home::A2aHome::resolve(Some(tmp.path())).unwrap();
        for id in ["impl-0814t1300-aaaa", "impl-0814t1300-aabb", "review-0814t1301-cccc"] {
            std::fs::create_dir_all(home.worker_dir(id)).unwrap();
        }
        assert_eq!(resolve(&home, "review").unwrap(), "review-0814t1301-cccc");
        let err = resolve(&home, "impl-0814t1300-aa").unwrap_err();
        assert_eq!(err.code, "AMBIGUOUS_WORKER");
        assert!(err.message.contains("aabb") && err.message.contains("aaaa"));
        assert_eq!(resolve(&home, "zzz").unwrap_err().code, "NO_SUCH_WORKER");
    }
}
