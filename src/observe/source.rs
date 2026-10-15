//! Read-only helpers for the JSON artifacts observed under an a2a home.

use serde_json::Value;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Read every complete, parseable JSON line in `path`.
///
/// Bytes after the final newline are deliberately ignored: the source may be
/// in the middle of appending that line, so a later poll must retry it.
pub fn read_jsonl(path: &Path) -> Vec<Value> {
    read_jsonl_from(path, 0).0
}

/// Read complete JSON lines beginning at `offset` and return the next safe
/// byte offset. Missing or unreadable files behave like an empty source.
pub fn read_jsonl_from(path: &Path, offset: u64) -> (Vec<Value>, u64) {
    let (rows, advanced) = read_jsonl_records_from(path, offset);
    (
        rows.into_iter().map(|(value, _end)| value).collect(),
        advanced,
    )
}

/// Read one JSON value, returning `None` for a missing, unreadable, or invalid
/// artifact.
pub fn read_json(path: &Path) -> Option<Value> {
    let file = File::open(path).ok()?;
    serde_json::from_reader(file).ok()
}

/// Modification time of `path` in nanoseconds since the Unix epoch, or `None`
/// when the file is missing or the platform cannot report it.
pub(crate) fn modified_nanos(path: &Path) -> Option<u64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    let since = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    u64::try_from(since.as_nanos()).ok()
}

/// The normalizers also need each row's ending byte position for auditable
/// provenance. This keeps the public reader API small while sharing exactly
/// the same torn-tail discipline.
pub(crate) fn read_jsonl_records_from(path: &Path, offset: u64) -> (Vec<(Value, u64)>, u64) {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return (Vec::new(), offset),
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return (Vec::new(), offset);
    }

    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return (Vec::new(), offset);
    }
    let consumed = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(index) => index + 1,
        None => 0,
    };

    let mut rows = Vec::new();
    let mut start = 0;
    while start < consumed {
        let newline = bytes[start..consumed]
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("consumed JSONL bytes end at a newline");
        let end = start + newline;
        let next = end + 1;
        if !bytes[start..end].is_empty() {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes[start..end]) {
                rows.push((value, offset.saturating_add(next as u64)));
            }
        }
        start = next;
    }

    (rows, offset.saturating_add(consumed as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs::{self, OpenOptions};
    use std::io::Write;

    #[test]
    fn jsonl_reader_skips_a_trailing_partial_line() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("events.jsonl");
        fs::write(&path, b"{\"n\":1}\n{\"n\":2").unwrap();

        assert_eq!(read_jsonl(&path), vec![json!({"n": 1})]);
    }

    #[test]
    fn missing_sources_are_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("missing.jsonl");

        assert!(read_jsonl(&path).is_empty());
        assert_eq!(read_jsonl_from(&path, 17), (Vec::new(), 17));
        assert_eq!(read_json(&path), None);
    }

    #[test]
    fn jsonl_reader_advances_only_over_whole_lines() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("events.jsonl");
        let first = b"{\"n\":1}\n";
        fs::write(&path, [first.as_slice(), b"{\"n\":2"].concat()).unwrap();

        let (rows, offset) = read_jsonl_from(&path, 0);
        assert_eq!(rows, vec![json!({"n": 1})]);
        assert_eq!(offset, first.len() as u64);
        assert_eq!(read_jsonl_from(&path, offset), (Vec::new(), offset));

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"}\n").unwrap();
        drop(file);

        let (rows, advanced) = read_jsonl_from(&path, offset);
        assert_eq!(rows, vec![json!({"n": 2})]);
        assert_eq!(advanced, fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn json_reader_accepts_one_object() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        fs::write(&path, b"{\"state\":\"ready\"}\n").unwrap();

        assert_eq!(read_json(&path), Some(json!({"state": "ready"})));
    }
}
