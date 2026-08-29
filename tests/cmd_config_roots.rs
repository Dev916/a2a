mod common;
use assert_cmd::Command;
use std::os::unix::fs::PermissionsExt;

fn run(home: &std::path::Path, args: &[&str]) -> assert_cmd::assert::Assert {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_BROKER_DISABLE", "1")
        .env_remove("A2A_ALLOWED_ROOTS")
        .args(args);
    c.assert()
}

#[test]
fn add_root_creates_config_and_reports_json_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let root = tmp.path().join("Development");
    std::fs::create_dir_all(&root).unwrap();

    let out = run(
        &home,
        &["--json", "config", "add-root", root.to_str().unwrap()],
    )
    .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let canon = std::fs::canonicalize(&root).unwrap();
    assert_eq!(v["data"]["added"], true);
    assert_eq!(v["data"]["path"], canon.to_str().unwrap());
    assert_eq!(
        v["data"]["allowed_roots"].as_array().unwrap()[0],
        serde_json::json!(canon.to_str().unwrap())
    );
    assert!(
        std::fs::read_to_string(home.join("config.toml"))
            .unwrap()
            .contains("allowed_roots"),
        "config.toml is created with the allowed_roots key"
    );
}

#[test]
fn add_root_preserves_existing_comments_and_dedupes_contained_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let root = tmp.path().join("Development");
    let child = root.join("project");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::write(
        home.join("config.toml"),
        "# keep this comment\nallowed_roots = []\n",
    )
    .unwrap();

    run(&home, &["config", "add-root", root.to_str().unwrap()]).success();
    let again = run(&home, &["config", "add-root", child.to_str().unwrap()]).success();
    let stdout = String::from_utf8(again.get_output().stdout.clone()).unwrap();
    assert!(
        stdout.contains("already covered"),
        "dedupe should be friendly: {stdout}"
    );
    let text = std::fs::read_to_string(home.join("config.toml")).unwrap();
    assert!(text.contains("# keep this comment"));
    assert_eq!(
        text.matches(std::fs::canonicalize(&root).unwrap().to_str().unwrap())
            .count(),
        1,
        "the root is present exactly once: {text}"
    );
}

#[test]
fn add_root_rejects_nonexistent_and_non_directory_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let missing = tmp.path().join("missing");
    let out = run(
        &home,
        &["--json", "config", "add-root", missing.to_str().unwrap()],
    )
    .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "ROOT_NOT_FOUND");
    assert!(v["error"]["remedy"]
        .as_str()
        .unwrap()
        .contains("pass a directory that exists"));

    let file = tmp.path().join("file");
    std::fs::write(&file, "not a dir").unwrap();
    let out = run(
        &home,
        &["--json", "config", "add-root", file.to_str().unwrap()],
    )
    .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "ROOT_NOT_FOUND");
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not a directory"));
}

#[test]
fn add_root_reports_config_not_writable() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let root = tmp.path().join("Development");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("LAYOUT_VERSION"), "1\n").unwrap();
    std::fs::create_dir_all(home.join("workers")).unwrap();
    std::fs::create_dir_all(home.join("worktrees")).unwrap();
    let original = std::fs::metadata(&home).unwrap().permissions();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o500)).unwrap();

    let out = run(
        &home,
        &["--json", "config", "add-root", root.to_str().unwrap()],
    )
    .code(9);

    std::fs::set_permissions(&home, original).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "CONFIG_NOT_WRITABLE");
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("config.toml"));
}
