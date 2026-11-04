use std::fs;
use std::path::Path;
use std::process::Command;

use rusqlite::Connection;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_driodcopy")
}

fn output_text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn run_copy(cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run driodcopy")
}

struct LogRow {
    source: String,
    destination: String,
    status: String,
    error_msg: String,
    timestamp: String,
}

fn log_rows(db: &Path) -> Vec<LogRow> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT source, destination, status, error_msg, timestamp
             FROM logs ORDER BY id",
        )
        .unwrap();
    stmt.query_map([], |row| {
        Ok(LogRow {
            source: row.get(0)?,
            destination: row.get(1)?,
            status: row.get(2)?,
            error_msg: row.get(3)?,
            timestamp: row.get(4)?,
        })
    })
    .unwrap()
    .map(|row| row.unwrap())
    .collect()
}

fn assert_schema(db: &Path) {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn.prepare("PRAGMA table_info(logs)").unwrap();
    let columns: Vec<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("source".to_string(), "TEXT".to_string()),
            ("destination".to_string(), "TEXT".to_string()),
            ("status".to_string(), "TEXT".to_string()),
            ("error_msg".to_string(), "TEXT".to_string()),
            ("timestamp".to_string(), "TEXT".to_string()),
        ]
    );
}

fn assert_process_complete(stdout: &str) {
    let last = stdout
        .lines()
        .rev()
        .find(|line| !line.is_empty())
        .unwrap_or("");
    assert_eq!(last, "Process complete.", "stdout:\n{stdout}");
}

#[test]
fn help_lists_default_workers_and_database() {
    let output = Command::new(bin()).arg("--help").output().unwrap();
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--workers"));
    assert!(stdout.contains("--db"));
    assert!(stdout.contains("[default: 10]"));
    assert!(stdout.contains("[default: ./copy_logs.db]"));
}

#[test]
fn copies_tree_logs_each_file_and_overwrites() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    fs::create_dir_all(src.join("sub").join("empty")).unwrap();
    fs::write(src.join("hello.txt"), b"hello").unwrap();
    fs::write(src.join("sub").join("nested.txt"), b"nested").unwrap();
    fs::write(src.join("notes file.txt"), b"with space").unwrap();

    let output = run_copy(root.path(), &["src", "dst"]);
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert_eq!(stdout.matches("Successfully copied").count(), 3, "{stdout}");
    assert!(
        stdout
            .lines()
            .filter(|line| line.contains("Successfully copied"))
            .all(|line| line.starts_with("[Worker ") && line.contains(']')),
        "{stdout}"
    );
    assert_process_complete(&stdout);
    assert!(output.stderr.is_empty(), "{}", output_text(&output));

    assert_eq!(fs::read(dst.join("hello.txt")).unwrap(), b"hello");
    assert_eq!(
        fs::read(dst.join("sub").join("nested.txt")).unwrap(),
        b"nested"
    );
    assert_eq!(fs::read(dst.join("notes file.txt")).unwrap(), b"with space");
    assert!(dst.join("sub").join("empty").is_dir());

    let db = root.path().join("copy_logs.db");
    assert!(db.is_file(), "default database path was not created");
    assert_schema(&db);
    let rows = log_rows(&db);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row.status == "success"));
    assert!(rows.iter().all(|row| row.error_msg.is_empty()));
    assert!(rows.iter().all(|row| !row.timestamp.is_empty()));
    assert!(rows
        .iter()
        .any(|row| Path::new(&row.source).ends_with("hello.txt")));
    assert!(rows
        .iter()
        .any(|row| Path::new(&row.destination).ends_with("nested.txt")));

    fs::write(src.join("hello.txt"), b"updated").unwrap();
    let again = run_copy(
        root.path(),
        &["src", "dst", "--workers", "4", "--db", "copy_logs.db"],
    );
    assert!(again.status.success(), "{}", output_text(&again));
    assert_eq!(fs::read(dst.join("hello.txt")).unwrap(), b"updated");
    let rows = log_rows(&db);
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|row| row.status == "success"));
}

#[test]
fn logs_file_copy_failure_with_worker_id() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dst).unwrap();
    fs::write(src.join("ok.txt"), b"ok").unwrap();
    fs::write(src.join("conflict"), b"file").unwrap();
    // A directory standing where the file should land makes std::fs::copy fail
    // even when the process is privileged.
    fs::create_dir(dst.join("conflict")).unwrap();

    let db = root.path().join("logs").join("copy.db");
    let output = run_copy(
        root.path(),
        &["src", "dst", "--workers", "2", "--db", db.to_str().unwrap()],
    );
    assert!(!output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.lines().any(|line| {
            line.starts_with("[Worker ")
                && line.contains("Error copying")
                && line.contains("conflict")
        }),
        "{stdout}"
    );
    assert!(
        stdout.contains("Successfully copied") && stdout.contains("ok.txt"),
        "{stdout}"
    );
    assert_process_complete(&stdout);
    assert_eq!(fs::read(dst.join("ok.txt")).unwrap(), b"ok");
    assert!(dst.join("conflict").is_dir());

    assert!(db.is_file(), "explicit --db path was not created");
    let rows = log_rows(&db);
    assert!(rows
        .iter()
        .any(|row| row.status == "success" && row.error_msg.is_empty()));
    assert!(rows.iter().any(|row| {
        row.status == "error"
            && !row.error_msg.is_empty()
            && Path::new(&row.source).ends_with("conflict")
    }));
}

#[test]
fn logs_directory_creation_failure() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    fs::create_dir_all(src.join("nested")).unwrap();
    fs::write(src.join("keep.txt"), b"keep").unwrap();
    fs::write(src.join("nested").join("file.txt"), b"hidden").unwrap();
    fs::create_dir_all(&dst).unwrap();
    fs::write(dst.join("nested"), b"not a directory").unwrap();

    let db = root.path().join("copy_logs.db");
    let output = run_copy(
        root.path(),
        &["src", "dst", "-w", "2", "-d", db.to_str().unwrap()],
    );
    assert!(!output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with("[Walker] Error creating directory")),
        "{stdout}"
    );
    assert_process_complete(&stdout);
    assert_eq!(fs::read(dst.join("keep.txt")).unwrap(), b"keep");
    assert!(!dst.join("nested").join("file.txt").exists());

    let rows = log_rows(&db);
    assert!(rows.iter().any(|row| row.status == "success"));
    assert!(rows.iter().any(|row| {
        row.status == "error"
            && row.error_msg.contains("Error creating directory")
            && Path::new(&row.destination).ends_with("nested")
    }));
}

#[test]
fn prints_sqlite_insert_failures() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("hello.txt"), b"hello").unwrap();

    let db = root.path().join("copy_logs.db");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            destination TEXT,
            status TEXT,
            error_msg TEXT,
            timestamp TEXT
        );
        CREATE TRIGGER fail_insert BEFORE INSERT ON logs
        BEGIN SELECT RAISE(ABORT, 'forced log failure'); END;",
    )
    .unwrap();
    drop(conn);

    let output = run_copy(root.path(), &["src", "dst", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("Successfully copied"), "{stdout}");
    assert_process_complete(&stdout);
    assert!(
        stderr.contains("[Logger] Failed to log") && stderr.contains("forced log failure"),
        "{stderr}"
    );
    assert_eq!(fs::read(dst.join("hello.txt")).unwrap(), b"hello");

    let conn = Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn rejects_a_destination_inside_the_source() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("file.txt"), b"data").unwrap();

    let output = run_copy(root.path(), &["src", "src/inside"]);
    assert!(!output.status.success(), "{}", output_text(&output));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("destination"), "{stderr}");
    assert!(!src.join("inside").exists());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("Process complete."));
}
