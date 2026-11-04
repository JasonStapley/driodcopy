//! Concurrent recursive directory copy with a SQLite result log.
//!
//! File copies run on a worker pool. Directory creation and SQLite writes do
//! not: SQLite allows one writer, so a single logger thread performs every
//! insert. Directory and insert failures are printed and counted.

use std::fs::{self, DirBuilder};
use std::io::{self, Write};
use std::path::{absolute, Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::Local;
use rusqlite::{params, Connection};
use walkdir::WalkDir;

/// Worker pool size used when `--workers` is omitted.
pub const DEFAULT_WORKERS: usize = 10;

/// Database path used when `--db` is omitted.
pub const DEFAULT_DB_PATH: &str = "./copy_logs.db";

/// Bounded queue between the walker and the copy workers, matching the original tool.
const TASK_CHANNEL_CAPACITY: usize = 100;

/// Settings for one copy run.
#[derive(Debug, Clone)]
pub struct Config {
    pub source_dir: PathBuf,
    pub dest_dir: PathBuf,
    pub workers: usize,
    pub db_path: PathBuf,
}

/// Counts from a finished run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    /// Files copied successfully.
    pub files_copied: u64,
    /// File copy failures, directory creation failures, and unreadable paths.
    pub failures: u64,
    /// Results that could not be written to SQLite.
    pub log_failures: u64,
}

impl Outcome {
    /// True when a copy, a directory create, or a log write failed.
    pub fn has_failures(&self) -> bool {
        self.failures > 0 || self.log_failures > 0
    }
}

/// Recoverable driodcopy failure.
#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

struct Task {
    source: PathBuf,
    destination: PathBuf,
}

struct LogRecord {
    source: String,
    destination: String,
    status: String,
    error_msg: String,
    timestamp: String,
}

/// Copy `config.source_dir` into `config.dest_dir` and log each result.
///
/// Prints a line per file, tagged with the worker that copied it, and prints
/// `Process complete.` after the workers and the logger have finished.
/// Returns an error when the run cannot start or a worker panics. Per-file
/// failures are reported in [`Outcome`] and still end with `Process complete.`
pub fn run(config: Config) -> Result<Outcome, Error> {
    validate(&config)?;

    let copied = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let log_send_failures = Arc::new(AtomicU64::new(0));

    let (log_tx, log_rx) = mpsc::channel::<LogRecord>();
    let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), String>>(1);
    let db_path = config.db_path.clone();
    let logger = thread::spawn(move || logger_main(db_path, log_rx, ready_tx));

    match ready_rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(message)) => {
            drop(log_tx);
            let _ = join_logger(logger);
            return Err(Error::new(message));
        }
        Err(_) => {
            drop(log_tx);
            return match join_logger(logger) {
                Err(err) => Err(err),
                Ok(_) => Err(Error::new("logger stopped before it was ready")),
            };
        }
    }

    let (task_tx, task_rx) = mpsc::sync_channel::<Task>(TASK_CHANNEL_CAPACITY);
    let task_rx = Arc::new(Mutex::new(task_rx));

    let mut workers = Vec::with_capacity(config.workers);
    for id in 1..=config.workers {
        let task_rx = Arc::clone(&task_rx);
        let log_tx = log_tx.clone();
        let copied = Arc::clone(&copied);
        let failed = Arc::clone(&failed);
        let log_send_failures = Arc::clone(&log_send_failures);
        workers.push(thread::spawn(move || {
            worker_loop(id, &task_rx, log_tx, &copied, &failed, &log_send_failures);
        }));
    }

    let walk_result = enqueue_tree(&config, &task_tx, &log_tx, &failed, &log_send_failures);
    drop(task_tx);

    let mut worker_panic = false;
    for handle in workers {
        if handle.join().is_err() {
            worker_panic = true;
        }
    }

    let files_copied = copied.load(Ordering::Relaxed);
    let failures = failed.load(Ordering::Relaxed);
    drop(log_tx);

    let logged = join_logger(logger)?;
    if worker_panic {
        return Err(Error::new("a copy worker panicked"));
    }
    walk_result?;

    println!("Process complete.");
    let _ = io::stdout().flush();

    Ok(Outcome {
        files_copied,
        failures,
        log_failures: logged + log_send_failures.load(Ordering::Relaxed),
    })
}

fn validate(config: &Config) -> Result<(), Error> {
    if config.workers == 0 {
        return Err(Error::new("worker count must be at least 1"));
    }

    let metadata = fs::metadata(&config.source_dir).map_err(|err| {
        Error::new(format!(
            "cannot read source {}: {err}",
            config.source_dir.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(Error::new(format!(
            "source {} is not a directory",
            config.source_dir.display()
        )));
    }

    if let Some(parent) = config.db_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            create_dir_all_with_mode(parent).map_err(|err| {
                Error::new(format!(
                    "cannot create database directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
    }

    if paths_conflict(&config.source_dir, &config.dest_dir)? {
        return Err(Error::new(
            "destination must not be the source directory or a directory inside it",
        ));
    }

    create_dir_all_with_mode(&config.dest_dir).map_err(|err| {
        Error::new(format!(
            "cannot create destination {}: {err}",
            config.dest_dir.display()
        ))
    })?;
    Ok(())
}

/// True when `dest` is `source` or a path inside it.
///
/// Copying a tree into itself makes the walk pick up newly created files.
fn paths_conflict(source: &Path, dest: &Path) -> Result<bool, Error> {
    let source_abs =
        normalize_lexical(&absolute(source).map_err(|err| {
            Error::new(format!("cannot resolve source {}: {err}", source.display()))
        })?);
    let dest_abs = normalize_lexical(&absolute(dest).map_err(|err| {
        Error::new(format!(
            "cannot resolve destination {}: {err}",
            dest.display()
        ))
    })?);
    if dest_abs.starts_with(&source_abs) {
        return Ok(true);
    }

    if dest.exists() {
        if let (Ok(source_canon), Ok(dest_canon)) = (source.canonicalize(), dest.canonicalize()) {
            if dest_canon.starts_with(&source_canon) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn enqueue_tree(
    config: &Config,
    task_tx: &SyncSender<Task>,
    log_tx: &Sender<LogRecord>,
    failed: &AtomicU64,
    log_send_failures: &AtomicU64,
) -> Result<(), Error> {
    let mut blocked: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(&config.source_dir)
        .min_depth(1)
        .follow_links(false)
    {
        match entry {
            Ok(entry) => {
                let source_path = entry.path();
                if blocked.iter().any(|prefix| source_path.starts_with(prefix)) {
                    continue;
                }

                let relative = source_path.strip_prefix(&config.source_dir).map_err(|_| {
                    Error::new(format!(
                        "path {} is not under the source directory",
                        source_path.display()
                    ))
                })?;
                let destination = config.dest_dir.join(relative);

                if entry.file_type().is_dir() {
                    if let Err(err) = create_dir_all_with_mode(&destination) {
                        let message =
                            format!("Error creating directory {}: {err}", destination.display());
                        println!("[Walker] {message}");
                        failed.fetch_add(1, Ordering::Relaxed);
                        send_log(
                            log_tx,
                            log_send_failures,
                            "Walker",
                            source_path,
                            &destination,
                            "error",
                            &message,
                        );
                        blocked.push(source_path.to_path_buf());
                    }
                } else {
                    task_tx
                        .send(Task {
                            source: source_path.to_path_buf(),
                            destination,
                        })
                        .map_err(|_| Error::new("copy workers stopped before the walk finished"))?;
                }
            }
            Err(err) => {
                let source = err
                    .path()
                    .unwrap_or(config.source_dir.as_path())
                    .to_path_buf();
                let destination = match source.strip_prefix(&config.source_dir) {
                    Ok(relative) if !relative.as_os_str().is_empty() => {
                        config.dest_dir.join(relative)
                    }
                    _ => config.dest_dir.clone(),
                };
                let message = format!("Error reading {}: {err}", source.display());
                println!("[Walker] {message}");
                failed.fetch_add(1, Ordering::Relaxed);
                send_log(
                    log_tx,
                    log_send_failures,
                    "Walker",
                    &source,
                    &destination,
                    "error",
                    &message,
                );
            }
        }
    }

    Ok(())
}

fn worker_loop(
    id: usize,
    tasks: &Mutex<Receiver<Task>>,
    log_tx: Sender<LogRecord>,
    copied: &AtomicU64,
    failed: &AtomicU64,
    log_send_failures: &AtomicU64,
) {
    loop {
        let task = {
            let receiver = tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.recv()
        };
        let task = match task {
            Ok(task) => task,
            Err(_) => break,
        };

        let who = format!("Worker {id}");
        match copy_file(&task.source, &task.destination) {
            Ok(()) => {
                println!(
                    "[Worker {id}] Successfully copied {} to {}",
                    task.source.display(),
                    task.destination.display()
                );
                copied.fetch_add(1, Ordering::Relaxed);
                send_log(
                    &log_tx,
                    log_send_failures,
                    &who,
                    &task.source,
                    &task.destination,
                    "success",
                    "",
                );
            }
            Err(message) => {
                println!(
                    "[Worker {id}] Error copying {} to {}: {message}",
                    task.source.display(),
                    task.destination.display()
                );
                failed.fetch_add(1, Ordering::Relaxed);
                send_log(
                    &log_tx,
                    log_send_failures,
                    &who,
                    &task.source,
                    &task.destination,
                    "error",
                    &message,
                );
            }
        }
    }
}

/// Copy one file, creating the destination's parent directory first.
fn copy_file(source: &Path, destination: &Path) -> Result<(), String> {
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all_with_mode(parent)
                .map_err(|err| format!("creating directory {}: {err}", parent.display()))?;
        }
    }
    fs::copy(source, destination).map_err(|err| format!("copying file: {err}"))?;
    Ok(())
}

fn create_dir_all_with_mode(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o755);
    }
    builder.create(path)
}

fn logger_main(
    db_path: PathBuf,
    rx: Receiver<LogRecord>,
    ready_tx: SyncSender<Result<(), String>>,
) -> Result<u64, Error> {
    let conn = match open_db(&db_path) {
        Ok(conn) => conn,
        Err(err) => {
            let _ = ready_tx.send(Err(err.to_string()));
            return Err(err);
        }
    };

    let mut statement = match prepare_insert(&conn) {
        Ok(statement) => statement,
        Err(err) => {
            let _ = ready_tx.send(Err(err.to_string()));
            return Err(err);
        }
    };

    if ready_tx.send(Ok(())).is_err() {
        return Err(Error::new("caller stopped before the logger was ready"));
    }

    Ok(insert_logs(&mut statement, rx))
}

const INSERT_SQL: &str = "INSERT INTO logs (source, destination, status, error_msg, timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5)";

fn prepare_insert(conn: &Connection) -> Result<rusqlite::Statement<'_>, Error> {
    conn.prepare(INSERT_SQL)
        .map_err(|err| Error::new(format!("cannot prepare log insert: {err}")))
}

/// Insert every record. A failed insert is printed and counted; later records are still attempted.
fn insert_logs(statement: &mut rusqlite::Statement<'_>, rx: Receiver<LogRecord>) -> u64 {
    let mut failures = 0u64;
    for record in rx {
        if let Err(err) = statement.execute(params![
            record.source,
            record.destination,
            record.status,
            record.error_msg,
            record.timestamp,
        ]) {
            failures += 1;
            eprintln!(
                "[Logger] Failed to log {} -> {}: {err}",
                record.source, record.destination
            );
        }
    }
    failures
}

fn open_db(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path)
        .map_err(|err| Error::new(format!("cannot open database {}: {err}", path.display())))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|err| Error::new(format!("cannot set database busy timeout: {err}")))?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            destination TEXT,
            status TEXT,
            error_msg TEXT,
            timestamp TEXT
        );",
    )
    .map_err(|err| Error::new(format!("cannot create logs table: {err}")))?;
    Ok(conn)
}

fn join_logger(handle: thread::JoinHandle<Result<u64, Error>>) -> Result<u64, Error> {
    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(Error::new("logger panicked")),
    }
}

fn send_log(
    tx: &Sender<LogRecord>,
    log_send_failures: &AtomicU64,
    who: &str,
    source: &Path,
    destination: &Path,
    status: &str,
    error_msg: &str,
) {
    let record = LogRecord {
        source: path_to_string(source),
        destination: path_to_string(destination),
        status: status.to_string(),
        error_msg: error_msg.to_string(),
        timestamp: Local::now().to_rfc3339(),
    };
    if let Err(err) = tx.send(record) {
        log_send_failures.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "[{who}] Failed to log {} -> {}: logger stopped",
            err.0.source, err.0.destination
        );
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logger_reports_failed_inserts() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_insert BEFORE INSERT ON logs
             BEGIN SELECT RAISE(ABORT, 'forced log failure'); END;",
        )
        .unwrap();

        let (tx, rx) = mpsc::channel();
        tx.send(LogRecord {
            source: "a.txt".to_string(),
            destination: "b.txt".to_string(),
            status: "success".to_string(),
            error_msg: String::new(),
            timestamp: Local::now().to_rfc3339(),
        })
        .unwrap();
        drop(tx);

        let mut statement = prepare_insert(&conn).unwrap();
        let failures = insert_logs(&mut statement, rx);
        assert_eq!(failures, 1);

        let stored: i64 = conn
            .query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stored, 0);
    }

    #[test]
    fn refuses_to_copy_into_itself() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("file.txt"), b"data").unwrap();

        let err = run(Config {
            source_dir: source.clone(),
            dest_dir: source.join("inside"),
            workers: 2,
            db_path: dir.path().join("logs.db"),
        })
        .unwrap_err();

        assert!(
            err.to_string().contains("destination"),
            "unexpected error: {err}"
        );
        assert!(!source.join("inside").exists());
    }

    #[test]
    fn rejects_missing_source_and_zero_workers() {
        let dir = tempfile::tempdir().unwrap();
        let missing = run(Config {
            source_dir: dir.path().join("missing"),
            dest_dir: dir.path().join("dest"),
            workers: 1,
            db_path: dir.path().join("logs.db"),
        })
        .unwrap_err();
        assert!(missing.to_string().contains("cannot read source"));

        let source = dir.path().join("src");
        fs::create_dir(&source).unwrap();
        let workers = run(Config {
            source_dir: source,
            dest_dir: dir.path().join("dest"),
            workers: 0,
            db_path: dir.path().join("logs.db"),
        })
        .unwrap_err();
        assert!(workers.to_string().contains("at least 1"));
    }
}
