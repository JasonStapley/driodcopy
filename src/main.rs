use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use driodcopy::{Config, DEFAULT_DB_PATH, DEFAULT_WORKERS};

/// Recursively copy a directory with a pool of workers and log every result to SQLite.
#[derive(Debug, Parser)]
#[command(name = "driodcopy", version, about)]
struct Args {
    /// Directory to copy from.
    source_dir: PathBuf,

    /// Directory to copy to. Created if it does not exist.
    dest_dir: PathBuf,

    /// Number of concurrent copy workers.
    #[arg(short = 'w', long = "workers", default_value_t = DEFAULT_WORKERS)]
    workers: usize,

    /// SQLite database that records each copy result.
    #[arg(short = 'd', long = "db", default_value = DEFAULT_DB_PATH)]
    db: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let outcome = match driodcopy::run(Config {
        source_dir: args.source_dir,
        dest_dir: args.dest_dir,
        workers: args.workers,
        db_path: args.db,
    }) {
        Ok(outcome) => outcome,
        Err(err) => {
            eprintln!("driodcopy: {err}");
            return ExitCode::from(1);
        }
    };

    if outcome.has_failures() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
