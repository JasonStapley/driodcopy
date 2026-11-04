# driodcopy

Concurrent directory copier for Windows 11, with the same idea as a small slice of `robocopy`: walk a source tree, recreate directories, and copy files on a pool of workers. Every file result is printed with the worker id and appended to a SQLite log.

The original Go program is kept in [`go/main.go`](go/main.go) for comparison. This crate is the supported implementation.

## Build

```bash
cargo build --release
```

Rust 1.85 or newer is required. The binary is `target/release/driodcopy` (`target\release\driodcopy.exe` on Windows).

SQLite is compiled into the binary (`rusqlite` with the `bundled` feature), so Windows does not need a separate SQLite install. Building still needs the usual Rust C toolchain (MSVC Build Tools on Windows, or a system C compiler elsewhere) because the bundled library is compiled from source.

Check the tests with:

```bash
cargo test
```

## Run

```bash
driodcopy <source_dir> <dest_dir>
driodcopy <source_dir> <dest_dir> --workers 10 --db ./copy_logs.db
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--workers`, `-w` | `10` | Concurrent copy workers. Must be at least 1. |
| `--db`, `-d` | `./copy_logs.db` | SQLite file that receives one row per result. |

Workers are numbered from 1. A successful file looks like:

```text
[Worker 3] Successfully copied photos/img.jpg to backup/img.jpg
```

A failed file names the same worker and the reason. Directory creation failures are printed with `[Walker]` and logged too. When the walk and the log thread have both finished, the program prints:

```text
Process complete.
```

Exit code `0` means every file and directory succeeded and every row was written. Exit code `1` means the run could not start, a copy or directory create failed, or a log insert failed. Per-file failures still finish with `Process complete.` so the log is complete before the process exits.

Copying a directory into itself is rejected. That walk would otherwise keep discovering files it had just created.

### Log schema

Table `logs` in the database:

| Column | Type |
| --- | --- |
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `source` | `TEXT` |
| `destination` | `TEXT` |
| `status` | `TEXT` (`success` or `error`) |
| `error_msg` | `TEXT` (empty when the copy succeeded) |
| `timestamp` | `TEXT` (local RFC 3339) |

Inserts run on one logger thread. SQLite is not safe for concurrent writers, and a failed insert is printed to stderr instead of being dropped.

## Compared with robocopy

`robocopy` is Windows' full copy utility: retries, restartable mode, `/MIR` mirroring, attribute and ACL copies, long-path handling, and the `/MT` thread switch. driodcopy is the small version of the `/MT` idea. It recursively copies a source tree with a fixed worker pool (10 by default), prints each file with the worker that copied it, and records every result in SQLite.

It does not delete extra files at the destination, retry failed copies, copy security descriptors or alternate data streams, or use robocopy's exit-code bitmask. Files already present at the destination are overwritten. Empty directories are recreated. Symlinks are not followed as directories; a symlink to a file is copied as the file contents.
