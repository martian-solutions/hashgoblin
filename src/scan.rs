use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::db::{self, FileRecord};

pub struct ScanResult {
    pub processed: u64,
    pub skipped: u64,
    pub errors: u64,
    pub stale: u64,
}

/// A work item describing one file to process.
struct WorkItem {
    path: PathBuf,
    inode: u64,
    size: u64,
    mtime: i64,
}

/// Result of processing a single file, sent back to the writer thread.
struct ProcessedFile {
    record: FileRecord,
}

pub fn run(root: &Path, db_path: &Path, threads: usize, scan_start: i64) -> Result<ScanResult> {
    // Open DB on the main thread for the initial cache queries.
    let conn = db::open(db_path)?;
    db::migrate(&conn)?;

    // Collect all walkable files first so we can show a spinner while walking.
    let spinner = ProgressBar::new_spinner();
    spinner.set_message("Walking directory tree...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    let mut work_items: Vec<WorkItem> = Vec::new();
    let mut walk_errors: Vec<String> = Vec::new();

    for entry in WalkDir::new(root).follow_links(false) {
        match entry {
            Err(e) => {
                walk_errors.push(format!("{}", e));
            }
            Ok(e) => {
                if !e.file_type().is_file() {
                    continue;
                }
                match e.metadata() {
                    Err(err) => {
                        walk_errors.push(format!("{}: {}", e.path().display(), err));
                    }
                    Ok(meta) => {
                        let mtime = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        work_items.push(WorkItem {
                            path: e.path().to_path_buf(),
                            inode: meta.ino(),
                            size: meta.len(),
                            mtime,
                        });
                    }
                }
            }
        }
    }

    spinner.finish_and_clear();

    let total = work_items.len() as u64;
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")?
            .progress_chars("=>-"),
    );

    // Separate the paths that can be skipped from those that need hashing.
    // We do cache lookups on the main thread (single DB connection, no locking needed).
    let mut to_hash: Vec<WorkItem> = Vec::new();
    let mut skip_paths: Vec<(String, i64)> = Vec::new(); // (path, last_seen)

    for item in work_items {
        let path_str = item.path.to_string_lossy().into_owned();
        match db::get_cached(&conn, &path_str)? {
            Some(cached)
                if cached.inode == Some(item.inode)
                    && cached.size == Some(item.size)
                    && cached.mtime == Some(item.mtime) =>
            {
                skip_paths.push((path_str, scan_start));
            }
            _ => {
                to_hash.push(item);
            }
        }
    }

    // Touch last_seen for skipped files in a transaction.
    {
        conn.execute_batch("BEGIN")?;
        for (path, ts) in &skip_paths {
            db::touch_last_seen(&conn, path, *ts)?;
        }
        conn.execute_batch("COMMIT")?;
    }

    let skipped = skip_paths.len() as u64;
    drop(skip_paths);

    // Channel: hashing workers → writer thread.
    let (tx, rx) = mpsc::channel::<ProcessedFile>();

    // Spawn a dedicated writer thread.
    let writer_conn = db::open(db_path)?;
    let pb_writer = pb.clone();
    let writer = std::thread::spawn(move || -> Result<(u64, u64)> {
        let mut processed = 0u64;
        let mut errors = 0u64;
        let mut batch = 0usize;

        writer_conn.execute_batch("BEGIN")?;
        for pf in rx {
            if pf.record.error.is_some() {
                errors += 1;
            } else {
                processed += 1;
            }
            db::upsert(&writer_conn, &pf.record)?;
            pb_writer.inc(1);
            batch += 1;
            // Commit every 500 records to keep transactions short.
            if batch >= 500 {
                writer_conn.execute_batch("COMMIT; BEGIN")?;
                batch = 0;
            }
        }
        writer_conn.execute_batch("COMMIT")?;
        Ok((processed, errors))
    });

    // Configure rayon thread pool.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;

    pool.install(|| {
        to_hash.par_iter().for_each(|item| {
            let path_str = item.path.to_string_lossy().into_owned();
            let pf = process_file(&item.path, &path_str, item.inode, item.size, item.mtime, scan_start);
            let _ = tx.send(pf);
        });
    });

    // Close the sender so the writer thread exits.
    drop(tx);

    let (hashed, errors) = writer.join().expect("writer thread panicked")?;

    // Log walk errors to stderr (non-fatal).
    for e in &walk_errors {
        eprintln!("walk error: {}", e);
    }

    pb.finish_with_message("Scan complete");

    // Mark stale.
    let stale = db::mark_stale(&conn, scan_start)?;

    Ok(ScanResult {
        processed: hashed,
        skipped,
        errors,
        stale,
    })
}

fn process_file(
    path: &Path,
    path_str: &str,
    inode: u64,
    size: u64,
    mtime: i64,
    scan_start: i64,
) -> ProcessedFile {
    match hash_and_magic(path) {
        Ok((sha256, mime_type, file_desc)) => ProcessedFile {
            record: FileRecord {
                path: path_str.to_owned(),
                inode: Some(inode),
                size: Some(size),
                mtime: Some(mtime),
                sha256: Some(sha256),
                mime_type: Some(mime_type),
                file_desc: Some(file_desc),
                last_seen: scan_start,
                stale: false,
                error: None,
            },
        },
        Err(e) => ProcessedFile {
            record: FileRecord {
                path: path_str.to_owned(),
                inode: Some(inode),
                size: Some(size),
                mtime: Some(mtime),
                sha256: None,
                mime_type: None,
                file_desc: None,
                last_seen: scan_start,
                stale: false,
                error: Some(e.to_string()),
            },
        },
    }
}

fn hash_and_magic(path: &Path) -> Result<(String, String, String)> {
    // SHA-256
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        hasher.update(&buf[..n]);
    }
    let hash = format!("{:x}", hasher.finalize());

    // MIME type via tree_magic_mini (pure Rust, reads file itself)
    let mime = tree_magic_mini::from_filepath(path)
        .unwrap_or("application/octet-stream")
        .to_string();

    // tree_magic_mini doesn't give a human description; use the MIME type as desc too.
    // This keeps us dependency-free from libmagic C bindings.
    let desc = mime.clone();

    Ok((hash, mime, desc))
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
