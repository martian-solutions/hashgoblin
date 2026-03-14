use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::db;
use crate::scan;

pub fn scan(path: PathBuf, db: PathBuf, threads: usize) -> Result<()> {
    let scan_start = scan::now_unix()?;
    println!("Scanning: {}", path.display());
    println!("Database: {}", db.display());
    println!("Threads:  {}", threads);

    let result = scan::run(&path, &db, threads, scan_start)?;

    println!("\nResults:");
    println!("  Hashed:  {}", result.processed);
    println!("  Skipped: {} (unchanged)", result.skipped);
    println!("  Errors:  {}", result.errors);
    println!("  Stale:   {} (marked missing)", result.stale);
    Ok(())
}

pub fn dupes(db: PathBuf, min_size: u64) -> Result<()> {
    let conn = db::open(&db)?;
    let groups = db::query_dupes(&conn, min_size)?;
    if groups.is_empty() {
        println!("No duplicates found.");
        return Ok(());
    }
    println!("{} duplicate group(s):\n", groups.len());
    for g in &groups {
        println!(
            "[{}] {} copies, {} bytes each",
            &g.sha256[..16],
            g.count,
            g.size
        );
        for p in &g.paths {
            println!("  {}", p);
        }
        println!();
    }
    Ok(())
}

/// Find duplicates or perceptually similar images for a given input.
///
/// `input` is either:
/// - A 64-character SHA-256 hex string → returns all files with that exact hash.
/// - A file path on disk → returns exact SHA-256 duplicates for any file type,
///   plus perceptually similar images (PDQ hash, Hamming distance ≤ `threshold`)
///   when the input is an image.
pub fn find(input: String, db: PathBuf, threshold: u32) -> Result<()> {
    if looks_like_sha256(&input) {
        // Normalise to lowercase: SHA-256 hashes are stored as lowercase by
        // sha2, but SQLite TEXT comparison is case-sensitive, so an uppercase
        // input would otherwise return no results.
        let hash = input.to_lowercase();
        let conn = db::open(&db)?;
        let paths = db::query_by_hash(&conn, &hash)?;
        if paths.is_empty() {
            println!("No files found for hash: {}", hash);
        } else {
            for p in &paths {
                println!("{}", p);
            }
        }
        return Ok(());
    }

    // --- file path: exact dupes + optional perceptual search ---
    let path = PathBuf::from(&input);
    if !path.exists() {
        bail!("Path does not exist: {}", path.display());
    }

    let sha256 = scan::sha256_file(&path)?;

    let conn = db::open(&db)?;

    // Exact SHA-256 duplicates (includes the file itself if it has been scanned).
    let exact = db::query_by_hash(&conn, &sha256)?;
    if exact.is_empty() {
        println!("No exact duplicates found.");
    } else {
        println!("{} exact duplicate(s) [sha256: {}]:", exact.len(), &sha256[..16]);
        for p in &exact {
            println!("  {}", p);
        }
    }

    // Perceptual similarity search for images.
    let mime = tree_magic_mini::from_filepath(&path).unwrap_or("application/octet-stream");
    if mime.starts_with("image/") {
        println!();
        match scan::compute_pdq(&path) {
            None => {
                println!("Could not compute PDQ hash for this image (unsupported format or corrupt file).");
            }
            Some(pdq_hex) => {
                let similar = db::query_similar_pdq(&conn, &pdq_hex, threshold)?;
                if similar.is_empty() {
                    println!("No perceptually similar images found (threshold: {} bits).", threshold);
                } else {
                    println!(
                        "{} perceptually similar image(s) (PDQ threshold: ≤ {} bits):\n",
                        similar.len(),
                        threshold
                    );
                    for s in &similar {
                        println!("  [{:>3} bits] {}", s.distance, s.path);
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn stats(db: PathBuf) -> Result<()> {
    let conn = db::open(&db)?;
    let s = db::query_stats(&conn)?;
    let size_str = human_size(s.total_size);
    println!("Files:       {}", s.total_files);
    println!("Total size:  {}", size_str);
    println!("Dupe groups: {}", s.dupe_groups);
    println!("Dupe files:  {}", s.dupe_files);
    println!("Errors:      {}", s.error_count);
    println!("Stale:       {}", s.stale_count);
    Ok(())
}

pub fn stale(db: PathBuf) -> Result<()> {
    let conn = db::open(&db)?;
    let paths = db::query_stale(&conn)?;
    if paths.is_empty() {
        println!("No stale files.");
    } else {
        println!("{} stale file(s):", paths.len());
        for p in &paths {
            println!("  {}", p);
        }
    }
    Ok(())
}

/// Returns true if `s` looks like a SHA-256 hex digest (exactly 64 hex chars).
fn looks_like_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.2} {}", size, UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_sha256_valid_lowercase() {
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe04294e576dce3e4f7e65eed40";
        assert!(looks_like_sha256(hash));
    }

    #[test]
    fn looks_like_sha256_valid_uppercase() {
        let hash = "B94D27B9934D3E08A52E52D7DA7DABFAC484EFE04294E576DCE3E4F7E65EED40";
        assert!(looks_like_sha256(hash));
    }

    #[test]
    fn looks_like_sha256_too_short() {
        assert!(!looks_like_sha256("abc123"));
    }

    #[test]
    fn looks_like_sha256_too_long() {
        let long = "a".repeat(65);
        assert!(!looks_like_sha256(&long));
    }

    #[test]
    fn looks_like_sha256_non_hex_char() {
        let bad = format!("{}g", "a".repeat(63));
        assert!(!looks_like_sha256(&bad));
    }

    #[test]
    fn looks_like_sha256_rejects_file_path() {
        assert!(!looks_like_sha256("/home/user/image.png"));
    }

    #[test]
    fn human_size_bytes() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(1), "1 B");
        assert_eq!(human_size(1023), "1023 B");
    }

    #[test]
    fn human_size_kilobytes() {
        assert_eq!(human_size(1024), "1.00 KB");
        assert_eq!(human_size(1025), "1.00 KB");
        assert_eq!(human_size(1024 * 1023), "1023.00 KB");
    }

    #[test]
    fn human_size_megabytes() {
        assert_eq!(human_size(1024 * 1024), "1.00 MB");
    }

    #[test]
    fn human_size_gigabytes() {
        assert_eq!(human_size(1024 * 1024 * 1024), "1.00 GB");
    }
}
