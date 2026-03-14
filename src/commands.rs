use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::db;
use crate::scan;

pub fn scan(path: PathBuf, db: PathBuf, threads: usize) -> Result<()> {
    let scan_start = scan::now_unix();
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
        // --- exact hash lookup ---
        let conn = db::open(&db)?;
        let paths = db::query_by_hash(&conn, &input)?;
        if paths.is_empty() {
            println!("No files found for hash: {}", input);
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

    // Compute SHA-256 of the input file.
    let sha256 = compute_sha256_for_file(&path)?;

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

/// Returns true if `s` looks like a SHA-256 hex digest (exactly 64 lowercase hex chars).
fn looks_like_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
}

/// Compute the SHA-256 hash of a file and return it as a hex string.
fn compute_sha256_for_file(path: &std::path::Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
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
        // 63 valid hex chars + one 'g'
        let bad = format!("{}g", "a".repeat(63));
        assert!(!looks_like_sha256(&bad));
    }

    #[test]
    fn looks_like_sha256_rejects_file_path() {
        assert!(!looks_like_sha256("/home/user/image.png"));
    }

    #[test]
    fn compute_sha256_known_content() {
        // SHA-256("abc") is well-known.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"abc").unwrap();
        let hash = compute_sha256_for_file(&path).unwrap();
        assert_eq!(hash, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    #[test]
    fn compute_sha256_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.txt");
        std::fs::write(&path, b"").unwrap();
        let hash = compute_sha256_for_file(&path).unwrap();
        assert_eq!(hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }
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
