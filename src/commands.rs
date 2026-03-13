use anyhow::Result;
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

pub fn find(hash: String, db: PathBuf) -> Result<()> {
    let conn = db::open(&db)?;
    let paths = db::query_by_hash(&conn, &hash)?;
    if paths.is_empty() {
        println!("No files found for hash: {}", hash);
    } else {
        for p in &paths {
            println!("{}", p);
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
