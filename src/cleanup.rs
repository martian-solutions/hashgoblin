use anyhow::Result;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::human_size;
use crate::db::DupeFileInfo;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Wrap a path in single quotes, escaping any embedded single quotes.
/// This is safe for all POSIX shell interpreters including bash.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Best-effort mtime for a file: use the DB value when valid, otherwise fall
/// back to a live `stat` call, and if that also fails return `i64::MAX` so
/// the file is treated as the *newest* copy (i.e. it gets replaced, not kept).
fn effective_mtime(info: &DupeFileInfo) -> i64 {
    match info.mtime {
        Some(m) if m != i64::MIN => return m,
        _ => {}
    }
    std::fs::metadata(&info.path)
        .ok()
        .and_then(|m| {
            m.modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
        })
        .unwrap_or(i64::MAX)
}

/// Return the device number for a path, or `None` if the file cannot be stat'd.
fn device_of(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.dev())
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Write a bash cleanup script to `out`.
///
/// For each duplicate group the oldest file (by mtime) is kept; all others are
/// replaced with a hard link (same filesystem) or a soft link (different
/// filesystem) pointing at the kept copy.  Using `ln -f` / `ln -sf` means
/// running the script a second time is safe.
///
/// Returns the number of groups that were written (groups where at least two
/// copies still exist on disk at generation time are skipped with a comment).
pub fn generate_script(
    groups: &[(String, Vec<DupeFileInfo>)],
    db_path: &Path,
    script_path: &Path,
    out: &mut impl Write,
) -> Result<usize> {
    let total_files: usize = groups.iter().map(|(_, f)| f.len()).sum();

    writeln!(out, "#!/usr/bin/env bash")?;
    writeln!(out, "# HashGoblin — deduplication cleanup script")?;
    writeln!(out, "# Database:   {}", db_path.display())?;
    writeln!(out, "# Script:     {}", script_path.display())?;
    writeln!(out, "# Groups:     {}", groups.len())?;
    writeln!(out, "# Files:      {}", total_files)?;
    writeln!(out, "#")?;
    writeln!(out, "# For each duplicate group the oldest copy (by mtime) is kept.")?;
    writeln!(out, "# Newer copies are replaced in-place using:")?;
    writeln!(out, "#   ln -f     hard link  (same filesystem)")?;
    writeln!(out, "#   ln -sf    soft link  (different filesystem)")?;
    writeln!(out, "# Running the script twice is safe — ln -f/-sf overwrites existing links.")?;
    writeln!(out, "#")?;
    writeln!(out, "# NOTE: soft link targets are the literal paths stored in the database.")?;
    writeln!(out, "#       If you scanned with a relative path the soft links may not resolve")?;
    writeln!(out, "#       correctly unless the script is run from the same directory.")?;
    writeln!(out, "#")?;
    writeln!(out, "# REVIEW THIS SCRIPT BEFORE RUNNING IT.")?;
    writeln!(out, "# To execute:  bash {}", shell_quote(&script_path.to_string_lossy()))?;
    writeln!(out)?;
    writeln!(out, "set -euo pipefail")?;

    let mut groups_written = 0usize;
    let total_groups = groups.len();

    for (idx, (sha256, files)) in groups.iter().enumerate() {
        // Only consider files that currently exist on disk.
        let existing: Vec<&DupeFileInfo> =
            files.iter().filter(|f| Path::new(&f.path).exists()).collect();

        let group_num = idx + 1;

        if existing.len() < 2 {
            writeln!(out)?;
            writeln!(
                out,
                "# ── Group {}/{}: sha256 {}…  (SKIPPED — fewer than 2 copies found on disk) ──",
                group_num,
                total_groups,
                &sha256[..sha256.len().min(16)],
            )?;
            continue;
        }

        // Sort by effective mtime ascending; break ties by path for determinism.
        let mut sortable: Vec<(&DupeFileInfo, i64)> =
            existing.iter().map(|f| (*f, effective_mtime(f))).collect();
        sortable.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.path.cmp(&b.0.path)));

        let keeper = sortable[0].0;
        let keeper_path = Path::new(&keeper.path);
        let keeper_dev = device_of(keeper_path);
        let size_str = human_size(keeper.size);

        writeln!(out)?;
        writeln!(
            out,
            "# ── Group {}/{}: sha256 {}…  ({} copies · {} each) ──────────────────────────────",
            group_num,
            total_groups,
            &sha256[..sha256.len().min(16)],
            existing.len(),
            size_str,
        )?;
        writeln!(out, "# Keep (oldest):  {}", keeper.path)?;

        for (file, _) in &sortable[1..] {
            let file_path = Path::new(&file.path);
            let file_dev = device_of(file_path);

            let same_fs = matches!((keeper_dev, file_dev), (Some(kd), Some(fd)) if kd == fd);

            if same_fs {
                writeln!(out, "# hard link  →  {}", file.path)?;
                writeln!(
                    out,
                    "ln -f -- {} {}",
                    shell_quote(&keeper.path),
                    shell_quote(&file.path),
                )?;
            } else {
                writeln!(out, "# soft link  →  {}  (different filesystem)", file.path)?;
                writeln!(
                    out,
                    "ln -sf -- {} {}",
                    shell_quote(&keeper.path),
                    shell_quote(&file.path),
                )?;
            }
        }

        groups_written += 1;
    }

    writeln!(out)?;

    // Stamp when the script was generated (Unix timestamp — no chrono dep needed).
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    writeln!(out, "# Generated at Unix timestamp {}", ts)?;
    writeln!(out, "echo 'HashGoblin cleanup complete.'")?;

    Ok(groups_written)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_no_special_chars() {
        assert_eq!(shell_quote("/foo/bar.txt"), "'/foo/bar.txt'");
    }

    #[test]
    fn shell_quote_with_single_quote() {
        // O'Brien.txt  →  'O'\''Brien.txt'
        assert_eq!(shell_quote("O'Brien.txt"), "'O'\\''Brien.txt'");
    }

    #[test]
    fn shell_quote_with_spaces() {
        assert_eq!(shell_quote("/my files/a b.jpg"), "'/my files/a b.jpg'");
    }

    #[test]
    fn effective_mtime_uses_db_value() {
        let info = DupeFileInfo { path: "/nonexistent".into(), mtime: Some(12345), size: 0 };
        assert_eq!(effective_mtime(&info), 12345);
    }

    #[test]
    fn effective_mtime_sentinel_falls_back() {
        // i64::MIN means "unavailable"; for a nonexistent path stat also fails,
        // so we expect i64::MAX (treated as newest).
        let info = DupeFileInfo { path: "/nonexistent_xyz_123".into(), mtime: Some(i64::MIN), size: 0 };
        assert_eq!(effective_mtime(&info), i64::MAX);
    }

    #[test]
    fn generate_script_basic() {
        let groups = vec![(
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string(),
            vec![
                DupeFileInfo { path: "/keep.txt".into(), mtime: Some(1000), size: 512 },
                DupeFileInfo { path: "/remove.txt".into(), mtime: Some(2000), size: 512 },
            ],
        )];

        let mut buf = Vec::<u8>::new();
        // /keep.txt and /remove.txt don't exist on disk, so the group will be
        // skipped. This just verifies the function runs without error.
        let n = generate_script(
            &groups,
            Path::new("/test.db"),
            Path::new("/cleanup.sh"),
            &mut buf,
        )
        .unwrap();

        let script = String::from_utf8(buf).unwrap();
        assert!(script.contains("#!/usr/bin/env bash"));
        assert!(script.contains("set -euo pipefail"));
        // Files don't exist → group skipped
        assert_eq!(n, 0);
        assert!(script.contains("SKIPPED"));
    }
}
