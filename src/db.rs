use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;

#[allow(dead_code)]
pub struct FileRecord {
    pub path: String,
    pub inode: Option<u64>,
    pub size: Option<u64>,
    pub mtime: Option<i64>,
    pub sha256: Option<String>,
    pub mime_type: Option<String>,
    pub file_desc: Option<String>,
    pub pdq_hash: Option<String>,
    pub last_seen: i64,
    pub error: Option<String>,
}

pub struct CachedRecord {
    pub inode: Option<u64>,
    pub size: Option<u64>,
    pub mtime: Option<i64>,
}

pub fn open(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open database: {}", db_path.display()))?;
    // Retry for up to 30 s when another connection holds a lock.  Without this,
    // the default timeout is 0 ms: any concurrent reader (e.g. `sqlite3` in
    // another terminal) would cause an immediate SQLITE_BUSY failure.
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    // Always migrate on open: CREATE TABLE IF NOT EXISTS is idempotent, and
    // this ensures commands like `dupes` or `stats` work correctly even if
    // the user hasn't run `scan` yet (rather than returning a confusing
    // "no such table: files" error).
    migrate(&conn)?;
    Ok(conn)
}

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS files (
            id          INTEGER PRIMARY KEY,
            path        TEXT NOT NULL UNIQUE,
            inode       INTEGER,
            size        INTEGER,
            mtime       INTEGER,
            sha256      TEXT,
            mime_type   TEXT,
            file_desc   TEXT,
            pdq_hash    TEXT,
            last_seen   INTEGER,
            stale       BOOLEAN DEFAULT 0,
            error       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sha256 ON files(sha256);",
    )?;
    // For databases created before pdq_hash was added, silently add the column.
    let _ = conn.execute_batch("ALTER TABLE files ADD COLUMN pdq_hash TEXT");
    Ok(())
}

pub fn get_cached(conn: &Connection, path: &str) -> Result<Option<CachedRecord>> {
    let mut stmt = conn.prepare_cached(
        "SELECT inode, size, mtime FROM files WHERE path = ?1",
    )?;
    let result = stmt.query_row(params![path], |row| {
        Ok(CachedRecord {
            inode: row.get(0)?,
            size: row.get(1)?,
            mtime: row.get(2)?,
        })
    });
    match result {
        Ok(r) => Ok(Some(r)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn upsert(conn: &Connection, rec: &FileRecord) -> Result<()> {
    conn.execute(
        "INSERT INTO files (path, inode, size, mtime, sha256, mime_type, file_desc, pdq_hash, last_seen, stale, error)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10)
         ON CONFLICT(path) DO UPDATE SET
             inode     = excluded.inode,
             size      = excluded.size,
             mtime     = excluded.mtime,
             sha256    = excluded.sha256,
             mime_type = excluded.mime_type,
             file_desc = excluded.file_desc,
             pdq_hash  = excluded.pdq_hash,
             last_seen = excluded.last_seen,
             stale     = 0,
             error     = excluded.error",
        params![
            rec.path,
            rec.inode,
            rec.size,
            rec.mtime,
            rec.sha256,
            rec.mime_type,
            rec.file_desc,
            rec.pdq_hash,
            rec.last_seen,
            rec.error,
        ],
    )?;
    Ok(())
}

pub fn touch_last_seen(conn: &Connection, path: &str, last_seen: i64) -> Result<()> {
    conn.execute(
        "UPDATE files SET last_seen = ?1, stale = 0 WHERE path = ?2",
        params![last_seen, path],
    )?;
    Ok(())
}

pub fn mark_stale(conn: &Connection, scan_start: i64) -> Result<u64> {
    let n = conn.execute(
        "UPDATE files SET stale = 1 WHERE last_seen < ?1 AND stale = 0",
        params![scan_start],
    )?;
    Ok(n as u64)
}

/// Per-file information needed to generate a cleanup script.
#[cfg(feature = "cleanup")]
pub struct DupeFileInfo {
    pub path: String,
    /// Modification time (seconds since Unix epoch) as stored in the database.
    /// `None` or `i64::MIN` means mtime was unavailable at scan time.
    pub mtime: Option<i64>,
    pub size: u64,
}

/// Return all files that belong to duplicate groups, grouped by sha256.
/// Used by the cleanup script generator; includes per-file mtime.
#[cfg(feature = "cleanup")]
pub fn query_dupes_for_cleanup(
    conn: &Connection,
    min_size: u64,
) -> Result<Vec<(String, Vec<DupeFileInfo>)>> {
    let mut stmt = conn.prepare(
        "SELECT f.sha256, f.path, f.mtime, f.size
         FROM files f
         INNER JOIN (
             SELECT sha256 FROM files
             WHERE sha256 IS NOT NULL AND stale = 0 AND size >= ?1
             GROUP BY sha256 HAVING COUNT(*) > 1
         ) dups ON f.sha256 = dups.sha256
         WHERE f.stale = 0
         ORDER BY f.sha256, f.path",
    )?;

    let rows = stmt.query_map(params![min_size], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, u64>(3)?,
        ))
    })?;

    let mut groups: Vec<(String, Vec<DupeFileInfo>)> = Vec::new();
    let mut cur_sha = String::new();
    for row in rows {
        let (sha256, path, mtime, size) = row?;
        if sha256 != cur_sha {
            groups.push((sha256.clone(), Vec::new()));
            cur_sha = sha256;
        }
        if let Some((_, files)) = groups.last_mut() {
            files.push(DupeFileInfo { path, mtime, size });
        }
    }
    Ok(groups)
}

pub struct DupeGroup {
    pub sha256: String,
    pub count: u64,
    pub paths: Vec<String>,
    pub size: u64,
}

pub fn query_dupes(conn: &Connection, min_size: u64) -> Result<Vec<DupeGroup>> {
    // Step 1: find sha256 values that appear more than once, ordered by
    // descending copy count then descending size.
    let mut meta_stmt = conn.prepare(
        "SELECT sha256, COUNT(*) as n, MAX(size)
         FROM files
         WHERE sha256 IS NOT NULL AND stale = 0 AND size >= ?1
         GROUP BY sha256
         HAVING n > 1
         ORDER BY n DESC, MAX(size) DESC",
    )?;
    let groups_meta: Vec<(String, u64, u64)> = meta_stmt
        .query_map(params![min_size], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, Option<u64>>(2)?.unwrap_or(0),
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Step 2: for each group, fetch the individual paths. Using a separate
    // query per group avoids the GROUP_CONCAT separator-collision bug: any
    // separator character is a legal part of a Linux filename, but building
    // a Vec via individual rows is always correct.
    let mut path_stmt = conn.prepare(
        "SELECT path FROM files WHERE sha256 = ?1 AND stale = 0 ORDER BY path",
    )?;

    let mut groups = Vec::with_capacity(groups_meta.len());
    for (sha256, count, size) in groups_meta {
        let paths: Vec<String> = path_stmt
            .query_map(params![sha256], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        groups.push(DupeGroup { sha256, count, paths, size });
    }
    Ok(groups)
}

pub fn query_by_hash(conn: &Connection, hash: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT path FROM files WHERE sha256 = ?1 AND stale = 0",
    )?;
    let rows = stmt.query_map(params![hash], |row| row.get(0))?;
    rows.collect::<Result<Vec<String>, _>>().map_err(Into::into)
}

pub struct Stats {
    pub total_files: u64,
    pub total_size: u64,
    pub dupe_groups: u64,
    pub dupe_files: u64,
    pub error_count: u64,
    pub stale_count: u64,
}

pub fn query_stats(conn: &Connection) -> Result<Stats> {
    let total_files: u64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE stale = 0",
        [],
        |r| r.get(0),
    )?;
    let total_size: u64 = conn.query_row(
        "SELECT COALESCE(SUM(size), 0) FROM files WHERE stale = 0",
        [],
        |r| r.get(0),
    )?;
    let (dupe_groups, dupe_files): (u64, u64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(n), 0) FROM (
             SELECT COUNT(*) as n FROM files
             WHERE sha256 IS NOT NULL AND stale = 0
             GROUP BY sha256 HAVING n > 1
         )",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let error_count: u64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE error IS NOT NULL AND stale = 0",
        [],
        |r| r.get(0),
    )?;
    let stale_count: u64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE stale = 1",
        [],
        |r| r.get(0),
    )?;
    Ok(Stats {
        total_files,
        total_size,
        dupe_groups,
        dupe_files,
        error_count,
        stale_count,
    })
}

/// Return all non-stale files that have a recorded error, ordered by path.
/// Each entry is `(path, error_message)`.
pub fn query_errors(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT path, error FROM files WHERE error IS NOT NULL AND stale = 0 ORDER BY path",
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

pub fn query_stale(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT path FROM files WHERE stale = 1 ORDER BY path")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    rows.collect::<Result<Vec<String>, _>>().map_err(Into::into)
}

/// Delete all stale file records from the database.
/// Returns the number of rows deleted.
pub fn purge_stale(conn: &Connection) -> Result<usize> {
    let n = conn.execute("DELETE FROM files WHERE stale = 1", [])?;
    Ok(n)
}

/// A file in the database that is perceptually similar to a query image.
pub struct SimilarFile {
    pub path: String,
    /// Hamming distance between the two PDQ hashes (0 = identical, 256 = maximally different).
    /// Facebook's PDQ paper treats ≤ 31 as "near-duplicate".
    pub distance: u32,
}

/// Convert a PDQ Hamming distance to a similarity percentage.
///
/// 100.0 = identical (distance 0), 0.0 = maximally different (distance 256).
/// Facebook's 31-bit near-duplicate threshold ≈ 87.9%.
pub fn pdq_similarity_pct(distance: u32) -> f32 {
    (256u32.saturating_sub(distance)) as f32 / 256.0 * 100.0
}

/// Find non-stale files whose PDQ hash is perceptually close to `query_hex`.
///
/// When `limit` is `None`, returns all files within `threshold` bits (Hamming distance),
/// sorted closest-first. When `limit` is `Some(n)`, `threshold` is ignored and the `n`
/// closest files across the entire database are returned instead.
pub fn query_similar_pdq(
    conn: &Connection,
    query_hex: &str,
    threshold: u32,
    limit: Option<usize>,
) -> Result<Vec<SimilarFile>> {
    let query_hash = pdq_hex_to_bytes(query_hex)
        .ok_or_else(|| anyhow::anyhow!("Invalid PDQ hash: {}", query_hex))?;

    let mut stmt = conn.prepare(
        "SELECT path, pdq_hash FROM files WHERE pdq_hash IS NOT NULL AND stale = 0",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    let effective_threshold = if limit.is_some() { 256 } else { threshold };
    let mut results = Vec::new();
    for row in rows {
        let (path, stored_hex) = row?;
        if let Some(stored_hash) = pdq_hex_to_bytes(&stored_hex) {
            let dist = hamming_distance(&query_hash, &stored_hash);
            if dist <= effective_threshold {
                results.push(SimilarFile { path, distance: dist });
            }
        }
    }
    results.sort_by_key(|r| r.distance);
    if let Some(n) = limit {
        results.truncate(n);
    }
    Ok(results)
}

/// Parse a 64-character hex string into 32 bytes.
fn pdq_hex_to_bytes(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Some(bytes)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Count the number of bit positions that differ between two 256-bit PDQ hashes.
fn hamming_distance(a: &[u8; 32], b: &[u8; 32]) -> u32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x ^ y).count_ones()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn
    }

    fn make_record(path: &str, sha256: Option<&str>, pdq_hash: Option<&str>, last_seen: i64) -> FileRecord {
        FileRecord {
            path: path.to_string(),
            inode: Some(1),
            size: Some(100),
            mtime: Some(1000),
            sha256: sha256.map(String::from),
            mime_type: Some("image/png".to_string()),
            file_desc: Some("image/png".to_string()),
            pdq_hash: pdq_hash.map(String::from),
            last_seen,
            error: None,
        }
    }

    // 64-char hex strings for known Hamming distances from ALL_ZEROS.
    const ALL_ZEROS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    // byte 0 = 0x01  →  1 bit differs
    const DIST_1: &str    = "0100000000000000000000000000000000000000000000000000000000000000";
    // byte 0 = 0xff  →  8 bits differ
    const DIST_8: &str    = "ff00000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn migrate_is_idempotent() {
        let conn = setup();
        migrate(&conn).unwrap();
    }

    #[test]
    fn upsert_and_get_cached() {
        let conn = setup();
        upsert(&conn, &make_record("/foo/bar.txt", Some("abc"), None, 1000)).unwrap();
        let cached = get_cached(&conn, "/foo/bar.txt").unwrap().unwrap();
        assert_eq!(cached.inode, Some(1));
        assert_eq!(cached.size, Some(100));
        assert_eq!(cached.mtime, Some(1000));
    }

    #[test]
    fn get_cached_returns_none_for_missing() {
        let conn = setup();
        assert!(get_cached(&conn, "/nonexistent").unwrap().is_none());
    }

    #[test]
    fn upsert_updates_existing_record() {
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("hash1"), None, 1000)).unwrap();

        let mut updated = make_record("/a", Some("hash2"), Some(ALL_ZEROS), 2000);
        updated.size = Some(999);
        upsert(&conn, &updated).unwrap();

        let cached = get_cached(&conn, "/a").unwrap().unwrap();
        assert_eq!(cached.size, Some(999));

        let paths = query_by_hash(&conn, "hash2").unwrap();
        assert_eq!(paths, vec!["/a"]);
    }

    #[test]
    fn query_by_hash_finds_matches() {
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("deadbeef"), None, 1000)).unwrap();
        upsert(&conn, &make_record("/b", Some("deadbeef"), None, 1000)).unwrap();
        upsert(&conn, &make_record("/c", Some("cafebabe"), None, 1000)).unwrap();

        let mut paths = query_by_hash(&conn, "deadbeef").unwrap();
        paths.sort();
        assert_eq!(paths, vec!["/a", "/b"]);
    }

    #[test]
    fn query_by_hash_is_case_sensitive() {
        // Documents that SQLite TEXT comparison is case-sensitive: an uppercase
        // hash never matches a lowercase-stored hash. The fix lives in
        // commands::find, which normalises input to lowercase before querying.
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("deadbeef"), None, 1000)).unwrap();
        assert!(query_by_hash(&conn, "DEADBEEF").unwrap().is_empty());
    }

    #[test]
    fn query_dupes_groups_by_hash() {
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("aaa"), None, 1000)).unwrap();
        upsert(&conn, &make_record("/b", Some("aaa"), None, 1000)).unwrap();
        upsert(&conn, &make_record("/c", Some("bbb"), None, 1000)).unwrap();

        let groups = query_dupes(&conn, 0).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].sha256, "aaa");
        assert_eq!(groups[0].count, 2);
    }

    #[test]
    fn query_dupes_path_with_pipe_character() {
        // Regression: the old GROUP_CONCAT('|') approach split incorrectly on
        // paths that contain '|', a legal Linux filename character.
        let conn = setup();
        upsert(&conn, &make_record("/foo/bar|baz.txt", Some("aaa"), None, 1000)).unwrap();
        upsert(&conn, &make_record("/foo/qux.txt",     Some("aaa"), None, 1000)).unwrap();

        let groups = query_dupes(&conn, 0).unwrap();
        assert_eq!(groups.len(), 1);
        let mut paths = groups[0].paths.clone();
        paths.sort();
        assert_eq!(paths, vec!["/foo/bar|baz.txt", "/foo/qux.txt"]);
    }

    #[test]
    fn query_dupes_excludes_stale() {
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("aaa"), None, 100)).unwrap();
        upsert(&conn, &make_record("/b", Some("aaa"), None, 100)).unwrap();
        mark_stale(&conn, 500).unwrap();
        assert!(query_dupes(&conn, 0).unwrap().is_empty());
    }

    #[test]
    fn mark_stale_marks_old_files() {
        let conn = setup();
        upsert(&conn, &make_record("/old", Some("x"), None, 100)).unwrap();
        upsert(&conn, &make_record("/new", Some("y"), None, 1000)).unwrap();
        let n = mark_stale(&conn, 500).unwrap();
        assert_eq!(n, 1);

        let stale = query_stale(&conn).unwrap();
        assert_eq!(stale, vec!["/old"]);
    }

    #[test]
    fn mark_stale_with_zero_scan_start_marks_nothing() {
        // If now_unix() ever silently returned 0 (e.g. misconfigured clock),
        // mark_stale would run with scan_start=0. No file has last_seen < 0,
        // so zero files would be marked stale — a silent no-op. now_unix()
        // now returns an error in this case, but this test documents the
        // underlying database behaviour.
        let conn = setup();
        upsert(&conn, &make_record("/a", Some("x"), None, 100)).unwrap();
        let n = mark_stale(&conn, 0).unwrap();
        assert_eq!(n, 0, "scan_start=0 should mark no files stale");
    }

    #[test]
    fn query_similar_pdq_exact_match() {
        let conn = setup();
        upsert(&conn, &make_record("/img", Some("h"), Some(ALL_ZEROS), 1000)).unwrap();

        let results = query_similar_pdq(&conn, ALL_ZEROS, 0, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "/img");
        assert_eq!(results[0].distance, 0);
    }

    #[test]
    fn query_similar_pdq_near_match_within_threshold() {
        let conn = setup();
        upsert(&conn, &make_record("/img", Some("h"), Some(DIST_1), 1000)).unwrap();

        assert!(query_similar_pdq(&conn, ALL_ZEROS, 0, None).unwrap().is_empty());
        let r = query_similar_pdq(&conn, ALL_ZEROS, 1, None).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].distance, 1);
    }

    #[test]
    fn query_similar_pdq_sorted_by_distance() {
        let conn = setup();
        upsert(&conn, &make_record("/far",  Some("h1"), Some(DIST_8), 1000)).unwrap();
        upsert(&conn, &make_record("/near", Some("h2"), Some(DIST_1), 1000)).unwrap();

        let results = query_similar_pdq(&conn, ALL_ZEROS, 10, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].path, "/near");
        assert_eq!(results[0].distance, 1);
        assert_eq!(results[1].path, "/far");
        assert_eq!(results[1].distance, 8);
    }

    #[test]
    fn query_similar_pdq_excludes_stale() {
        let conn = setup();
        upsert(&conn, &make_record("/stale_img", Some("h"), Some(ALL_ZEROS), 100)).unwrap();
        mark_stale(&conn, 500).unwrap();
        assert!(query_similar_pdq(&conn, ALL_ZEROS, 256, None).unwrap().is_empty());
    }

    #[test]
    fn hamming_distance_known_values() {
        let zeros = [0u8; 32];
        let mut one_bit = [0u8; 32];
        one_bit[0] = 0x01;
        let mut eight_bits = [0u8; 32];
        eight_bits[0] = 0xff;
        let all_ones = [0xffu8; 32];

        assert_eq!(hamming_distance(&zeros, &zeros), 0);
        assert_eq!(hamming_distance(&zeros, &one_bit), 1);
        assert_eq!(hamming_distance(&zeros, &eight_bits), 8);
        assert_eq!(hamming_distance(&zeros, &all_ones), 256);
    }
}
