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
    pub last_seen: i64,
    pub stale: bool,
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
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
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
            last_seen   INTEGER,
            stale       BOOLEAN DEFAULT 0,
            error       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sha256 ON files(sha256);",
    )?;
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
        "INSERT INTO files (path, inode, size, mtime, sha256, mime_type, file_desc, last_seen, stale, error)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9)
         ON CONFLICT(path) DO UPDATE SET
             inode     = excluded.inode,
             size      = excluded.size,
             mtime     = excluded.mtime,
             sha256    = excluded.sha256,
             mime_type = excluded.mime_type,
             file_desc = excluded.file_desc,
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

pub struct DupeGroup {
    pub sha256: String,
    pub count: u64,
    pub paths: Vec<String>,
    pub size: u64,
}

pub fn query_dupes(conn: &Connection, min_size: u64) -> Result<Vec<DupeGroup>> {
    let mut stmt = conn.prepare(
        "SELECT sha256, COUNT(*) as n, GROUP_CONCAT(path, '|'), MAX(size)
         FROM files
         WHERE sha256 IS NOT NULL AND stale = 0 AND size >= ?1
         GROUP BY sha256
         HAVING n > 1
         ORDER BY n DESC, MAX(size) DESC",
    )?;
    let rows = stmt.query_map(params![min_size], |row| {
        let paths_str: String = row.get(2)?;
        Ok(DupeGroup {
            sha256: row.get(0)?,
            count: row.get::<_, u64>(1)?,
            paths: paths_str.split('|').map(String::from).collect(),
            size: row.get::<_, Option<u64>>(3)?.unwrap_or(0),
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
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

pub fn query_stale(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT path FROM files WHERE stale = 1 ORDER BY path")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    rows.collect::<Result<Vec<String>, _>>().map_err(Into::into)
}
