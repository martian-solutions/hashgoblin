use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "hashgoblin",
    about = "Resumable file hashing and duplicate detection",
    help_template = "hashgoblin — resumable file hashing and duplicate detection

Usage:
  hashgoblin scan   <PATH> [--db FILE] [--threads N]
  hashgoblin dupes  [--db FILE] [--min-size BYTES]
  hashgoblin find   <HASH|PATH> [--db FILE] [--threshold N]
  hashgoblin stats  [--db FILE]
  hashgoblin stale  [--db FILE]

Commands:
  scan   Walk PATH hashing every file; unchanged files (same inode/size/mtime)
         are skipped. --threads defaults to CPU count.
  dupes  Group files that share a SHA-256. --min-size BYTES (default 1) skips
         smaller files.
  find   HASH (64 hex chars): exact SHA-256 lookup.
         PATH: SHA-256 dupes for all files; PDQ perceptual near-dupes for
         images (Hamming distance <= --threshold bits, default 31).
  stats  Total files, size, duplicate groups, error count, stale count.
  stale  Files recorded in the DB but absent from the most recent scan.

  --db FILE defaults to hashgoblin.db in the current directory.

Options:
  -h, --help  Print this help
"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Scan/rescan a directory tree
    Scan {
        /// Directory to scan
        path: PathBuf,
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
        /// Number of hashing threads
        #[arg(long, default_value_t = num_cpus())]
        threads: usize,
    },
    /// Report duplicate files grouped by hash
    Dupes {
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
        /// Minimum file size to consider (bytes)
        #[arg(long, default_value_t = 1)]
        min_size: u64,
    },
    /// Look up files by SHA-256 hash or find duplicates/similar matches for a file path.
    ///
    /// If INPUT is a 64-character hex string it is treated as a SHA-256 hash and
    /// exact matches are returned. Otherwise INPUT is treated as a file path: exact
    /// SHA-256 duplicates are shown for all files, and perceptually similar images
    /// (via PDQ hash) are shown for image files.
    Find {
        /// SHA-256 hash (64 hex chars) or path to a file on disk
        input: String,
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
        /// Maximum PDQ Hamming distance to consider a perceptual match (0–256).
        /// Only used when INPUT is an image file path. The PDQ paper treats ≤ 31
        /// as "near-duplicate"; raise this value to cast a wider net.
        /// Ignored when --top is set.
        #[arg(long, default_value_t = 31)]
        threshold: u32,
        /// Return the N closest perceptual matches regardless of threshold.
        /// When set, --threshold is ignored and all images in the database are
        /// ranked by similarity; the N closest are shown. Use 0 (default) to
        /// use --threshold filtering instead.
        #[arg(long, default_value_t = 0)]
        top: usize,
    },
    /// Show summary statistics
    Stats {
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
    },
    /// List files marked stale (gone since last scan)
    Stale {
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
        /// Delete all stale records from the database after listing them
        #[arg(long)]
        purge: bool,
    },
    /// Generate a bash cleanup script that replaces duplicate files with links
    #[cfg(feature = "cleanup")]
    Cleanup {
        /// Path to write the generated bash script
        #[arg(long)]
        output: PathBuf,
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
        /// Minimum file size to consider (bytes)
        #[arg(long, default_value_t = 1)]
        min_size: u64,
    },
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
