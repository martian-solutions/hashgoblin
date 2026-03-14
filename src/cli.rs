use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "hashgoblin", about = "Resumable file hashing and duplicate detection")]
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
        #[arg(long, default_value_t = 31)]
        threshold: u32,
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
    },
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
