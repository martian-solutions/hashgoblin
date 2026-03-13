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
    /// Look up files by SHA-256 hash
    Find {
        /// SHA-256 hash to search for
        hash: String,
        /// SQLite database path
        #[arg(long, default_value = "hashgoblin.db")]
        db: PathBuf,
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
