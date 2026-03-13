mod cli;
mod db;
mod scan;
mod commands;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Scan { path, db, threads } => commands::scan(path, db, threads),
        Command::Dupes { db, min_size } => commands::dupes(db, min_size),
        Command::Find { hash, db } => commands::find(hash, db),
        Command::Stats { db } => commands::stats(db),
        Command::Stale { db } => commands::stale(db),
    }
}
