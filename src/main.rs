mod cli;
#[cfg(feature = "cleanup")]
mod cleanup;
mod db;
mod scan;
mod commands;
#[cfg(feature = "gui")]
mod gui;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

fn main() -> Result<()> {
    // Launch the GUI when built with the `gui` feature and no subcommand is given.
    #[cfg(feature = "gui")]
    if std::env::args().len() == 1 {
        return gui::run();
    }

    let cli = Cli::parse();
    match cli.command {
        Command::Scan { path, db, threads } => commands::scan(path, db, threads),
        Command::Dupes { db, min_size } => commands::dupes(db, min_size),
        Command::Find { input, db, threshold } => commands::find(input, db, threshold),
        Command::Stats { db } => commands::stats(db),
        Command::Stale { db } => commands::stale(db),
        #[cfg(feature = "cleanup")]
        Command::Cleanup { db, output, min_size } => commands::cleanup_script(db, output, min_size),
    }
}
