/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![forbid(unsafe_code)]

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use facet_server::{Config, serve};

#[derive(Debug, Parser)]
#[command(name = "facet", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let result = match Cli::parse().command {
        Command::Serve { config } => match Config::from_path(&config) {
            Ok(config) => serve(config).await,
            Err(error) => Err(error),
        },
    };

    if let Err(error) = result {
        eprintln!("Facet failed: {error}");
        std::process::exit(1);
    }
}
