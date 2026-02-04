mod command;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use enum_as_inner::EnumAsInner;

use attic_server::config;
use command::make_token::{self, MakeToken};
#[cfg(feature = "turso")]
use command::migrate_to_turso::{self, MigrateToTurso};

/// Attic server administration utilities.
#[derive(Debug, Parser)]
#[clap(version, author = "Zhaofeng Li <hello@zhaofeng.li>")]
#[clap(propagate_version = true)]
pub struct Opts {
    /// Path to the config file.
    #[clap(short = 'f', long, global = true)]
    config: Option<PathBuf>,

    /// The sub-command.
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, EnumAsInner)]
pub enum Command {
    MakeToken(MakeToken),
    #[cfg(feature = "turso")]
    MigrateToTurso(MigrateToTurso),
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let config = config::load_config(opts.config.as_deref(), false).await?;

    match opts.command {
        Command::MakeToken(_) => make_token::run(config, opts).await?,
        #[cfg(feature = "turso")]
        Command::MigrateToTurso(_) => migrate_to_turso::run(config, opts).await?,
    }

    Ok(())
}
