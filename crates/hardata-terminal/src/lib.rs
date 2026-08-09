pub mod bootstrap;

use clap::{Parser, Subcommand};
use hardata_app::shared::error::Result;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(name = "hardata")]
#[command(about = "High-performance data transfer service", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Sync {
        #[arg(short = 'c', long, default_value = "config.yaml")]
        config: String,
    },

    Agent {
        #[arg(short = 'c', long, default_value = "config.yaml")]
        config: String,
    },

    Diff {
        #[arg(long, value_name = "DIR")]
        dir: String,
    },
}

/// Parse terminal arguments, initialize logging, and run the selected mode.
pub async fn run() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    match Cli::parse().command {
        Commands::Sync { config } => bootstrap::sync::run_sync(config).await,
        Commands::Agent { config } => bootstrap::agent::run_agent(config).await,
        Commands::Diff { dir } => hardata_tool_cli::run_diff(dir).await,
    }
}
