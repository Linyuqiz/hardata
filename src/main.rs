#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::{Parser, Subcommand};
use tracing::error;
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

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Sync { config } => {
            if let Err(e) = hardata::sync::run_sync(config).await {
                error!("Sync failed: {}", e);
                std::process::exit(1);
            }
        }

        Commands::Agent { config } => {
            if let Err(e) = hardata::agent::run_agent(config).await {
                error!("Agent failed: {}", e);
                std::process::exit(1);
            }
        }

        Commands::Diff { dir } => {
            if let Err(e) = hardata::diff::run_diff(dir).await {
                error!("Diff failed: {}", e);
                std::process::exit(1);
            }
        }
    }
}
