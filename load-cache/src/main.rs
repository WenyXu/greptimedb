#![feature(array_chunks)]

pub mod generator;
pub mod loader;

use std::fmt::Display;

use clap::{Args, Parser, Subcommand};
use common_telemetry::logging::LoggingOptions;
use loader::Loader;

use crate::generator::Generator;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "INFO")]
    log_level: String,
    #[arg(long, default_value = "/tmp/load-cache/logs")]
    log_dir: String,
    #[command(subcommand)]
    command: Commands,
}

const ETCD_ADDR: &str = "127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379";

#[derive(Args)]
pub struct GeneratorArgs {
    #[arg(long, default_value = ETCD_ADDR)]
    etcd_addr: String,
    #[arg(long, default_value_t = 100000)]
    /// Generates data amount.
    amount: usize,
    /// Num region in TableRouteValue
    #[arg(long, default_value_t = 3)]
    region_num: usize,
}

#[derive(Args)]
pub struct LoadArgs {
    #[arg(long, default_value = ETCD_ADDR)]
    etcd_addr: String,
    #[arg(long, default_value = "__table_route")]
    prefix: String,
    #[arg(long, default_value_t = 1024)]
    page_size: usize,
}

#[derive(Subcommand)]
enum Commands {
    /// Generates and load data into Etcd.
    Generate(GeneratorArgs),
    /// Loads data from Etcd.
    Load(LoadArgs),
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Commands::Generate(_) => write!(f, "generator"),
            Commands::Load(_) => write!(f, "loader"),
        }
    }
}

impl Commands {
    pub fn build(self) -> Box<dyn Runnable> {
        match self {
            Commands::Generate(args) => Generator::new(args),
            Commands::Load(args) => Loader::new(args),
        }
    }
}

#[async_trait::async_trait]
pub trait Runnable: Send + Sync {
    async fn run(&self);
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let appname = cli.command.to_string();

    let _guard = common_telemetry::init_global_logging(
        &appname,
        &LoggingOptions {
            dir: cli.log_dir.to_string(),
            level: Some(cli.log_level.to_string()),
            enable_jaeger_tracing: false,
        },
        Default::default(),
    );

    let runnable = cli.command.build();

    runnable.run().await;
}
