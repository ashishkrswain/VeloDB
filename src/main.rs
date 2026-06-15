mod config;
mod error;
mod resp;
mod net;
mod cmd;
mod store;
mod server;

use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(name = "velodb-server", version = "0.1.0")]
struct Args {
    #[arg(short, long, default_value = "velodb.conf")]
    config: String,
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("velodb=info".parse()?))
        .init();

    let args = Args::parse();
    let mut config = config::load(&args.config)?;
    if let Some(port) = args.port {
        config.port = port;
    }

    tracing::info!("VeloDB {} starting", env!("CARGO_PKG_VERSION"));
    server::run(config).await?;
    Ok(())
}
