use clap::Parser;
use fake_exchange::{config::Config, server::Server};

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    config.validate()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.cores)
        .event_interval(4)
        .global_queue_interval(4)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let server = Server::start(config).await?;
        eprintln!(
            "{}",
            serde_json::json!({"event":"ready","ws":server.ws_addr,"gamma":server.gamma_addr})
        );
        tokio::signal::ctrl_c().await?;
        server.shutdown().await
    })
}
