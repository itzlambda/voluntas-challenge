use std::process::ExitCode;

use engineering_challenge::core::app;
use engineering_challenge::exchange::polymarket::config::Settings;

/// Collects Polymarket order books and writes SBE to stdout (or `--output`).
/// Diagnostics go to stderr as JSON lines. Stop with SIGINT (Ctrl+C).
#[tokio::main]
async fn main() -> ExitCode {
    let config = match Settings::from_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error:#}");
            return ExitCode::from(2);
        }
    };
    app::run(config).await
}
