//! Polymarket endpoints, discovery settings, and the collector CLI.
use crate::core::config::{Config, OutputMode};
use crate::model::ListingId;
use anyhow::{Context, bail, ensure};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Settings {
    pub collector: Config,
    pub ws_url: String,
    pub gamma_url: String,
    pub gamma_requests_per_second: f64,
    pub lifecycle_connections: usize,
    pub markets: Vec<u64>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            collector: Config::default(),
            ws_url: crate::exchange::polymarket::ws::MARKET_WS_URL.to_owned(),
            gamma_url: crate::exchange::polymarket::gamma::GAMMA_URL.to_owned(),
            gamma_requests_per_second: 10.,
            lifecycle_connections: 3,
            markets: Vec::new(),
        }
    }
}
impl Settings {
    pub fn listing_ids(&self) -> Vec<ListingId> {
        self.markets
            .iter()
            .map(|id| ListingId(id.to_string()))
            .collect()
    }
}

const USAGE: &str = "usage: engineering-challenge [--workers N] [--markets-per-connection N] \
[--scan-interval-secs S] [--worker-queue N] [--writer-queue N] [--writer-stall-timeout-secs S] \
[--max-frame-bytes N] [--output-mode file|stream] [--output PATH] [--stream-bind ADDRESS] \
[--stream-client-queue N] [--stream-max-books N] [--decode-error-log PATH] [--ws-url URL] [--gamma-url URL] \
[--gamma-requests-per-second R] [--connection-opens-per-second R] [--lifecycle-connections N] \
[--markets GAMMA_ID,...] [--capture PATH] [--metrics-bind ADDRESS]";

impl Settings {
    /// Parses `--flag value` pairs on top of the defaults.
    pub fn from_args(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .with_context(|| format!("missing value for {flag}\n{USAGE}"))?;
            let number = || -> anyhow::Result<u64> {
                value
                    .parse()
                    .with_context(|| format!("{flag} expects a number, got {value:?}"))
            };
            match flag.as_str() {
                "--metrics-bind" => {
                    config.collector.metrics_bind =
                        value.parse().context("--metrics-bind expects IP:port")?
                }
                "--workers" => config.collector.workers = number()? as usize,
                "--markets-per-connection" => {
                    config.collector.markets_per_connection = number()? as usize
                }
                "--scan-interval-secs" => {
                    config.collector.scan_interval = Duration::from_secs(number()?)
                }
                "--worker-queue" => config.collector.worker_queue = number()? as usize,
                "--writer-queue" => config.collector.writer_queue = number()? as usize,
                "--writer-stall-timeout-secs" => {
                    config.collector.writer_stall_timeout = Duration::from_secs(number()?)
                }
                "--max-frame-bytes" => config.collector.max_frame_bytes = number()? as usize,
                "--output-mode" => {
                    config.collector.output_mode = match value.as_str() {
                        "file" => OutputMode::File,
                        "stream" => OutputMode::Stream,
                        _ => bail!("--output-mode expects file or stream, got {value:?}"),
                    }
                }
                "--output" => config.collector.output = value.into(),
                "--stream-bind" => {
                    config.collector.stream_bind =
                        value.parse().context("--stream-bind expects IP:port")?
                }
                "--stream-client-queue" => {
                    config.collector.stream_client_queue = number()? as usize
                }
                "--stream-max-books" => config.collector.stream_max_books = number()? as usize,
                "--decode-error-log" => config.collector.decode_error_log = value.into(),
                "--ws-url" => config.ws_url = value,
                "--gamma-url" => config.gamma_url = value,
                "--gamma-requests-per-second" => {
                    config.gamma_requests_per_second = value
                        .parse()
                        .with_context(|| format!("{flag} expects a number, got {value:?}"))?
                }
                "--connection-opens-per-second" => {
                    config.collector.connection_opens_per_second = value
                        .parse()
                        .with_context(|| format!("{flag} expects a number, got {value:?}"))?
                }
                "--lifecycle-connections" => config.lifecycle_connections = number()? as usize,
                "--markets" => {
                    config.markets = value
                        .split(',')
                        .map(|id| id.trim().parse())
                        .collect::<Result<_, _>>()
                        .with_context(|| format!("--markets expects Gamma IDs, got {value:?}"))?
                }
                "--capture" => config.collector.capture = Some(value.into()),
                _ => bail!("unknown flag {flag}\n{USAGE}"),
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.collector.validate()?;
        let url = reqwest::Url::parse(&self.ws_url)
            .map_err(|_| anyhow::anyhow!("--ws-url must be a valid WebSocket URL"))?;
        ensure!(
            matches!(url.scheme(), "ws" | "wss")
                && url.host_str().is_some()
                && url.fragment().is_none(),
            "--ws-url requires ws or wss, a host, and no fragment"
        );
        let url = reqwest::Url::parse(&self.gamma_url)
            .map_err(|_| anyhow::anyhow!("--gamma-url must be a valid HTTP URL"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "--gamma-url requires http or https and a host"
        );
        ensure!(
            self.gamma_requests_per_second.is_finite() && self.gamma_requests_per_second > 0.,
            "rates must be positive and finite"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::exchange::polymarket::config::Settings;
    use std::path::PathBuf;

    fn parse(args: &[&str]) -> anyhow::Result<Settings> {
        Settings::from_args(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn defaults_and_flags() {
        let config = parse(&["--workers", "8", "--output", "out.sbe", "--markets", "5,6"]).unwrap();
        assert_eq!(config.markets, [5, 6]);
        assert_eq!(config.collector.workers, 8);
        assert_eq!(config.collector.output, PathBuf::from("out.sbe"));
        assert_eq!(config.collector.markets_per_connection, 256);
        assert_eq!(
            config.collector.scan_interval,
            std::time::Duration::from_secs(900)
        );
        assert_eq!(config.gamma_requests_per_second, 10.0);
    }

    #[test]
    fn output_mode_defaults_to_file() {
        use crate::core::config::OutputMode;
        assert_eq!(parse(&[]).unwrap().collector.output_mode, OutputMode::File);
        let config = parse(&["--output-mode", "stream", "--stream-bind", "0.0.0.0:9200"]).unwrap();
        assert_eq!(config.collector.output_mode, OutputMode::Stream);
        assert_eq!(config.collector.stream_bind.port(), 9200);
        assert!(parse(&["--output-mode", "both"]).is_err());
        assert!(parse(&["--stream-client-queue", "0"]).is_err());
    }

    #[test]
    fn accepts_non_power_of_two_and_large_worker_counts() {
        for workers in [3, 6, 24, 32, 64] {
            let config = parse(&["--workers", &workers.to_string()]).unwrap();
            assert_eq!(config.collector.workers, workers);
        }
    }

    #[test]
    fn rejects_bad_worker_counts() {
        for workers in ["0", "-1", "not-a-number"] {
            assert!(parse(&["--workers", workers]).is_err(), "{workers}");
        }
        assert!(parse(&["--bogus", "1"]).is_err());
        assert!(parse(&["--workers"]).is_err());
    }
}
