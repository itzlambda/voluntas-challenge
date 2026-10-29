//! Collector configuration, from command-line flags with spec defaults.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;

/// Snapshot interval per order book: exactly 60 s, with no random spread.
pub const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
/// Chunk size of the SBE output. The supplied reader reads 64 MiB chunks.
pub const OUTPUT_CHUNK_BYTES: usize = 64 << 20;

/// Where records go. Exactly one sink runs, chosen at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    /// SBE in 64 MiB aligned chunks to `output` (the challenge contract).
    #[default]
    File,
    /// SBE over WebSocket to subscribed clients; nothing is written to `output`.
    Stream,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub metrics_bind: SocketAddr,
    /// Book-processing tasks. Defaults to the process's available CPU parallelism.
    pub workers: usize,
    pub markets_per_connection: usize,
    pub scan_interval: Duration,
    /// Message-count bound of each worker's input channel.
    pub worker_queue: usize,
    /// Batch-count bound of the writer channel; one input or timer produces one batch.
    pub writer_queue: usize,
    /// How long the writer channel may stay full before the process exits.
    pub writer_stall_timeout: Duration,
    /// Largest WebSocket frame accepted for decoding.
    pub max_frame_bytes: usize,
    pub output_mode: OutputMode,
    /// Where SBE output goes in file mode.
    pub output: PathBuf,
    /// WebSocket listen address in stream mode.
    pub stream_bind: SocketAddr,
    /// Encoded messages a stream client may have queued before it is disconnected.
    pub stream_client_queue: usize,
    /// Order books one stream client may subscribe to at once.
    pub stream_max_books: usize,
    /// Where frames that fail to decode are logged.
    pub decode_error_log: PathBuf,
    /// New WebSocket connections opened per second, at most.
    pub connection_opens_per_second: f64,
    /// If set, raw frames and controls are recorded here in the fixture format.
    pub capture: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            metrics_bind: SocketAddr::from(([127, 0, 0, 1], 9090)),
            workers: std::thread::available_parallelism().map_or(1, |count| count.get()),
            markets_per_connection: 256,
            scan_interval: Duration::from_secs(900),
            worker_queue: 65_536,
            writer_queue: 65_536,
            writer_stall_timeout: Duration::from_secs(30),
            max_frame_bytes: 16 << 20,
            output_mode: OutputMode::File,
            output: PathBuf::from("/dev/stdout"),
            stream_bind: SocketAddr::from(([127, 0, 0, 1], 9100)),
            stream_client_queue: 4096,
            stream_max_books: 1024,
            decode_error_log: PathBuf::from("decode-errors.jsonl"),
            connection_opens_per_second: 20.0,
            capture: None,
        }
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.workers > 0, "--workers must be positive");
        ensure!(
            self.markets_per_connection > 0,
            "--markets-per-connection must be positive"
        );
        ensure!(self.worker_queue > 0, "--worker-queue must be positive");
        ensure!(self.writer_queue > 0, "--writer-queue must be positive");
        ensure!(
            self.stream_client_queue > 0,
            "--stream-client-queue must be positive"
        );
        ensure!(
            self.stream_max_books > 0,
            "--stream-max-books must be positive"
        );
        ensure!(
            self.connection_opens_per_second.is_finite() && self.connection_opens_per_second > 0.,
            "rates must be positive and finite"
        );
        Ok(())
    }
}
