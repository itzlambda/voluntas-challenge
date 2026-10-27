use std::path::PathBuf;

use anyhow::ensure;
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(about = "Synthetic loopback Polymarket exchange")]
pub struct Config {
    #[arg(long, default_value_t = 9001)]
    pub ws_port: u16,
    #[arg(long, default_value_t = 9002)]
    pub gamma_port: u16,
    #[arg(long, default_value_t = 2)]
    pub cores: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(long, default_value_t = 200)]
    pub markets: u64,
    #[arg(long, default_value_t = 5)]
    pub levels_min: usize,
    #[arg(long, default_value_t = 15)]
    pub levels_max: usize,
    /// Total true level changes/s, including mirrored entries.
    #[arg(long, default_value_t = 1000.0)]
    pub rate: f64,
    #[arg(long, default_value_t = 1.2)]
    pub zipf: f64,
    #[arg(long, default_value_t = 1)]
    pub entries_min: usize,
    #[arg(long, default_value_t = 4)]
    pub entries_max: usize,
    /// Probability that a generated change also changes the opposite outcome.
    #[arg(long, default_value_t = 0.25)]
    pub mirror_probability: f64,
    /// Total unsolicited books/s, selected using the same market weights.
    #[arg(long, default_value_t = 2.0)]
    pub repeat_book_rate: f64,
    /// Total noise frames/s (round-robin through three ignored event types).
    #[arg(long, default_value_t = 1.0)]
    pub noise_rate: f64,
    #[arg(long, default_value_t = 0.0)]
    pub new_markets_per_second: f64,
    #[arg(long, default_value_t = 0.0)]
    pub resolutions_per_second: f64,
    #[arg(long, default_value_t = 4096)]
    pub outbound_capacity: usize,
    /// Directory must be absent or empty. Omit to disable history.
    #[arg(long)]
    pub history: Option<PathBuf>,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.cores > 0 && self.cores <= 64, "cores must be 1..=64");
        ensure!(
            self.markets > 0 && self.markets < u64::MAX / 2,
            "invalid market count"
        );
        ensure!(
            self.levels_min <= self.levels_max && self.levels_max <= 49,
            "levels must be 0..=49 in order"
        );
        ensure!(
            self.entries_min > 0
                && self.entries_min <= self.entries_max
                && self.entries_max <= 1000,
            "entries must be 1..=1000 in order"
        );
        ensure!(
            self.outbound_capacity > 0,
            "outbound capacity must be positive"
        );
        for rate in [
            self.rate,
            self.repeat_book_rate,
            self.noise_rate,
            self.new_markets_per_second,
            self.resolutions_per_second,
            self.zipf,
        ] {
            ensure!(
                rate.is_finite() && (0.0..=1e9).contains(&rate),
                "rates and Zipf exponent must be finite, non-negative and <= 1e9"
            );
        }
        ensure!(
            (0.0..=1.0).contains(&self.mirror_probability),
            "mirror probability must be in [0,1]"
        );
        Ok(())
    }
}
