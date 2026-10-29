//! The exchange-independent core: workers, writer, connections, telemetry.

pub mod app;
pub mod book_connection;
pub mod capture;
pub mod config;
pub mod connection;
pub mod decode_log;
pub mod discovery;
pub mod fixture;
pub mod metrics;
pub mod metrics_http;
pub mod order_book;
pub mod pipeline;
pub mod rate_limit;
pub(crate) mod reconnect_backoff;
pub mod recovery;
pub mod registry;
pub mod routing;
pub mod stream;
pub mod subscriptions;
pub mod telemetry;
pub mod worker;
pub mod writer;
