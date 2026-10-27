//! Owns service startup, component supervision, and bounded shutdown.
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::core::book_connection::{BookConnectionContext, SILENCE_LIMIT, run_book_connection};
use crate::core::capture::Capture;
use crate::core::decode_log::DecodeErrorLog;
use crate::core::discovery::Discovery;
use crate::core::metrics::{Component, ConnectionKind, Metrics};
use crate::core::metrics_http::MetricsServer;
use crate::core::pipeline::Pipeline;
use crate::core::rate_limit::RateLimiter;
use crate::core::subscriptions::{SpawnConnection, SubscriptionManager};
use crate::core::telemetry;
use crate::exchange::polymarket::config::Settings;
use crate::exchange::polymarket::gamma::GammaClient;
use crate::exchange::polymarket::lifecycle::run_lifecycle_connection;

const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

pub async fn run(config: Settings) -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let code = match run_collector(config).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            telemetry::emit("fatal", json!({"error":format!("{error:#}")}));
            ExitCode::FAILURE
        }
    };
    telemetry::flush(Duration::from_secs(2));
    code
}
async fn run_collector(settings: Settings) -> anyhow::Result<bool> {
    settings.validate()?;
    let config = &settings.collector;
    let cancel = CancellationToken::new();
    let fatal = CancellationToken::new();
    let metrics = Metrics::new(fatal.clone());
    // Bind before creating the output or opening exchange connections.
    let mut server =
        MetricsServer::start(config.metrics_bind, metrics.clone(), fatal.clone()).await?;
    telemetry::emit(
        "metrics_available",
        json!({"endpoint":format!("http://{}/metrics",server.address),"history":"Prometheus HTTP API; operational statistics are not printed at shutdown"}),
    );
    let (capture, capture_file) = match &config.capture {
        Some(path) => {
            let (capture, file) = Capture::create(path)?;
            (Some(capture), Some(file))
        }
        None => (None, None),
    };
    let pipeline = Pipeline::start_with_metrics(config, &metrics)?;
    let open_limiter = Arc::new(RateLimiter::per_second(config.connection_opens_per_second));
    let tracker = TaskTracker::new();
    let (recovery_tx, recovery_rx) = mpsc::channel(1024);
    let spawn: SpawnConnection = {
        let ctx = Arc::new(BookConnectionContext {
            recovery: Some(recovery_tx),
            workers: pipeline.workers().clone(),
            decode_log: Arc::new(DecodeErrorLog::open(&config.decode_error_log)?),
            open_limiter: open_limiter.clone(),
            capture: capture.clone(),
            cancel: cancel.clone(),
            max_frame_bytes: config.max_frame_bytes,
            silence_limit: SILENCE_LIMIT,
            metrics: metrics.clone(),
        });
        let protocol =
            crate::exchange::polymarket::protocol::PolymarketProtocol::new(settings.ws_url.clone());
        let tracker = tracker.clone();
        let fatal = fatal.clone();
        Box::new(move |id| {
            let (commands, rx) = mpsc::unbounded_channel();
            let ctx = ctx.clone();
            let protocol = protocol.clone();
            let fatal = fatal.clone();
            tracker.spawn(async move {
                let guard = fatal.drop_guard();
                if let Err(error) = run_book_connection(id, rx, ctx, protocol).await {
                    telemetry::emit(
                        "fatal",
                        json!({"component":"orderbook_connection","error":format!("{error:#}")}),
                    );
                } else {
                    guard.disarm();
                }
            });
            commands
        })
    };
    let manager = SubscriptionManager::new(
        pipeline.workers().clone(),
        config.markets_per_connection,
        spawn,
        capture.clone(),
    );
    drop(capture);
    let (manager_tx, manager_rx) = mpsc::channel(64);
    let mut manager_task = tokio::spawn(manager.run(manager_rx, cancel.clone()));
    let discovery = Discovery::new(
        GammaClient::new(&settings.gamma_url, settings.gamma_requests_per_second)?,
        manager_tx,
        config.scan_interval,
        metrics.register(Component::Discovery),
    )
    .with_recovery(recovery_rx);
    let mut discovery_connections = Vec::new();
    let mut discovery_task = if settings.markets.is_empty() {
        let (events, rx) = mpsc::unbounded_channel();
        for index in 0..settings.lifecycle_connections {
            let task = run_lifecycle_connection(
                index,
                settings.ws_url.clone(),
                open_limiter.clone(),
                events.clone(),
                cancel.clone(),
                metrics.register(Component::Connection(ConnectionKind::MarketDiscovery)),
            );
            let failure = fatal.clone();
            let stopping = cancel.clone();
            discovery_connections.push(tokio::spawn(async move {
                let guard = failure.drop_guard();
                let totals = task.await;
                if stopping.is_cancelled() {
                    guard.disarm();
                }
                totals
            }));
        }
        tokio::spawn(discovery.run(rx, cancel.clone()))
    } else {
        tokio::spawn(discovery.run_fixed(settings.listing_ids(), cancel.clone()))
    };
    telemetry::emit(
        "started",
        json!({"workers":config.workers,"markets_per_connection":config.markets_per_connection,"fixed_markets":settings.markets.len()}),
    );
    let mut discovery_done = false;
    let mut manager_done = false;
    let mut clean = tokio::select! {
        result = shutdown_signal() => { result?; true }
        _ = fatal.cancelled() => false,
        result = &mut discovery_task => { discovery_done = true; telemetry::emit("fatal",json!({"component":"discovery","result":format!("{result:?}")})); false }
        result = &mut manager_task => { manager_done = true; telemetry::emit("fatal",json!({"component":"subscription_manager","result":format!("{result:?}")})); false }
    };
    server.begin_shutdown();
    telemetry::emit("shutdown_started", json!({"clean":clean}));
    cancel.cancel();
    let second_signal = tokio::spawn(async {
        if shutdown_signal().await.is_ok() {
            telemetry::emit("forced_exit", json!({"reason":"second shutdown signal"}));
            telemetry::flush(Duration::from_secs(1));
            std::process::exit(130);
        }
    });
    let result = timeout(SHUTDOWN_DEADLINE, async {
        if !discovery_done {
            clean &= discovery_task.await.context("discovery task")?.is_ok();
        }
        if !manager_done {
            clean &= manager_task
                .await
                .context("subscription manager task")?
                .is_ok();
        }
        for task in discovery_connections {
            task.await.context("market discovery connection task")?;
        }
        tracker.close();
        tracker.wait().await;
        pipeline.finish().await?;
        if let Some(file) = capture_file {
            file.finish()?;
        }
        server.finish().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await;
    second_signal.abort();
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            clean = false;
            telemetry::emit(
                "fatal",
                json!({"component":"shutdown","error":format!("{error:#}")}),
            );
        }
        Err(_) => {
            clean = false;
            telemetry::emit(
                "forced_exit",
                json!({"reason":"shutdown deadline","deadline_s":SHUTDOWN_DEADLINE.as_secs()}),
            );
        }
    }
    telemetry::emit(
        "shutdown_completed",
        json!({"clean":clean && !fatal.is_cancelled()}),
    );
    Ok(clean && !fatal.is_cancelled())
}
async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("listen for SIGTERM")?;
        tokio::select! { result = tokio::signal::ctrl_c() => result.context("listen for SIGINT"), _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("listen for SIGINT")
    }
}
