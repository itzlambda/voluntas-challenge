//! The pipeline: workers and the output sink (file writer or stream hub),
//! plus the offline replay fed from a fixture. The
//! fixture replay plays the subscription manager's and connections' parts.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::core::config::{Config, OutputMode};
use crate::core::connection::{ConnectionIds, FrameTotals, handle_frame};
use crate::core::decode_log::DecodeErrorLog;
use crate::core::fixture::{FixtureEvent, ProtocolCommand};
use crate::core::metrics::{Component, Metrics};
use crate::core::routing::Workers;
use crate::core::stream::{StreamSettings, spawn_stream};
use crate::core::telemetry;
use crate::core::worker::{WorkerTotals, spawn_workers};
use crate::core::writer::{WriterTotals, spawn_writer};
use crate::exchange::protocol::BookProtocol;
use crate::model::{
    ConnectionId, MarketId, MarketSources, OrderBookId, UnsyncReason, WorkerMessage,
};

/// Workers plus the writer, running.
pub struct Pipeline {
    workers: Workers,
    worker_tasks: Vec<JoinHandle<anyhow::Result<WorkerTotals>>>,
    writer_done: oneshot::Receiver<anyhow::Result<WriterTotals>>,
}

#[derive(Debug, Clone, Default)]
pub struct PipelineTotals {
    pub workers: Vec<WorkerTotals>,
    pub writer: WriterTotals,
}

impl Pipeline {
    /// Starts the writer thread and the worker tasks on the current runtime.
    pub fn start(config: &Config) -> anyhow::Result<Self> {
        Self::start_with_metrics(config, &Metrics::default())
    }

    pub fn start_with_metrics(config: &Config, metrics: &Metrics) -> anyhow::Result<Self> {
        config.validate()?;
        let (writer_tx, writer_rx) = mpsc::channel(config.writer_queue);
        // The stream hub reports as the writer: it is the one output sink.
        let writer_metrics = metrics.register(Component::Writer);
        let writer_done = match config.output_mode {
            OutputMode::File => spawn_writer(config.output.clone(), writer_rx, writer_metrics)?,
            OutputMode::Stream => {
                let stream = spawn_stream(StreamSettings::from(config), writer_rx, writer_metrics)?;
                telemetry::emit(
                    "stream_available",
                    json!({"endpoint":format!("ws://{}",stream.address)}),
                );
                stream.done
            }
        };
        let (workers, worker_tasks) = spawn_workers(
            config.workers,
            config.worker_queue,
            &writer_tx,
            config.writer_stall_timeout,
            metrics,
        );
        Ok(Self {
            workers: Workers::from(workers),
            worker_tasks,
            writer_done,
        })
    }

    /// Producer clones must be dropped before `finish` can drain the workers.
    pub fn workers(&self) -> &Workers {
        &self.workers
    }

    /// Closes worker inputs, lets workers drain, then waits for the writer to
    /// drain and flush.
    pub async fn finish(self) -> anyhow::Result<PipelineTotals> {
        drop(self.workers);
        let mut totals = PipelineTotals::default();
        let mut failure = None;
        for (index, task) in self.worker_tasks.into_iter().enumerate() {
            match task
                .await
                .with_context(|| format!("worker {index} panicked"))
                .and_then(|result| result)
            {
                Ok(worker) => totals.workers.push(worker),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        // Workers have released their writer senders even if one failed.
        // Always drain and flush the writer before returning the original failure.
        let written = self
            .writer_done
            .await
            .context("writer thread exited")
            .and_then(|result| result);
        if let Some(error) = failure {
            return Err(error);
        }
        totals.writer = written?;
        Ok(totals)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplayTotals {
    pub pipeline: PipelineTotals,
    pub frames: FrameTotals,
}

/// Replays fixture events through the pipeline to `config.output`. Time
/// advances with `tokio::time::sleep`, so on a paused clock the run is
/// deterministic.
pub async fn replay<P: BookProtocol>(
    config: &Config,
    events: impl IntoIterator<Item = FixtureEvent>,
    make_protocol: impl Fn() -> P,
) -> anyhow::Result<ReplayTotals> {
    let decode_log = DecodeErrorLog::open(&config.decode_error_log)?;
    let pipeline = Pipeline::start(config)?;
    let mut connections: HashMap<ConnectionId, ConnectionIds> = HashMap::new();
    let mut placed: HashMap<MarketId, (MarketSources, Vec<OrderBookId>)> = HashMap::new();
    let mut frames = FrameTotals::default();
    let mut protocols: HashMap<ConnectionId, P> = HashMap::new();
    let mut explicit_sessions = HashSet::new();
    let mut faulted = HashSet::new();
    let mut pending_resets = HashMap::new();

    let mut failure = None;
    {
        let mut play = async |event: FixtureEvent| -> anyhow::Result<()> {
            match event {
                FixtureEvent::SessionStart { connection } => {
                    let id = ConnectionId(connection);
                    explicit_sessions.insert(id);
                    faulted.remove(&id);
                    pending_resets.remove(&id);
                    protocols
                        .entry(id)
                        .or_insert_with(&make_protocol)
                        .start_session();
                }
                FixtureEvent::ProtocolCommand {
                    connection,
                    command,
                } => {
                    let protocol = protocols
                        .entry(ConnectionId(connection))
                        .or_insert_with(&make_protocol);
                    match command {
                        ProtocolCommand::Subscribe { books, initial } => {
                            let ids = connections.entry(ConnectionId(connection)).or_default();
                            for market in &books {
                                ids.add(&market.market, &market.order_books);
                            }
                            protocol.subscribe(&books, initial);
                        }
                        ProtocolCommand::Unsubscribe { books } => {
                            protocol.unsubscribe(&books);
                        }
                        ProtocolCommand::PrepareBooks { books } => {
                            let _ = protocol.prepare_book_request(&books);
                            connections
                                .entry(ConnectionId(connection))
                                .or_default()
                                .pause_books(&books);
                        }
                        ProtocolCommand::RequestBooks { books } => {
                            protocol.request_books(&books);
                            connections
                                .entry(ConnectionId(connection))
                                .or_default()
                                .resume_books(&books);
                        }
                        ProtocolCommand::Heartbeat => {
                            protocol.heartbeat();
                        }
                    }
                }
                event @ (FixtureEvent::Frame { .. }
                | FixtureEvent::BinaryFrame { .. }
                | FixtureEvent::PingFrame { .. }
                | FixtureEvent::PongFrame { .. }) => {
                    let (connection, received_at, frame) = match event {
                        FixtureEvent::Frame {
                            connection,
                            received_at,
                            text,
                        } => (connection, received_at, Message::Text(text.into())),
                        FixtureEvent::BinaryFrame {
                            connection,
                            received_at,
                            bytes,
                        } => (connection, received_at, Message::Binary(bytes.into())),
                        FixtureEvent::PingFrame {
                            connection,
                            received_at,
                            bytes,
                        } => (connection, received_at, Message::Ping(bytes.into())),
                        FixtureEvent::PongFrame {
                            connection,
                            received_at,
                            bytes,
                        } => (connection, received_at, Message::Pong(bytes.into())),
                        _ => unreachable!(),
                    };
                    let connection = ConnectionId(connection);
                    if faulted.contains(&connection) {
                        return Ok(());
                    }
                    let ids = connections.entry(connection).or_default();
                    let protocol = protocols.entry(connection).or_insert_with(&make_protocol);
                    let outcome = handle_frame(
                        protocol,
                        &frame,
                        connection,
                        received_at,
                        std::time::Instant::now(),
                        ids,
                        pipeline.workers(),
                        config.max_frame_bytes,
                        &decode_log,
                        &mut frames,
                        |_| {},
                    )
                    .await?;
                    if let Some(reason) = outcome.fault
                        && explicit_sessions.contains(&connection)
                    {
                        faulted.insert(connection);
                        pending_resets.insert(connection, reason);
                    }
                }
                FixtureEvent::SourceAttached {
                    market,
                    generation,
                    connection,
                } => {
                    pipeline
                        .workers()
                        .send(WorkerMessage::SourceAttached {
                            market: market.as_str().into(),
                            generation,
                            connection: ConnectionId(connection),
                        })
                        .await?;
                }
                FixtureEvent::AddMarket {
                    generation,
                    market,
                    order_books,
                    connection,
                    standby,
                } => {
                    let market = MarketId::from(market.as_str());
                    let order_books: Vec<OrderBookId> = order_books
                        .iter()
                        .map(|id| OrderBookId::from(id.as_str()))
                        .collect();
                    let sources = match standby {
                        Some(standby) => MarketSources::mirrored(
                            ConnectionId(connection),
                            ConnectionId(standby),
                        )?,
                        None => MarketSources::single(ConnectionId(connection)),
                    };
                    if let Some((old, old_books)) = placed.remove(&market) {
                        for connection in old.connections() {
                            connections
                                .entry(*connection)
                                .or_default()
                                .remove(&market, &old_books);
                        }
                    }
                    let message = WorkerMessage::AddMarket {
                        generation,
                        market: market.clone(),
                        order_books: order_books.clone(),
                        sources,
                    };
                    pipeline.workers().send(message).await?;
                    for connection in sources.connections() {
                        connections
                            .entry(*connection)
                            .or_default()
                            .add(&market, &order_books);
                    }
                    placed.insert(market, (sources, order_books));
                }
                FixtureEvent::RemoveMarket { market } => {
                    let market = MarketId::from(market.as_str());
                    pipeline
                        .workers()
                        .send(WorkerMessage::RemoveMarket(market.clone()))
                        .await?;
                    if let Some((sources, order_books)) = placed.remove(&market) {
                        for connection in sources.connections() {
                            connections
                                .entry(*connection)
                                .or_default()
                                .remove(&market, &order_books);
                        }
                    }
                }
                FixtureEvent::MarkUnsynced { connection, reason } => {
                    let id = ConnectionId(connection);
                    // Skip the reset that handle_frame already emitted for this frame.
                    if pending_resets.get(&id) == Some(&reason) {
                        pending_resets.remove(&id);
                        return Ok(());
                    }
                    anyhow::ensure!(
                        reason != UnsyncReason::Disconnected
                            || !placed.values().any(|(sources, _)| sources.is_mirrored()
                                && sources.connections().contains(&ConnectionId(connection))),
                        "legacy disconnected reset is ambiguous for a mirrored fixture source; use an explicit failure reason"
                    );
                    let message = WorkerMessage::MarkUnsynced {
                        connection: ConnectionId(connection),
                        reason,
                    };
                    pipeline.workers().send(message).await?;
                }
                FixtureEvent::Advance { millis } => {
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                }
            }
            Ok(())
        };

        for event in events {
            if let Err(error) = play(event).await {
                failure = Some(error);
                break;
            }
        }
    }
    // A failed send means a worker stopped; its own error explains why.
    let finished = pipeline.finish().await;
    if let Some(error) = failure {
        return Err(finished.err().unwrap_or(error));
    }
    Ok(ReplayTotals {
        pipeline: finished?,
        frames,
    })
}
