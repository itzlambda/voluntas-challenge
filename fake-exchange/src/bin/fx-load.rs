use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, ensure};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use fake_exchange::book::now_ms;
use fake_exchange::check::Latency;

#[derive(Parser)]
#[command(about = "Null WebSocket client; parses event headers and skips level payloads")]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:9001/ws/market")]
    ws_url: String,
    #[arg(long, default_value = "http://127.0.0.1:9002")]
    gamma_url: String,
    #[arg(long, default_value_t = 50)]
    connections: usize,
    #[arg(long, default_value_t = 200)]
    assets_per_connection: usize,
    #[arg(long, default_value_t = 20)]
    seconds: u64,
    #[arg(long, default_value_t = 1)]
    cores: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    ensure!(
        args.connections > 0
            && args.assets_per_connection > 0
            && args.seconds > 0
            && args.cores > 0,
        "counts must be positive"
    );
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.cores)
        .event_interval(4)
        .global_queue_interval(4)
        .enable_all()
        .build()?
        .block_on(run(args))
}

#[derive(Default)]
struct Delays {
    all: Latency,
    books: Latency,
    other: Latency,
}
impl Delays {
    fn merge(&mut self, other: Self) {
        self.all.merge(other.all);
        self.books.merge(other.books);
        self.other.merge(other.other);
    }
}

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    event_type: Option<&'a str>,
    #[serde(borrow)]
    timestamp: Option<&'a str>,
}

fn record_event(event: Event<'_>, received: i64, latency: &mut Delays, books: &mut u64) {
    if event.event_type == Some("book") {
        *books += 1;
    }
    if let Some(time) = event.timestamp.and_then(|s| s.parse::<i64>().ok()) {
        let delay = (received - time) * 1000;
        latency.all.add_us(delay);
        if event.event_type == Some("book") {
            latency.books.add_us(delay);
        } else {
            latency.other.add_us(delay);
        }
    }
}

fn record_frame(text: &str, received: i64, latency: &mut Delays, books: &mut u64) -> bool {
    // A null client trusts generated update bodies; correctness tests parse the full frame.
    if text.starts_with(r#"{"event_type":"price_change","#)
        && text.ends_with('}')
        && let Some((_, tail)) = text.rsplit_once(r#""timestamp":""#)
        && let Some((timestamp, _)) = tail.split_once('"')
        && !timestamp.is_empty()
        && timestamp.bytes().all(|b| b.is_ascii_digit())
    {
        record_event(
            Event {
                event_type: Some("price_change"),
                timestamp: Some(timestamp),
            },
            received,
            latency,
            books,
        );
        return true;
    }
    if text.trim_start().starts_with('[') {
        if let Ok(events) = serde_json::from_str::<Vec<Event<'_>>>(text) {
            for event in events {
                record_event(event, received, latency, books);
            }
            return true;
        }
    } else if let Ok(event) = serde_json::from_str::<Event<'_>>(text) {
        record_event(event, received, latency, books);
        return true;
    }
    // Retain the old treatment of valid non-event JSON and escaped scalar fields.
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let events = value
        .as_array()
        .map_or_else(|| vec![&value], |array| array.iter().collect());
    for event in events {
        record_event(
            Event {
                event_type: event["event_type"].as_str(),
                timestamp: event["timestamp"].as_str(),
            },
            received,
            latency,
            books,
        );
    }
    true
}

fn runqueue_seconds() -> Option<f64> {
    let tasks = std::fs::read_dir("/proc/self/task").ok()?;
    let mut total = 0u64;
    for task in tasks {
        let path = task.ok()?.path().join("schedstat");
        let text = std::fs::read_to_string(path).ok()?;
        total += text.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    }
    Some(total as f64 / 1e9)
}

async fn run(args: Args) -> anyhow::Result<()> {
    let queue_start = runqueue_seconds();
    let http = reqwest::Client::new();
    let mut cursor: Option<String> = None;
    let mut assets = Vec::new();
    loop {
        let mut url = reqwest::Url::parse(&format!(
            "{}/markets/keyset",
            args.gamma_url.trim_end_matches('/')
        ))?;
        url.query_pairs_mut()
            .append_pair("closed", "false")
            .append_pair("limit", "100");
        if let Some(cursor) = &cursor {
            url.query_pairs_mut().append_pair("after_cursor", cursor);
        }
        let page: Value = http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        for market in page["markets"].as_array().context("missing markets")? {
            let tokens: Vec<String> = serde_json::from_str(
                market["clobTokenIds"]
                    .as_str()
                    .context("missing token IDs")?,
            )?;
            assets.extend(tokens);
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    ensure!(!assets.is_empty(), "empty catalog");
    // Put the most active (lowest synthetic ID) assets first for small subscriptions.
    assets.sort_by_key(|id| id.parse::<u64>().unwrap_or(u64::MAX));
    let barrier = Arc::new(Barrier::new(args.connections + 1));
    let frames = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    // Complete handshakes before the timed interval, and fail rather than leaving a barrier stranded.
    let mut sockets = Vec::new();
    for _ in 0..args.connections {
        sockets.push(
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio_tungstenite::connect_async_with_config(
                    &args.ws_url,
                    Some(
                        tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                            .read_buffer_size(16 * 1024),
                    ),
                    true,
                ),
            )
            .await??
            .0,
        );
    }
    for (index, mut socket) in sockets.into_iter().enumerate() {
        let requested: Vec<_> = (0..args.assets_per_connection)
            .map(|offset| {
                assets[(index * args.assets_per_connection + offset) % assets.len()].clone()
            })
            .collect();
        let barrier = barrier.clone();
        let frames = frames.clone();
        let bytes = bytes.clone();
        let seconds = args.seconds;
        tasks.spawn(async move {
            let mut latency=Delays::default();let mut errors=0;let mut closes=0;let mut initial_books=0;
            barrier.wait().await;
            socket.send(Message::Text(json!({"type":"market","assets_ids":requested,"initial_dump":true}).to_string().into())).await?;
            let timeout=tokio::time::sleep(Duration::from_secs(seconds));tokio::pin!(timeout);
            let mut batch=0;let mut last_yield=Instant::now();
            let mut ping=tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _=&mut timeout=>break,
                    _=ping.tick()=>{socket.send(Message::Text("PING".into())).await?;}
                    message=socket.next()=>match message {
                        Some(Ok(Message::Text(text)))=>{
                            let received=now_ms();frames.fetch_add(1,Ordering::Relaxed);bytes.fetch_add(text.len() as u64,Ordering::Relaxed);
                            if text=="PONG" {continue;}
                            if !record_frame(&text,received,&mut latency,&mut initial_books) {errors+=1;}
                            batch+=1;
                            if batch>=64 || last_yield.elapsed()>=Duration::from_micros(500) {
                                tokio::task::yield_now().await;batch=0;last_yield=Instant::now();
                            }
                        }
                        Some(Ok(Message::Close(_)))|None=>{closes+=1;break;}
                        Some(Err(_))=>{errors+=1;break;}
                        _=>{}
                    }
                }
            }
            let _=tokio::time::timeout(Duration::from_secs(1),socket.close(None)).await;
            Ok::<_,anyhow::Error>((latency,errors,closes,initial_books))
        });
    }
    let start = Instant::now();
    barrier.wait().await;
    let mut latency = Delays::default();
    let mut errors = 0;
    let mut closes = 0;
    let mut books = 0;
    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok((samples, e, c, b)) => {
                latency.merge(samples);
                errors += e;
                closes += c;
                books += b;
            }
            Err(_) => errors += 1,
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let runqueue = runqueue_seconds()
        .zip(queue_start)
        .map(|(end, start)| (end - start).max(0.0));
    println!(
        "{}",
        json!({"connections":args.connections,"assets_per_connection":args.assets_per_connection,"seconds":elapsed,"frames":frames.load(Ordering::Relaxed),"frames_per_second":frames.load(Ordering::Relaxed) as f64/elapsed,"bytes_per_second":bytes.load(Ordering::Relaxed) as f64/elapsed,"book_frames":books,"errors":errors,"early_closes":closes,"latency_ms":latency.all.summary(),"book_latency_ms":latency.books.summary(),"non_book_latency_ms":latency.other.summary(),"runqueue_seconds":runqueue})
    );
    ensure!(
        errors == 0 && closes == 0 && books > 0,
        "load run had errors, early closes, or no books"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Delays, record_frame};

    #[test]
    fn metadata_parser_preserves_counts_and_valid_json_fallback() {
        let mut latency = Delays::default();
        let mut books = 0;
        for frame in [
            r#"{"event_type":"price_change","timestamp":"100","price_changes":[{"size":"5"}]}"#,
            r#"[{"event_type":"book","timestamp":"101","bids":[{"price":"0.5","size":"1"}]}]"#,
            r#"{"event_type":"b\u006fok","timestamp":"102"}"#,
            r#"[null,12,{"event_type":"book","timestamp":12}]"#,
            "null",
        ] {
            assert!(record_frame(frame, 103, &mut latency, &mut books));
        }
        assert_eq!(books, 3);
        assert_eq!(latency.all.summary()["count"], 3);
        assert_eq!(latency.all.summary()["max"], 3.0);
        assert_eq!(latency.all.summary()["min"], 1.0);
        assert!(!record_frame("{broken", 103, &mut latency, &mut books));
    }
}
