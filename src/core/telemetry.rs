//! Structured JSON telemetry on stderr (SPEC section 14).
//!
//! Telemetry never blocks data: events go through a bounded queue to a
//! background thread that writes stderr. When the queue is full, events are
//! dropped. Completeness counters live in component totals, not here, so they
//! are never lost.

use std::io::Write;
use std::sync::LazyLock;

use crossbeam::channel::{Sender, bounded};
use serde_json::{Map, Value};

const QUEUE_LINES: usize = 16_384;

enum Item {
    Line(String),
    /// Answered once every earlier line is written.
    Flush(Sender<()>),
}

struct Telemetry {
    lines: Sender<Item>,
}

static TELEMETRY: LazyLock<Telemetry> = LazyLock::new(|| {
    let (lines, rx) = bounded::<Item>(QUEUE_LINES);
    std::thread::Builder::new()
        .name("telemetry".into())
        .spawn(move || {
            let mut stderr = std::io::stderr();
            for item in rx {
                match item {
                    // Nowhere left to report a failed stderr write.
                    Item::Line(line) => drop(stderr.write_all(line.as_bytes())),
                    Item::Flush(done) => {
                        let _ = stderr.flush();
                        let _ = done.send(());
                    }
                }
            }
        })
        .expect("spawn telemetry thread");
    Telemetry { lines }
});

/// Emits one JSON line: `{"ts": …, "event": <event>, …fields}`. `fields`
/// should be a JSON object; anything else is stored under `"value"`.
pub fn emit(event: &str, fields: Value) {
    let mut line = Map::new();
    line.insert("ts".into(), chrono::Utc::now().to_rfc3339().into());
    line.insert("event".into(), event.into());
    match fields {
        Value::Object(fields) => line.extend(fields),
        Value::Null => {}
        other => {
            line.insert("value".into(), other);
        }
    }
    let mut text = Value::Object(line).to_string();
    text.push('\n');
    // A full or closed queue drops the event rather than blocking data.
    let _ = TELEMETRY.lines.try_send(Item::Line(text));
}

/// Waits up to `limit` for queued events to reach stderr.
pub fn flush(limit: std::time::Duration) {
    let (done, wait) = bounded(1);
    if TELEMETRY
        .lines
        .send_timeout(Item::Flush(done), limit)
        .is_ok()
    {
        let _ = wait.recv_timeout(limit);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn telemetry_does_not_hold_stderr_while_idle() {
        crate::core::telemetry::emit("stderr_lock_check", serde_json::Value::Null);
        crate::core::telemetry::flush(Duration::from_secs(1));
        let (done, waited) = mpsc::channel();
        std::thread::spawn(move || {
            let stderr = std::io::stderr();
            let _guard = stderr.lock();
            let _ = done.send(());
        });
        assert!(
            waited.recv_timeout(Duration::from_secs(1)).is_ok(),
            "diagnostics must not prevent another thread from acquiring stderr"
        );
    }
}
