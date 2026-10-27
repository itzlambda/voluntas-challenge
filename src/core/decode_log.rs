//! The debug log for frames that fail to decode (SPEC section 6). Failures
//! aren't expected, so each one is written straight to the file.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::model::ConnectionId;

pub struct DecodeErrorLog {
    file: Mutex<File>,
}

impl DecodeErrorLog {
    /// Opens `path` for appending, creating it if needed.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open decode error log {}", path.display()))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Appends one JSON line with the raw frame. Returns the write error, if
    /// any, so the caller can report it; data handling continues either way.
    pub fn record(
        &self,
        connection: ConnectionId,
        received_at: DateTime<Utc>,
        error: &str,
        frame: &str,
    ) -> std::io::Result<()> {
        let mut line = json!({
            "logged_at": Utc::now().to_rfc3339(),
            "connection": connection.0,
            "received_at": received_at.to_rfc3339(),
            "error": error,
            "frame": frame,
        })
        .to_string();
        line.push('\n');
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        file.write_all(line.as_bytes())
    }
}
