//! Capture mode: records raw frames and controls in the fixture format, so a
//! live session can be replayed in tests. A development tool: lines go
//! through an unbounded queue to a background thread.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc;
use std::thread::JoinHandle;

use anyhow::Context;

use super::fixture::{FixtureEvent, to_jsonl_line};

#[derive(Clone)]
pub struct Capture {
    lines: mpsc::Sender<String>,
}

pub struct CaptureFile {
    thread: JoinHandle<std::io::Result<()>>,
}

impl Capture {
    pub fn create(path: &Path) -> anyhow::Result<(Self, CaptureFile)> {
        let file =
            File::create(path).with_context(|| format!("create capture {}", path.display()))?;
        let (lines, rx) = mpsc::channel::<String>();
        let thread = std::thread::Builder::new()
            .name("capture".into())
            .spawn(move || {
                let mut out = BufWriter::new(file);
                for line in rx {
                    out.write_all(line.as_bytes())?;
                    out.write_all(b"\n")?;
                }
                out.flush()
            })?;
        Ok((Self { lines }, CaptureFile { thread }))
    }

    pub fn record(&self, event: &FixtureEvent) {
        // A send fails only after the capture thread stopped on a write error,
        // which `CaptureFile::finish` reports.
        let _ = self.lines.send(to_jsonl_line(event));
    }
}

impl CaptureFile {
    /// Waits for the file to be written once every `Capture` is dropped.
    pub fn finish(self) -> anyhow::Result<()> {
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("capture thread panicked"))?
            .context("write capture")
    }
}
