// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;

use super::ReadMode;

/// Nonblocking, bounded diagnostics. It never consumes or polls the response stream.
#[derive(Clone, Debug)]
pub struct ReadTrace {
    sender: SyncSender<Message>,
    ids: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

#[derive(Debug)]
enum Message {
    Record(Record),
    Finish,
}

#[derive(Debug, Serialize)]
pub(super) struct Record {
    pub request_id: u64,
    pub event: &'static str,
    pub started_unix_us: u128,
    pub elapsed_us: u128,
    pub offset: usize,
    pub bytes: usize,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestInfo>,
}

#[derive(Debug, Serialize)]
pub(super) struct RequestInfo {
    pub phase: String,
    pub key: String,
    pub reader_id: usize,
    pub mode: ReadMode,
    pub range_start: usize,
    pub range_length: usize,
}

#[derive(Debug, Serialize)]
pub struct TraceSummary {
    pub records_written: u64,
    pub records_dropped: u64,
}

/// Keep this guard until all benchmark readers finish, then call `finish` to check
/// write errors and dropped records. Drop also drains the writer on early errors.
pub struct TraceWriter {
    trace: ReadTrace,
    worker: Option<JoinHandle<Result<u64>>>,
}

impl TraceWriter {
    /// Refuse to overwrite existing files. Parent directories must already exist.
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .context("create benchmark read trace (destination must not exist)")?;
        let (sender, receiver) = mpsc::sync_channel(8192);
        let worker = std::thread::Builder::new()
            .name("bench-read-trace".into())
            .spawn(move || {
                let mut file = BufWriter::new(file);
                let mut count = 0;
                let mut flushed = Instant::now();
                loop {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(Message::Record(record)) => {
                            serde_json::to_writer(&mut file, &record)?;
                            file.write_all(b"\n")?;
                            count += 1;
                        }
                        Ok(Message::Finish) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if flushed.elapsed() >= Duration::from_secs(1) {
                        file.flush()?;
                        flushed = Instant::now();
                    }
                }
                file.flush()?;
                Ok(count)
            })
            .context("start benchmark trace writer")?;
        Ok(Self {
            trace: ReadTrace {
                sender,
                ids: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
            },
            worker: Some(worker),
        })
    }

    pub fn trace(&self) -> ReadTrace {
        self.trace.clone()
    }

    pub fn finish(mut self) -> Result<TraceSummary> {
        self.join()
    }

    fn join(&mut self) -> Result<TraceSummary> {
        // Readers have finished; blocking here cannot affect their timings.
        let _ = self.trace.sender.send(Message::Finish);
        let records_written = self
            .worker
            .take()
            .expect("writer joined once")
            .join()
            .map_err(|_| anyhow::anyhow!("benchmark trace writer panicked"))?
            .context("write benchmark read trace")?;
        Ok(TraceSummary {
            records_written,
            records_dropped: self.trace.dropped.load(Ordering::Relaxed),
        })
    }
}

impl Drop for TraceWriter {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.join();
        }
    }
}

impl ReadTrace {
    pub(super) fn request_id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn record(&self, record: Record) {
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            self.sender.try_send(Message::Record(record))
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(super) fn unix_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> Record {
        Record {
            request_id: 0,
            event: "next",
            started_unix_us: 0,
            elapsed_us: 1,
            offset: 0,
            bytes: 1,
            status: "data",
            request: None,
        }
    }

    #[test]
    fn bounded_writer_counts_loss_and_preserves_existing_files() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let trace = ReadTrace {
            sender,
            ids: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        };
        trace.record(record());
        trace.record(record());
        assert_eq!(trace.dropped.load(Ordering::Relaxed), 1);
        drop(receiver);
        trace.record(record());
        assert_eq!(trace.dropped.load(Ordering::Relaxed), 2);

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace.jsonl");
        {
            let writer = TraceWriter::create(&path).unwrap();
            writer.trace().record(record());
            assert!(TraceWriter::create(&path).is_err());
            // Early-return Drop still drains all queued records.
        }
        let rows = std::fs::read_to_string(&path).unwrap();
        assert_eq!(rows.lines().count(), 1);
        assert!(TraceWriter::create(&path).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), rows);
    }
}
