// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::AtomicU64;

use bincode::{Decode, Encode};
use either::Either;
use flume::{unbounded, Receiver, Sender};
use tokio::task_local;

use crate::write::{TraceWriter, TraceWriterImpl};
use crate::{
    ConcurrentIdGenerator, Operation, OperationResult, Record, RecordId, RecordIdGenerator,
    UniqueIdGenerator,
};

// create a global singleton of collector as well as record id generator
lazy_static! {
    static ref GLOBAL_COLLECTOR: GlobalCollector = GlobalCollector::new();
    static ref GLOBAL_RECORD_ID: RecordIdGenerator = UniqueIdGenerator::new(AtomicU64::new(0));
    static ref SHOULD_USE_TRACE: bool = set_use_trace();
    pub static ref CONCURRENT_ID: ConcurrentIdGenerator = UniqueIdGenerator::new(AtomicU64::new(0));
}

const USE_TRACE: &str = "USE_HM_TRACE";
const LOG_PATH: &str = "HM_TRACE_PATH";
const DEFAULT_PATH: &str = ".trace/hummock.ht";
const WRITER_BUFFER_SIZE: usize = 1024;

pub fn should_use_trace() -> bool {
    *SHOULD_USE_TRACE
}

fn set_use_trace() -> bool {
    if let Ok(v) = std::env::var(USE_TRACE) {
        v.parse().unwrap()
    } else {
        false
    }
}

/// Initialize the `GLOBAL_COLLECTOR` with configured log file
pub fn init_collector() {
    let path = match env::var(LOG_PATH) {
        Ok(p) => p,
        Err(_) => DEFAULT_PATH.to_string(),
    };
    let path = Path::new(&path);

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            create_dir_all(parent).unwrap();
        }
    }

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .expect("failed to open log file");

    let writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, f);
    let writer = TraceWriterImpl::new_bincode(writer).unwrap();
    tokio::spawn(GLOBAL_COLLECTOR.run(Box::new(writer)));
}

/// `GlobalCollector` collects traced hummock operations.
/// It starts a collector thread and writer thread.
struct GlobalCollector {
    tx: Sender<RecordMsg>,
    rx: Receiver<RecordMsg>,
}

impl GlobalCollector {
    fn new() -> Self {
        let (tx, rx) = unbounded();
        Self { tx, rx }
    }

    async fn run(&self, writer: Box<dyn TraceWriter + Send>) {
        let (writer_tx, writer_rx) = unbounded();

        let writer_handle = tokio::spawn(GlobalCollector::start_writer_worker(writer_rx, writer));

        let rx = self.rx.clone();

        let collect_handle = tokio::spawn(GlobalCollector::start_collect_worker(rx, writer_tx));

        collect_handle
            .await
            .expect("failed to stop collector thread");
        writer_handle.await.expect("failed to stop writer thread");
    }

    async fn start_collect_worker(rx: Receiver<RecordMsg>, writer_tx: Sender<WriteMsg>) {
        let mut records = Vec::new();
        loop {
            if let Ok(message) = rx.recv_async().await {
                match message {
                    RecordMsg::Left(record) => {
                        records.push(record);
                    }
                    RecordMsg::Right(()) => {
                        writer_tx
                            .send(WriteMsg::Left(records))
                            .expect("failed to send write req");
                        writer_tx
                            .send(WriteMsg::Right(()))
                            .expect("failed to kill writer thread");
                        return;
                    }
                }
            }
            if !records.is_empty() && !rx.is_empty() {
                writer_tx
                    .send(WriteMsg::Left(records))
                    .expect("failed to send write req");
                records = Vec::new();
            }
        }
    }

    async fn start_writer_worker(rx: Receiver<WriteMsg>, mut writer: Box<dyn TraceWriter + Send>) {
        loop {
            if let Ok(msg) = rx.recv_async().await {
                match msg {
                    WriteMsg::Left(records) => {
                        writer
                            .write_all(records)
                            .expect("failed to write the log file");
                    }
                    WriteMsg::Right(()) => {
                        writer.flush().expect("failed to flush content");
                        return;
                    }
                }
            }
        }
    }

    fn finish(&self) {
        self.tx
            .send(Either::Right(()))
            .expect("failed to finish collector");
    }

    fn tx(&self) -> Sender<RecordMsg> {
        self.tx.clone()
    }

    #[cfg(test)]
    fn rx(&self) -> Receiver<RecordMsg> {
        self.rx.clone()
    }
}

impl Drop for GlobalCollector {
    fn drop(&mut self) {
        self.finish();
    }
}

/// `TraceSpan` traces hummock operations. It marks the beginning of an operation and
/// the end when the span is dropped. So, please make sure the span live long enough.
/// Underscore binding like `let _ = span` will drop the span immediately.
#[must_use = "TraceSpan Lifetime is important"]
#[derive(Clone)]
pub struct TraceSpan {
    tx: Option<Sender<RecordMsg>>,
    id: Option<RecordId>,
    storage_type: Option<StorageType>,
}

impl TraceSpan {
    pub fn new(tx: Sender<RecordMsg>, id: RecordId, storage_type: StorageType) -> Self {
        Self {
            tx: Some(tx),
            id: Some(id),
            storage_type: Some(storage_type),
        }
    }

    pub fn send(&self, op: Operation) {
        match &self.tx {
            Some(tx) => {
                tx.send(Either::Left(Record::new(
                    self.storage_type(),
                    self.id(),
                    op,
                )))
                .expect("failed to log record");
            }
            None => {}
        }
    }

    pub fn send_result(&self, res: OperationResult) {
        self.send(Operation::Result(res));
    }

    pub fn finish(&self) {
        if self.tx.is_some() {
            self.send(Operation::Finish);
        }
    }

    pub fn id(&self) -> RecordId {
        self.id.unwrap()
    }

    fn storage_type(&self) -> StorageType {
        self.storage_type.unwrap()
    }

    /// Create a span and send operation to the `GLOBAL_COLLECTOR`
    pub fn new_to_global(op: Operation, storage_type: StorageType) -> Self {
        let span = TraceSpan::new(GLOBAL_COLLECTOR.tx(), GLOBAL_RECORD_ID.next(), storage_type);
        span.send(op);
        span
    }

    pub fn none() -> Self {
        TraceSpan {
            tx: None,
            id: None,
            storage_type: None,
        }
    }

    #[cfg(test)]
    pub fn new_op(
        tx: Sender<RecordMsg>,
        id: RecordId,
        op: Operation,
        storage_type: StorageType,
    ) -> Self {
        let span = TraceSpan::new(tx, id, storage_type);
        span.send(op);
        span
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

pub type RecordMsg = Either<Record, ()>;
pub type WriteMsg = Either<Vec<Record>, ()>;

pub type ConcurrentId = u64;

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum StorageType {
    Global,
    Local(ConcurrentId),
}

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{MockTraceWriter, TracedBytes};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_new_span() {
        let rx = GLOBAL_COLLECTOR.rx();

        let op1 = Operation::Sync(0);
        let op2 = Operation::Seal(0, false);

        let record1 = Record::new(StorageType::Global, 0, op1.clone());
        let record2 = Record::new(StorageType::Global, 1, op2.clone());

        let _span1 = TraceSpan::new_to_global(op1, StorageType::Global);
        let _span2 = TraceSpan::new_to_global(op2, StorageType::Global);

        let msg1 = rx.recv().unwrap();
        let msg2 = rx.recv().unwrap();

        assert!(rx.is_empty());
        assert_eq!(msg1, Either::Left(record1));
        assert_eq!(msg2, Either::Left(record2));

        drop(_span1);
        drop(_span2);

        let msg1 = rx.recv().unwrap();
        let msg2 = rx.recv().unwrap();

        assert!(rx.is_empty());
        assert_eq!(
            msg1,
            Either::Left(Record::new_local_none(0, Operation::Finish))
        );
        assert_eq!(
            msg2,
            Either::Left(Record::new_local_none(1, Operation::Finish))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_spans_concurrent() {
        let count = 200;

        let collector = Arc::new(GlobalCollector::new());
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));
        let mut handles = Vec::with_capacity(count);

        for i in 0..count {
            let collector = collector.clone();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let op = Operation::get(
                    TracedBytes::from(vec![i as u8]),
                    123,
                    None,
                    true,
                    Some(12),
                    123,
                    false,
                );
                let _span =
                    TraceSpan::new_op(collector.tx(), generator.next(), op, StorageType::Global);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let rx = collector.rx();
        assert_eq!(rx.len(), count * 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collector_run() {
        let count = 5000;
        let collector = Arc::new(GlobalCollector::new());
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));

        let op = Operation::get(
            TracedBytes::from(vec![74, 56, 43, 67]),
            256,
            None,
            true,
            Some(242),
            167,
            false,
        );
        let mut mock_writer = MockTraceWriter::new();

        mock_writer.expect_write_all().returning(|_| Ok(0));
        mock_writer.expect_flush().times(1).returning(|| Ok(()));
        let _collector = collector.clone();

        let runner_handle = tokio::spawn(async move {
            _collector.clone().run(Box::new(mock_writer)).await;
        });

        let mut handles = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let op = op.clone();
            let collector = collector.clone();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let _span =
                    TraceSpan::new_op(collector.tx(), generator.next(), op, StorageType::Local(0));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        collector.finish();

        runner_handle.await.unwrap();
    }
}
