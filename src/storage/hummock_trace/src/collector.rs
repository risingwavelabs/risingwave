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

use flume::{unbounded, Receiver, Sender};
use risingwave_common::hm_trace::TraceLocalId;

use crate::write::{TraceWriter, TraceWriterImpl};
use crate::{Operation, Record, RecordId, RecordIdGenerator};

// create a global singleton of collector as well as record id generator
// https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
lazy_static! {
    static ref GLOBAL_COLLECTOR: GlobalCollector = GlobalCollector::new();
    static ref GLOBAL_RECORD_ID: RecordIdGenerator = RecordIdGenerator::new();
}

const LOG_PATH: &str = "HM_TRACE_PATH";
const DEFAULT_PATH: &str = ".trace/hummock.ht";
const WRITER_BUFFER_SIZE: usize = 1024;

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

    let writer = BufWriter::new(f);
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

    async fn start_writer_worker(rx: Receiver<WriteMsg>, mut writer: Box<dyn TraceWriter + Send>) {
        let mut size = 0;
        loop {
            if let Ok(msg) = rx.recv_async().await {
                match msg {
                    WriteMsg::Write(record) => {
                        size += writer.write(record).expect("failed to write the log file");
                        // default to use a BufWriter, must flush memory
                        if size > WRITER_BUFFER_SIZE {
                            writer.flush().expect("failed to sync file");
                            size = 0;
                        }
                    }
                    WriteMsg::Shutdown => {
                        return;
                    }
                }
            }
        }
    }

    async fn start_collect_worker(rx: Receiver<RecordMsg>, writer_tx: Sender<WriteMsg>) {
        loop {
            if let Ok(message) = rx.recv_async().await {
                match message {
                    RecordMsg::Record(record) => {
                        writer_tx
                            .send(WriteMsg::Write(record))
                            .expect("failed to send write req");
                    }
                    RecordMsg::Shutdown => {
                        writer_tx
                            .send(WriteMsg::Shutdown)
                            .expect("failed to kill writer thread");
                        return;
                    }
                }
            }
        }
    }

    fn finish(&self) {
        self.tx
            .send(RecordMsg::Shutdown)
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
    tx: Sender<RecordMsg>,
    id: RecordId,
}

impl TraceSpan {
    pub fn new(tx: Sender<RecordMsg>, id: RecordId) -> Self {
        Self { tx, id }
    }

    pub fn send(&self, op: Operation, local_id: TraceLocalId) {
        self.tx
            .send(RecordMsg::Record(Record::new(local_id, self.id, op)))
            .expect("failed to log record");
    }

    pub fn finish(&self) {
        self.tx
            .send(RecordMsg::Record(Record::new_local_none(
                self.id,
                Operation::Finish,
            )))
            .expect("failed to finish a record");
    }

    pub fn id(&self) -> RecordId {
        self.id
    }

    /// Create a span and send operation to the `GLOBAL_COLLECTOR`
    pub fn new_to_global(op: Operation, local_id: TraceLocalId) -> Self {
        let span = TraceSpan::new(GLOBAL_COLLECTOR.tx(), GLOBAL_RECORD_ID.next());
        span.send(op, local_id);
        span
    }

    #[cfg(test)]
    pub fn new_op(tx: Sender<RecordMsg>, id: RecordId, op: Operation) -> Self {
        let span = TraceSpan::new(tx, id);
        span.send(op, TraceLocalId::None);
        span
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Debug, PartialEq)]
pub enum RecordMsg {
    Record(Record),
    Shutdown,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WriteMsg {
    Write(Record),
    Shutdown,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::MockTraceWriter;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_new_span() {
        let rx = GLOBAL_COLLECTOR.rx();

        let op1 = Operation::Sync(0);
        let op2 = Operation::Seal(0, false);

        let record1 = Record::new_local_none(0, op1.clone());
        let record2 = Record::new_local_none(1, op2.clone());

        let _span1 = TraceSpan::new_to_global(op1, TraceLocalId::None);
        let _span2 = TraceSpan::new_to_global(op2, TraceLocalId::None);

        let msg1 = rx.recv().unwrap();
        let msg2 = rx.recv().unwrap();

        assert!(rx.is_empty());
        assert_eq!(msg1, RecordMsg::Record(record1));
        assert_eq!(msg2, RecordMsg::Record(record2));

        drop(_span1);
        drop(_span2);

        let msg1 = rx.recv().unwrap();
        let msg2 = rx.recv().unwrap();

        assert!(rx.is_empty());
        assert_eq!(
            msg1,
            RecordMsg::Record(Record::new_local_none(0, Operation::Finish))
        );
        assert_eq!(
            msg2,
            RecordMsg::Record(Record::new_local_none(1, Operation::Finish))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_spans_concurrent() {
        let count = 200;

        let collector = Arc::new(GlobalCollector::new());
        let generator = Arc::new(RecordIdGenerator::new());
        let mut handles = Vec::with_capacity(count);

        for i in 0..count {
            let collector = collector.clone();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let op = Operation::Get(vec![i as u8], true, 1, 1, Some(1));
                let _span = TraceSpan::new_op(collector.tx(), generator.next(), op);
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
        let generator = Arc::new(RecordIdGenerator::new());

        let op = Operation::Get(vec![103, 200, 234], true, 1, 1, Some(1));
        let mut mock_writer = MockTraceWriter::new();

        mock_writer
            .expect_write()
            .times(count * 2)
            .returning(|_| Ok(0));

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
                let _span = TraceSpan::new_op(collector.tx(), generator.next(), op);
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
