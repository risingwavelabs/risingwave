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

use std::fs::File;

use crossbeam::channel::{unbounded, Receiver, Sender};
use tokio::task::JoinHandle;

use super::record::{Operation, Record, RecordId, RecordIdGenerator};
use super::write::{TraceWriter, TraceWriterImpl};

// HummockTrace traces operations from Hummock
pub struct HummockTrace {
    records_tx: Sender<RecordMsg>,
    record_id_gen: RecordIdGenerator,
}

impl HummockTrace {
    pub fn new() -> Self {
        let file = File::create("hummock.trace").unwrap();
        let writer = TraceWriterImpl::new_bincode(file).unwrap();
        Self::new_with_writer(Box::new(writer)).0
    }

    #[cfg(test)]
    pub(crate) fn new_with_handler() -> (Self, JoinHandle<()>) {
        let file = File::create("hummock.trace").unwrap();
        let writer = TraceWriterImpl::new_bincode(file).unwrap();
        Self::new_with_writer(Box::new(writer))
    }

    pub(crate) fn new_with_writer(writer: Box<dyn TraceWriter + Send>) -> (Self, JoinHandle<()>) {
        let (records_tx, records_rx) = unbounded::<RecordMsg>();
        let (writer_tx, writer_rx) = unbounded::<WriteMsg>();

        // start a worker to receive hummock records
        tokio::spawn(Self::start_collector_worker(records_rx, writer_tx, 100));

        // start a worker to receive write requests
        let handle = tokio::spawn(Self::start_writer_worker(writer, writer_rx));

        let record_id_gen = RecordIdGenerator::new();

        (
            Self {
                records_tx,
                record_id_gen,
            },
            handle,
        )
    }

    pub fn new_trace_span(&self, op: Operation) -> TraceSpan {
        let id = self.record_id_gen.next();

        // let actor_id = task_local_get();
        let span = TraceSpan::new(self.records_tx.clone(), id);
        span.send(op);
        span
    }

    async fn start_collector_worker(
        records_rx: Receiver<RecordMsg>,
        writer_tx: Sender<WriteMsg>,
        records_capacity: usize,
    ) {
        let mut records = Vec::with_capacity(records_capacity); // sorted records?

        loop {
            if let Ok(message) = records_rx.recv() {
                match message {
                    RecordMsg::Record(record) => {
                        records.push(record);
                        if records.len() == records_capacity {
                            writer_tx.send(WriteMsg::Write(records)).unwrap();
                            records = Vec::with_capacity(records_capacity);
                        }
                    }
                    RecordMsg::Fin() => {
                        writer_tx.send(WriteMsg::Write(records)).unwrap();
                        writer_tx.send(WriteMsg::Fin()).unwrap();
                        return;
                    }
                }
            }
        }
    }

    async fn start_writer_worker(
        mut writer: Box<dyn TraceWriter + Send>,
        writer_rx: Receiver<WriteMsg>,
    ) {
        loop {
            if let Ok(request) = writer_rx.recv() {
                match request {
                    WriteMsg::Write(records) => {
                        writer.write_all(records).unwrap();
                    }
                    WriteMsg::Fin() => {
                        writer.sync().unwrap();
                        return;
                    }
                }
            }
        }
    }
}

impl Default for HummockTrace {
    fn default() -> Self {
        HummockTrace::new()
    }
}

impl Drop for HummockTrace {
    fn drop(&mut self) {
        // close the workers
        self.records_tx.send(RecordMsg::Fin()).unwrap();
    }
}

pub(crate) enum RecordMsg {
    Record(Record),
    Fin(),
}

pub(crate) enum WriteMsg {
    Write(Vec<Record>),
    Fin(),
}

#[derive(Clone)]
pub struct TraceSpan {
    tx: Sender<RecordMsg>,
    id: RecordId,
}

impl TraceSpan {
    pub(crate) fn new(tx: Sender<RecordMsg>, id: RecordId) -> Self {
        Self { tx, id }
    }

    pub(crate) fn send(&self, op: Operation) {
        self.tx
            .send(RecordMsg::Record(Record::new(self.id, op)))
            .unwrap();
    }

    pub(crate) fn finish(&self) {
        self.tx
            .send(RecordMsg::Record(Record::new(self.id, Operation::Finish)))
            .unwrap();
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use risingwave_common::monitor::task_local_scope;

    use super::{HummockTrace, Operation};
    use crate::error::Result;
    use crate::record::Record;
    use crate::write::TraceWriter;
    #[derive(Clone)]
    struct MockTraceWriter(Arc<Mutex<Vec<Record>>>);

    impl MockTraceWriter {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }
    }

    impl TraceWriter for MockTraceWriter {
        fn write(&mut self, record: Record) -> Result<usize> {
            self.0.lock().push(record);
            Ok(0)
        }

        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test()]
    async fn span_sequential() {
        let writer = MockTraceWriter::new();
        let writer_c = writer.clone();
        tokio::spawn(async move {
            let (tracer, join) = HummockTrace::new_with_writer(Box::new(writer_c));
            let tracer = Arc::new(tracer);
            {
                tracer.new_trace_span(Operation::Get(vec![0], true, 0, 0, None));
            }
            {
                tracer.new_trace_span(Operation::Sync(0));
            }
            drop(tracer);
            join.await.unwrap();
        })
        .await
        .unwrap();

        let log = writer.0.lock();

        assert_eq!(log.len(), 4);
        assert_eq!(
            log.get(0).unwrap(),
            &Record::new(0, Operation::Get(vec![0], true, 0, 0, None))
        );
        assert_eq!(log.get(1).unwrap(), &Record::new(0, Operation::Finish));
        assert_eq!(log.get(2).unwrap(), &Record::new(1, Operation::Sync(0)));
        assert_eq!(log.get(3).unwrap(), &Record::new(1, Operation::Finish));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn span_concurrent_in_memory() {
        let writer = MockTraceWriter::new();
        let count = 100;
        {
            let (tracer, join) = HummockTrace::new_with_writer(Box::new(writer.clone()));
            let tracer = Arc::new(tracer);

            let mut handles = Vec::new();

            for i in 0..count {
                let t = tracer.clone();
                handles.push(tokio::spawn(async move {
                    t.new_trace_span(Operation::Get(vec![i], true, 0, 0, None));
                    t.new_trace_span(Operation::Sync(i as u64));
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }
            drop(tracer);
            join.await.unwrap();
        }

        let log = writer.0.lock();
        assert_eq!(log.len(), (count as usize) * 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn span_concurrent_in_file() {
        let count: u8 = 100;
        let (tracer, join) = HummockTrace::new_with_handler();
        let tracer = Arc::new(tracer);

        let mut handles = Vec::new();
        for i in 0..count {
            let t = tracer.clone();
            #[cfg(not(madsim))]
            let f = task_local_scope(i as u64, async move {
                t.new_trace_span(Operation::Get(vec![i], true, 0, 0, None));
                t.new_trace_span(Operation::Sync(i as u64));
                let k = format!("key{}", i).as_bytes().to_vec();
                let v = format!("value{}", i).as_bytes().to_vec();
                t.new_trace_span(Operation::Ingest(vec![(k, Some(v))], 0, 0));
            });

            #[cfg(madsim)]
            let f = async move {
                t.new_trace_span(Operation::Get(vec![i], true));
                t.new_trace_span(Operation::Sync(i as u64));
                let k = format!("key{}", i).as_bytes().to_vec();
                let v = format!("value{}", i).as_bytes().to_vec();
                t.new_trace_span(Operation::Ingest(vec![(k, v)]));
            };

            handles.push(tokio::spawn(f));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        drop(tracer);
        join.await.unwrap();
    }
}
