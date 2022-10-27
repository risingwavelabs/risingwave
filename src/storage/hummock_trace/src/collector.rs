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
use std::fs::{OpenOptions};
use std::io::BufWriter;

use crossbeam::channel::{unbounded, Receiver, Sender};

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

pub fn init_collector() {
    let path = match env::var(LOG_PATH) {
        Ok(p) => p,
        Err(_) => DEFAULT_PATH.to_string(),
    };

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .expect("failed to open log file");

    let writer = BufWriter::new(f);
    let writer = TraceWriterImpl::new_bincode(writer).unwrap();
    GlobalCollector::run(Box::new(writer));
}

struct GlobalCollector {
    tx: Sender<RecordMsg>,
    rx: Receiver<RecordMsg>,
}

impl GlobalCollector {
    fn new() -> Self {
        let (tx, rx) = unbounded();
        Self { tx, rx }
    }

    fn run(writer: Box<dyn TraceWriter + Send>) {
        let (writer_tx, writer_rx) = unbounded();

        tokio::spawn(async move {
            GlobalCollector::handle_write(writer_rx, writer);
        });

        tokio::spawn(async move {
            GLOBAL_COLLECTOR.handle_record(writer_tx);
        });
    }

    fn handle_record(&self, writer_tx: Sender<WriteMsg>) {
        loop {
            if let Ok(message) = self.rx.recv() {
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

    fn handle_write(rx: Receiver<WriteMsg>, mut writer: Box<dyn TraceWriter>) {
        let mut size = 0;
        loop {
            if let Ok(msg) = rx.recv() {
                match msg {
                    WriteMsg::Write(record) => {
                        size += writer.write(record).expect("failed to write the log file");
                        // default to use a BufWriter, must flush memory
                        if size > 1024 {
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
            .expect("failed to log record");
    }

    pub(crate) fn finish(&self) {
        self.tx
            .send(RecordMsg::Record(Record::new(self.id, Operation::Finish)))
            .expect("failed to finish a record");
    }

    pub fn id(&self) -> RecordId {
        self.id
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

pub fn new_span(op: Operation) -> TraceSpan {
    let span = TraceSpan::new(GLOBAL_COLLECTOR.tx(), GLOBAL_RECORD_ID.next());
    span.send(op);
    span
}

#[derive(Debug, PartialEq)]
pub(crate) enum RecordMsg {
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
    use super::*;
    use crate::MockTraceWriter;

    #[test]
    fn test_new_span() {
        let rx = GLOBAL_COLLECTOR.rx();

        let op1 = Operation::Sync(0);
        let op2 = Operation::Seal(0, false);

        let record1 = Record::new(0, op1.clone());
        let record2 = Record::new(1, op2.clone());

        let _span1 = new_span(op1);
        let _span2 = new_span(op2);

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
        assert_eq!(msg1, RecordMsg::Record(Record::new(0, Operation::Finish)));
        assert_eq!(msg2, RecordMsg::Record(Record::new(1, Operation::Finish)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn test_new_spans_concurrent() {
        let count = 200;
        let mut handles = Vec::with_capacity(count);
        for i in 0..count {
            let handle = tokio::spawn(async move {
                let op = Operation::Get(vec![i as u8], true, 1, 1, Some(1));
                let _span = new_span(op);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }
        let rx = GLOBAL_COLLECTOR.rx();
        assert_eq!(rx.len(), count * 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn test_collector_run() {
        let count = 5000;
        let op = Operation::Get(vec![103, 200, 234], true, 1, 1, Some(1));
        let mut mock_writer = MockTraceWriter::new();

        mock_writer
            .expect_write()
            .times(count * 2)
            .returning(|_| Ok(0));

        GlobalCollector::run(Box::new(mock_writer));

        let mut handles = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let op = op.clone();
            let handle = tokio::spawn(async move {
                let _span = new_span(op);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        GLOBAL_COLLECTOR.finish();
    }
}
