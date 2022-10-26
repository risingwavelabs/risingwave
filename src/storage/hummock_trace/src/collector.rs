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
use std::io::BufWriter;

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::write::{BincodeSerializer, TraceWriter, TraceWriterImpl};
use crate::{Operation, Record, RecordId, RecordIdGenerator, RecordMsg, WriteMsg};

// create a global singleton of collector as well as record id generator
// https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
lazy_static! {
    static ref GLOBAL_COLLECTOR: Collector = Collector::new();
    static ref GLOBAL_RECORD_ID: RecordIdGenerator = RecordIdGenerator::new();
}

pub fn init_collector() {
    let f = File::create("hummock.trace").unwrap();
    let writer = BufWriter::new(f);
    let writer = TraceWriterImpl::new_bincode(writer).unwrap();
    Collector::run(Box::new(writer));
}

struct Collector {
    tx: Sender<RecordMsg>,
    rx: Receiver<RecordMsg>,
    records_capacity: usize,
}

impl Collector {
    fn new() -> Self {
        let (tx, rx) = unbounded();
        Self {
            tx,
            rx,
            records_capacity: 1,
        }
    }

    fn run(writer: Box<dyn TraceWriter + Send>) {
        let (writer_tx, writer_rx) = unbounded();

        tokio::spawn(async move {
            Collector::handle_write(writer_rx, writer);
        });

        tokio::spawn(async move {
            GLOBAL_COLLECTOR.handle_record(writer_tx);
        });
    }

    fn handle_record(&self, writer_tx: Sender<WriteMsg>) {
        let mut records = Vec::with_capacity(self.records_capacity);
        loop {
            if let Ok(message) = self.rx.recv() {
                match message {
                    RecordMsg::Record(record) => {
                        records.push(record);
                        if records.len() == self.records_capacity {
                            writer_tx.send(WriteMsg::Write(records)).unwrap();
                            records = Vec::with_capacity(self.records_capacity);
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

    fn handle_write(rx: Receiver<WriteMsg>, mut writer: Box<dyn TraceWriter>) {
        loop {
            if let Ok(msg) = rx.recv() {
                match msg {
                    WriteMsg::Write(records) => {
                        let _ = writer.write_all(records).expect("failed to write log file");
                    }
                    WriteMsg::Fin() => return,
                }
            }
        }
    }

    fn tx(&self) -> Sender<RecordMsg> {
        self.tx.clone()
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.tx
            .send(RecordMsg::Fin())
            .expect("failed to finish collector");
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
    let span = TraceSpan {
        tx: GLOBAL_COLLECTOR.tx(),
        id: GLOBAL_RECORD_ID.next(),
    };
    span.send(op);
    span
}
