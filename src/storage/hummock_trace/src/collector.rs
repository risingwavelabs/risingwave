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

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::{Operation, Record, RecordId, RecordIdGenerator, RecordMsg, WriteMsg};

// create a global singleton of collector as well as record id generator
// https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
lazy_static! {
    static ref GLOBAL_COLLECTOR: Collector = Collector::new();
    static ref GLOBAL_RECORD_ID: RecordIdGenerator = RecordIdGenerator::new();
}

pub fn init_collector() {
    Collector::run();
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
            records_capacity: 100,
        }
    }

    fn run() {
        let (writer_tx, writer_rx) = unbounded();

        tokio::spawn(async move {
            Collector::handle_write(writer_rx);
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

    fn handle_write(rx: Receiver<WriteMsg>) {
        loop {
            if let Ok(msg) = rx.recv() {
                match msg {
                    WriteMsg::Write(op) => {
                        println!("{:?}", op);
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
            .expect("fail to log record");
    }

    pub(crate) fn finish(&self) {
        self.tx
            .send(RecordMsg::Record(Record::new(self.id, Operation::Finish)))
            .expect("fail to finish a record");
    }

    pub fn id(&self) -> RecordId {
        self.id
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
