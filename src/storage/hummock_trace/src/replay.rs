use std::collections::HashMap;

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::error::{Result, TraceError};
use crate::read::TraceReader;
use crate::{Operation, Record};

pub(crate) struct HummockReplay<R: TraceReader> {
    reader: R,
    tx: Sender<ReplayMessage>,
}

impl<R: TraceReader> HummockReplay<R> {
    pub(crate) fn new(reader: R) -> Self {
        let (tx, rx) = unbounded::<ReplayMessage>();
        tokio::spawn(start_replay_worker(rx));
        Self { reader, tx }
    }

    pub(crate) fn run(&mut self) -> Result<()> {
        let mut ops = HashMap::new();
        let mut ops_send = Vec::new();
        while let Ok(record) = self.reader.read() {
            // an operation finished
            if let Operation::Finish() = record.op() {
                if !ops.contains_key(&record.id()) {
                    return Err(TraceError::FinRecordError(record.id()));
                }
                ops_send.push(record);
            } else {
                ops.insert(record.id(), record);
            }

            // all operations have been finished
            if ops.is_empty() && !ops_send.is_empty() {
                self.tx.send(ReplayMessage::Group(ops_send)).unwrap();
                ops_send = Vec::new();
            }
        }
        Ok(())
    }
}

impl<R: TraceReader> Drop for HummockReplay<R> {
    fn drop(&mut self) {
        self.tx.send(ReplayMessage::FIN).unwrap();
    }
}

async fn start_replay_worker(rx: Receiver<ReplayMessage>) {
    loop {
        if let Ok(msg) = rx.recv() {
            match msg {
                ReplayMessage::Group(records) => for r in records {},
                ReplayMessage::FIN => break,
            };
        }
    }
}

enum ReplayMessage {
    Group(Vec<Record>),
    FIN,
}
