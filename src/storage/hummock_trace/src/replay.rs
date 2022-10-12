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

use std::collections::HashMap;

use crossbeam::channel::{unbounded, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::error::{Result, TraceError};
use crate::read::TraceReader;
use crate::{Operation, Record};

pub trait Replayable {
    fn get(&self, key: Vec<u8>) -> Option<&Vec<u8>>;
    fn ingest(&mut self, kv_pairs: Vec<(Vec<u8>, Vec<u8>)>);
    fn iter(&self);
    fn sync(&mut self, id: u64);
    fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    fn update_version(&self, version_id: u64);
}

pub struct HummockReplay<R: TraceReader> {
    reader: R,
    tx: Sender<ReplayMessage>,
}

impl<R: TraceReader> HummockReplay<R> {
    pub fn new(reader: R) -> (Self, JoinHandle<()>) {
        let (tx, rx) = unbounded::<ReplayMessage>();

        let handle = tokio::spawn(start_replay_worker(rx));
        (Self { reader, tx }, handle)
    }

    pub fn run(&mut self) -> Result<()> {
        let mut ops = HashMap::new();
        let mut ops_send = Vec::new();

        while let Ok(record) = self.reader.read() {
            // an operation finished
            if let Operation::Finish = record.op() {
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
        self.tx.send(ReplayMessage::Fin).unwrap();
        Ok(())
    }
}

async fn start_replay_worker(rx: Receiver<ReplayMessage>) {
    loop {
        if let Ok(msg) = rx.recv() {
            match msg {
                ReplayMessage::Group(records) => {
                    for r in records {
                        match r.op() {
                            Operation::Get(_, _) => {}
                            Operation::Ingest(_) => todo!(),
                            Operation::Iter(_) => todo!(),
                            Operation::Sync(_) => todo!(),
                            Operation::Seal(_, _) => todo!(),
                            Operation::Finish => todo!(),
                            Operation::UpdateVersion() => todo!(),
                        }
                    }
                }
                ReplayMessage::Fin => break,
            };
        }
    }
}

enum ReplayMessage {
    Group(Vec<Record>),
    Fin,
}
