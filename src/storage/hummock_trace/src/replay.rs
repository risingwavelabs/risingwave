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
#[cfg(test)]
use mockall::automock;
use tokio::task::JoinHandle;

use crate::error::{Result, TraceError};
use crate::read::TraceReader;
use crate::{Operation, Record};

#[cfg_attr(test, automock)]
pub trait Replayable: Send + Sync {
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>>;
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

impl<T: TraceReader> HummockReplay<T> {
    pub fn new(reader: T, replay: Box<dyn Replayable>) -> (Self, JoinHandle<()>) {
        let (tx, rx) = unbounded::<ReplayMessage>();

        let handle = tokio::spawn(start_replay_worker(rx, replay));
        (Self { reader, tx }, handle)
    }

    pub fn run(&mut self) -> Result<()> {
        let mut ops = HashMap::new();
        let mut ops_send = Vec::new();

        while let Ok(record) = self.reader.read() {
            // an operation finished
            if let Operation::Finish = record.op() {
                if let Some(r) = ops.remove(&record.id()) {
                    ops_send.push(r);
                } else {
                    return Err(TraceError::FinRecordError(record.id()));
                }
            } else {
                ops.insert(record.id(), record);
            }

            // all operations have been finished
            if ops.is_empty() && !ops_send.is_empty() {
                self.tx.send(ReplayMessage::Group(ops_send)).unwrap();
                ops_send = Vec::new();
            }
        }

        assert!(ops.is_empty(), "operations not finished");
        self.tx
            .send(ReplayMessage::Fin)
            .expect("failed to finish writer");
        Ok(())
    }
}

async fn start_replay_worker(rx: Receiver<ReplayMessage>, mut replay: Box<dyn Replayable>) {
    loop {
        if let Ok(msg) = rx.recv() {
            match msg {
                ReplayMessage::Group(records) => {
                    for r in records {
                        match r.op() {
                            Operation::Get(key, _) => {
                                // tokio::spawn(||{});
                                replay.get(key);
                            }
                            Operation::Ingest(kv_pairs) => {
                                replay.ingest(kv_pairs.to_vec());
                            }
                            Operation::Iter(_) => todo!(),
                            Operation::Sync(_) => todo!(),
                            Operation::Seal(_, _) => todo!(),
                            Operation::Finish => todo!(),
                            Operation::UpdateVersion() => todo!(),
                        }
                    }
                }
                ReplayMessage::Fin => return,
            };
        }
    }
}

enum ReplayMessage {
    Group(Vec<Record>),
    Fin,
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::MockTraceReader;

    #[tokio::test]
    async fn test_replay() {
        let mut reader = MockTraceReader::new();

        let mut i = 0;

        let f = move || {
            let r = match i {
                0 => Ok(Record::new(0, Operation::Get(vec![0], true))),
                1 => Ok(Record::new(1, Operation::Get(vec![1], true))),
                2 => Ok(Record::new(2, Operation::Get(vec![0], true))),
                3 => Ok(Record::new(2, Operation::Finish)),
                4 => Ok(Record::new(1, Operation::Finish)),
                5 => Ok(Record::new(0, Operation::Finish)),
                _ => Err(TraceError::FinRecordError(5)), // intentional error
            };
            i += 1;
            r
        };

        reader.expect_read().times(7).returning(f);

        let mut mock_replay = MockReplayable::new();

        mock_replay.expect_get().times(3).return_const(vec![1]);

        let (mut replay, join) = HummockReplay::new(reader, Box::new(mock_replay));
        replay.run().unwrap();

        join.await.unwrap();
    }
}
