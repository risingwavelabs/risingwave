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
use std::ops::Bound;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};
#[cfg(test)]
use mockall::automock;
use tokio::task::JoinHandle;

use crate::error::{Result, TraceError};
use crate::read::TraceReader;
use crate::{Operation, Record};

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait Replayable: Send + Sync {
    async fn get(
        &self,
        key: Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Option<Vec<u8>>;
    async fn ingest(
        &self,
        kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize>;
    async fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        left_bound: Bound<Vec<u8>>,
        right_bound: Bound<Vec<u8>>,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Box<dyn ReplayIter>;
    async fn sync(&self, id: u64);
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn update_version(&self, version_id: u64);
}

#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

pub struct HummockReplay<R: TraceReader> {
    reader: R,
    tx: Sender<ReplayMessage>,
}

impl<T: TraceReader> HummockReplay<T> {
    pub fn new(reader: T, replay: Box<dyn Replayable>) -> (Self, JoinHandle<()>) {
        let (tx, rx) = unbounded::<ReplayMessage>();

        let handle = tokio::spawn(start_replay_worker(rx, Arc::new(replay)));
        (Self { reader, tx }, handle)
    }

    /// run groups records
    pub fn run(&mut self) -> Result<()> {
        let mut ops_open = HashMap::new();
        // put finished operations
        let mut ops_done = Vec::new();

        while let Ok(record) = self.reader.read() {
            // an operation finished
            if let Operation::Finish = record.op() {
                // find a finished op, remove it from ops and push it to ops_done
                if let Some(r) = ops_open.remove(&record.record_id()) {
                    ops_done.push(r);
                } else {
                    return Err(TraceError::FinRecord(record.record_id()));
                }
            } else {
                ops_open.insert(record.record_id(), record);
            }

            // all operations have been finished
            if ops_open.is_empty() && !ops_done.is_empty() {
                let records_group = self.aggregate_records(ops_done);
                // in each group, records are replayed sequentially
                self.tx
                    .send(ReplayMessage::Group(records_group))
                    .expect("failed to send records to replay");
                ops_done = Vec::new();
            }
        }

        // after reading all records, we still find unclosed operations
        // it could happen because of ungraceful shutdown
        if !ops_open.is_empty() {
            println!(
                "{} operations not finished, it may be caused by ungraceful shutdown",
                ops_open.len()
            );
        }

        self.tx
            .send(ReplayMessage::Fin)
            .expect("failed to finish writer");
        Ok(())
    }

    /// aggregate records by its `TraceLocalId` which are `actor_id`, executor `task_id`, or None
    /// return Record groups. In each group, records will be replayed sequentially
    pub fn aggregate_records(&mut self, records: Vec<Record>) -> Vec<ReplayGroup> {
        let mut records_map = HashMap::new();

        for r in records {
            let local_id = r.local_id();
            let entry = records_map.entry(local_id).or_insert(vec![]);
            entry.push(r);
        }

        // after the loop, we have to deal with records whose local_id is None
        records_map.into_values().collect()
    }
}

/// worker that actually replays hummock
async fn start_replay_worker(rx: Receiver<ReplayMessage>, replay: Arc<Box<dyn Replayable>>) {
    loop {
        if let Ok(msg) = rx.recv() {
            match msg {
                ReplayMessage::Group(groups) => {
                    let mut handles = Vec::with_capacity(groups.len());
                    for group in groups {
                        let replay = replay.clone();
                        let h = tokio::spawn(handle_replay_records(group, replay));
                        handles.push(h);
                    }

                    // await all concurrent workers
                    for handle in handles {
                        handle.await.unwrap();
                    }
                }
                ReplayMessage::Fin => return,
            };
        }
    }
}

async fn handle_replay_records(records: Vec<Record>, replay: Arc<Box<dyn Replayable>>) {
    let mut iters = HashMap::new();
    for r in records {
        let Record(_, record_id, op) = r;
        match op {
            Operation::Get(key, check_bloom_filter, epoch, table_id, retention_seconds) => {
                replay
                    .get(key, check_bloom_filter, epoch, table_id, retention_seconds)
                    .await;
            }
            Operation::Ingest(kv_pairs, epoch, table_id) => {
                let _ = replay.ingest(kv_pairs, epoch, table_id).await.unwrap();
            }
            Operation::Iter(
                prefix_hint,
                left_bound,
                right_bound,
                epoch,
                table_id,
                retention_seconds,
            ) => {
                let iter = replay
                    .iter(
                        prefix_hint,
                        left_bound,
                        right_bound,
                        epoch,
                        table_id,
                        retention_seconds,
                    )
                    .await;
                iters.insert(record_id, iter);
            }
            Operation::Sync(epoch_id) => {
                replay.sync(epoch_id).await;
            }
            Operation::Seal(epoch_id, is_checkpoint) => {
                replay.seal_epoch(epoch_id, is_checkpoint).await;
            }
            Operation::IterNext(id, expected) => {
                if let Some(iter) = iters.get_mut(&id) {
                    let actual = iter.next().await;
                    assert_eq!(expected, actual);
                }
            }
            Operation::UpdateVersion() => todo!(),
            Operation::Finish => unreachable!(),
        }
    }
}

type ReplayGroup = Vec<Record>;

enum ReplayMessage {
    Group(Vec<ReplayGroup>),
    Fin,
}

#[cfg(test)]
mod tests {
    use mockall::predicate;

    use super::*;
    use crate::MockTraceReader;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_replay() {
        let mut mock_reader = MockTraceReader::new();

        let mut i = 0;

        let f = move || {
            let r = match i {
                0 => Ok(Record::new_local_none(
                    0,
                    Operation::Get(vec![0], true, 0, 0, None),
                )),
                1 => Ok(Record::new_local_none(
                    1,
                    Operation::Get(vec![1], true, 0, 0, None),
                )),
                2 => Ok(Record::new_local_none(
                    2,
                    Operation::Get(vec![0], true, 0, 0, None),
                )),
                3 => Ok(Record::new_local_none(2, Operation::Finish)),
                4 => Ok(Record::new_local_none(1, Operation::Finish)),
                5 => Ok(Record::new_local_none(0, Operation::Finish)),
                6 => Ok(Record::new_local_none(
                    3,
                    Operation::Ingest(vec![(vec![1], Some(vec![1]))], 0, 0),
                )),
                7 => Ok(Record::new_local_none(4, Operation::Sync(123))),
                8 => Ok(Record::new_local_none(5, Operation::Seal(321, true))),
                9 => Ok(Record::new_local_none(3, Operation::Finish)),
                10 => Ok(Record::new_local_none(4, Operation::Finish)),
                11 => Ok(Record::new_local_none(5, Operation::Finish)),
                _ => Err(TraceError::FinRecord(5)), // intentional error
            };
            i += 1;
            r
        };

        mock_reader.expect_read().times(13).returning(f);

        let mut mock_replay = MockReplayable::new();

        mock_replay.expect_get().times(3).return_const(vec![1]);
        mock_replay
            .expect_ingest()
            .times(1)
            .returning(|_, _, _| Ok(0));
        mock_replay
            .expect_sync()
            .with(predicate::eq(123))
            .times(1)
            .return_const(());
        mock_replay
            .expect_seal_epoch()
            .with(predicate::eq(321), predicate::eq(true))
            .times(1)
            .return_const(());

        let (mut replay, join) = HummockReplay::new(mock_reader, Box::new(mock_replay));
        replay.run().unwrap();

        join.await.unwrap();
    }
}
