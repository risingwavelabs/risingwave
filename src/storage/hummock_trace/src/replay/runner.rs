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

use std::sync::Arc;
use std::time::Instant;

use crate::error::Result;
use crate::read::TraceReader;
use crate::{Operation, ReplayWorkerScheduler, Replayable, WorkerScheduler};

pub struct HummockReplay<R: TraceReader> {
    reader: R,
    replay: Arc<Box<dyn Replayable>>,
}

impl<R: TraceReader> HummockReplay<R> {
    pub fn new(reader: R, replay: Box<dyn Replayable>) -> Self {
        Self {
            reader,
            replay: Arc::new(replay),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.run_with_scheduler(WorkerScheduler::new(self.replay.clone()))
            .await
    }

    pub async fn run_with_scheduler<S: ReplayWorkerScheduler>(
        &mut self,
        mut worker_scheduler: S,
    ) -> Result<()> {
        let time = Instant::now();
        let mut total_ops: u64 = 0;

        while let Ok(r) = self.reader.read() {
            match r.op() {
                Operation::Result(_) => {
                    worker_scheduler.send_result(r);
                }
                Operation::Finish => {
                    worker_scheduler.wait_finish(r).await;
                }
                _ => {
                    worker_scheduler.schedule(r);
                    total_ops += 1;
                    if total_ops % 10000 == 0 {
                        println!("replayed {} ops", total_ops);
                    }
                }
            };
        }

        worker_scheduler.shutdown().await;

        println!("Replay finished, totally {} operations", total_ops);
        println!("Total time {} seconds", time.elapsed().as_secs());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use itertools::Itertools;
    use mockall::predicate;

    use super::*;
    use crate::{
        MockReplayable, MockTraceReader, OperationResult, Record, StorageType, TraceError,
        TraceResult,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replay() {
        let mut mock_reader = MockTraceReader::new();
        let get_result = vec![54, 32, 198, 236, 24];
        let ingest_result = 536248723;
        let seal_checkpoint = true;
        let sync_id = 4561245432;
        let seal_id = 5734875243;

        let storage_type1 = StorageType::Local(0);
        let storage_type2 = StorageType::Local(1);
        let storage_type3 = StorageType::Local(2);
        let storage_type4 = StorageType::Global;

        let table_id1 = 1;
        let table_id2 = 2;
        let table_id3 = 3;
        let actor_1 = vec![
            (
                0,
                Operation::get(
                    vec![0, 1, 2, 3],
                    123,
                    None,
                    true,
                    Some(12),
                    table_id1,
                    false,
                ),
            ),
            (
                0,
                Operation::Result(OperationResult::Get(TraceResult::Ok(Some(
                    get_result.clone(),
                )))),
            ),
            (0, Operation::Finish),
            (
                3,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id1),
            ),
            (
                3,
                Operation::Result(OperationResult::Ingest(TraceResult::Ok(ingest_result))),
            ),
            (3, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type1, record_id, op)));

        let actor_2 = vec![
            (
                1,
                Operation::get(
                    vec![0, 1, 2, 3],
                    123,
                    None,
                    true,
                    Some(12),
                    table_id2,
                    false,
                ),
            ),
            (
                1,
                Operation::Result(OperationResult::Get(TraceResult::Ok(Some(
                    get_result.clone(),
                )))),
            ),
            (1, Operation::Finish),
            (
                2,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id2),
            ),
            (
                2,
                Operation::Result(OperationResult::Ingest(TraceResult::Ok(ingest_result))),
            ),
            (2, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type2, record_id, op)));

        let actor_3 = vec![
            (
                4,
                Operation::get(
                    vec![0, 1, 2, 3],
                    123,
                    None,
                    true,
                    Some(12),
                    table_id3,
                    false,
                ),
            ),
            (
                4,
                Operation::Result(OperationResult::Get(TraceResult::Ok(Some(
                    get_result.clone(),
                )))),
            ),
            (4, Operation::Finish),
            (
                5,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id3),
            ),
            (
                5,
                Operation::Result(OperationResult::Ingest(TraceResult::Ok(ingest_result))),
            ),
            (5, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type3, record_id, op)));

        let mut non_local: Vec<Result<Record>> = vec![
            (6, Operation::Seal(seal_id, seal_checkpoint)),
            (6, Operation::Finish),
            (7, Operation::Sync(sync_id)),
            (
                7,
                Operation::Result(OperationResult::Sync(TraceResult::Ok(0))),
            ),
            (7, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type4, record_id, op)))
        .collect();

        // interleave vectors to simulate concurrency
        let mut actors = actor_1
            .into_iter()
            .interleave(actor_2.into_iter().interleave(actor_3.into_iter()))
            .collect::<Vec<_>>();

        actors.append(&mut non_local);

        actors.push(Err(TraceError::FinRecord(8))); // intentional error to stop loop

        let mut records: VecDeque<Result<Record>> = VecDeque::from(actors);

        let records_len = records.len();
        let f = move || records.pop_front().unwrap();

        mock_reader.expect_read().times(records_len).returning(f);

        let mut mock_replay = MockReplayable::new();

        mock_replay.expect_new_local().times(3).returning(move |_| {
            let mut mock_local = MockReplayable::new();

            mock_local
                .expect_get()
                .times(1)
                .returning(move |_, _, _| Ok(Some(vec![54, 32, 198, 236, 24])));

            mock_local
                .expect_ingest()
                .times(1)
                .returning(move |_, _, _| Ok(ingest_result));

            Box::new(mock_local)
        });

        mock_replay
            .expect_sync()
            .with(predicate::eq(sync_id))
            .times(1)
            .returning(|_| Ok(0));

        mock_replay
            .expect_seal_epoch()
            .with(predicate::eq(seal_id), predicate::eq(seal_checkpoint))
            .times(1)
            .return_const(());

        let mock_replay = Box::new(mock_replay);
        let mut replay = HummockReplay::new(mock_reader, mock_replay);

        replay.run().await.unwrap();
    }
}
