// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use super::{GlobalReplay, ReplayWorkerScheduler, WorkerScheduler};
use crate::error::Result;
use crate::read::TraceReader;
use crate::Operation;

pub struct HummockReplay<R: TraceReader, G: GlobalReplay> {
    reader: R,
    replay: Arc<G>,
}

impl<R: TraceReader, G: GlobalReplay + 'static> HummockReplay<R, G> {
    pub fn new(reader: R, replay: G) -> Self {
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
            match r.operation() {
                // check results
                Operation::Result(_) => {
                    worker_scheduler.send_result(r);
                }
                Operation::Finish => {
                    worker_scheduler.wait_finish(r.clone()).await;
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

        println!("Replay finished, totally {} operations", total_ops);
        worker_scheduler.shutdown().await;
        println!("Total time {} seconds", time.elapsed().as_secs());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bytes::Bytes;
    use itertools::Itertools;
    use mockall::predicate;

    use super::*;
    use crate::replay::{MockGlobalReplayInterface, MockLocalReplayInterface};
    use crate::{
        MockTraceReader, OperationResult, Record, StorageType, TraceError, TraceResult,
        TracedBytes, TracedNewLocalOptions, TracedReadOptions,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replay() {
        let mut mock_reader = MockTraceReader::new();
        let get_result = TracedBytes::from(vec![54, 32, 198, 236, 24]);
        let seal_checkpoint = true;
        let sync_id = 4561245432;
        let seal_id = 5734875243;

        let opts1 = TracedNewLocalOptions::for_test(1);
        let opts2 = TracedNewLocalOptions::for_test(2);
        let opts3 = TracedNewLocalOptions::for_test(3);

        let storage_type1 = StorageType::Local(0, 1);
        let storage_type2 = StorageType::Local(1, 2);
        let storage_type3 = StorageType::Local(2, 3);
        let storage_type4 = StorageType::Global;

        let actor_1 = vec![
            (0, Operation::NewLocalStorage(opts1, 1)),
            (
                1,
                Operation::get(
                    Bytes::from(vec![0, 1, 2, 3]),
                    Some(123),
                    TracedReadOptions::for_test(opts1.table_id.table_id),
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
                Operation::insert(Bytes::from(vec![123]), Bytes::from(vec![123]), None),
            ),
            (
                2,
                Operation::Result(OperationResult::Insert(TraceResult::Ok(()))),
            ),
            (2, Operation::Finish),
            (3, Operation::DropLocalStorage),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type1, record_id, op)));

        let actor_2 = vec![
            (4, Operation::NewLocalStorage(opts2, 2)),
            (
                5,
                Operation::get(
                    TracedBytes::from(vec![0, 1, 2, 3]).into(),
                    Some(123),
                    TracedReadOptions::for_test(opts2.table_id.table_id),
                ),
            ),
            (
                5,
                Operation::Result(OperationResult::Get(TraceResult::Ok(Some(
                    get_result.clone(),
                )))),
            ),
            (5, Operation::Finish),
            (
                6,
                Operation::insert(Bytes::from(vec![123]), Bytes::from(vec![123]), None),
            ),
            (
                6,
                Operation::Result(OperationResult::Insert(TraceResult::Ok(()))),
            ),
            (6, Operation::Finish),
            (7, Operation::DropLocalStorage),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type2, record_id, op)));

        let actor_3 = vec![
            (8, Operation::NewLocalStorage(opts3, 3)),
            (
                9,
                Operation::get(
                    TracedBytes::from(vec![0, 1, 2, 3]).into(),
                    Some(123),
                    TracedReadOptions::for_test(opts3.table_id.table_id),
                ),
            ),
            (
                9,
                Operation::Result(OperationResult::Get(TraceResult::Ok(Some(
                    get_result.clone(),
                )))),
            ),
            (9, Operation::Finish),
            (
                10,
                Operation::insert(Bytes::from(vec![123]), Bytes::from(vec![123]), None),
            ),
            (
                10,
                Operation::Result(OperationResult::Insert(TraceResult::Ok(()))),
            ),
            (10, Operation::Finish),
            (11, Operation::DropLocalStorage),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type3, record_id, op)));

        let mut non_local: Vec<Result<Record>> = vec![
            (12, Operation::Seal(seal_id, seal_checkpoint)),
            (12, Operation::Finish),
            (13, Operation::Sync(sync_id)),
            (
                13,
                Operation::Result(OperationResult::Sync(TraceResult::Ok(0))),
            ),
            (13, Operation::Finish),
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

        let mut mock_replay = MockGlobalReplayInterface::new();

        mock_replay.expect_new_local().times(3).returning(move |_| {
            let mut mock_local = MockLocalReplayInterface::new();

            mock_local
                .expect_get()
                .times(1)
                .returning(move |_, _| Ok(Some(TracedBytes::from(vec![54, 32, 198, 236, 24]))));

            mock_local
                .expect_insert()
                .times(1)
                .returning(move |_, _, _| Ok(()));

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

        let mut replay = HummockReplay::new(mock_reader, mock_replay);

        replay.run().await.unwrap();
    }
}
