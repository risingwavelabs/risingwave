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
use std::sync::Arc;

use risingwave_common::hm_trace::TraceLocalId;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::{ReplayRequest, WorkerId, WorkerResponse};
use crate::error::Result;
use crate::read::TraceReader;
use crate::{replay_worker, Operation, OperationResult, RecordId, Replayable};

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
        let mut workers: HashMap<WorkerId, WorkerHandler> = HashMap::new();
        let mut total_ops: u64 = 0;
        while let Ok(r) = self.reader.read() {
            let local_id = r.local_id();
            let record_id = r.record_id();

            let worker_id = self.allocate_worker_id(&local_id, record_id);

            match r.op() {
                Operation::Result(trace_result) => {
                    if let Some(handler) = workers.get_mut(&worker_id) {
                        handler.send_result(trace_result.to_owned());
                    }
                }
                Operation::Finish => {
                    if let Some(handler) = workers.get_mut(&worker_id) {
                        handler.wait_resp().await;
                        if let TraceLocalId::None = local_id {
                            handler.finish();
                            workers.remove(&worker_id);
                        }
                    }
                }
                _ => {
                    let handler = workers.entry(worker_id.clone()).or_insert_with(|| {
                        let (req_tx, req_rx) = unbounded_channel();
                        let (resp_tx, resp_rx) = unbounded_channel();
                        let (res_tx, res_rx) = unbounded_channel();
                        let replay = self.replay.clone();
                        let join = tokio::spawn(replay_worker(req_rx, res_rx, resp_tx, replay));
                        WorkerHandler {
                            req_tx,
                            res_tx,
                            resp_rx,
                            join,
                        }
                    });

                    handler.send_replay_req(ReplayRequest::Task(vec![r]));
                    total_ops += 1;
                    if total_ops % 10000 == 0 {
                        println!("replayed {} ops", total_ops);
                    }
                }
            };
        }

        for handler in workers.into_values() {
            handler.finish();
            handler.join().await;
        }
        println!("replay finished, totally {} operations", total_ops);
        Ok(())
    }

    fn allocate_worker_id(&self, local_id: &TraceLocalId, record_id: RecordId) -> WorkerId {
        match local_id {
            TraceLocalId::Actor(id) => WorkerId::Actor(*id),
            TraceLocalId::Executor(id) => WorkerId::Executor(*id),
            TraceLocalId::None => WorkerId::None(record_id),
        }
    }
}

struct WorkerHandler {
    req_tx: UnboundedSender<ReplayRequest>,
    res_tx: UnboundedSender<OperationResult>,
    resp_rx: UnboundedReceiver<WorkerResponse>,
    join: JoinHandle<()>,
}

impl WorkerHandler {
    async fn join(self) {
        self.join.await.expect("failed to stop worker");
    }

    fn finish(&self) {
        self.send_replay_req(ReplayRequest::Fin);
    }

    fn send_replay_req(&self, req: ReplayRequest) {
        self.req_tx
            .send(req)
            .expect("failed to send replay request");
    }

    fn send_result(&self, result: OperationResult) {
        self.res_tx.send(result).expect("failed to send result");
    }

    async fn wait_resp(&mut self) {
        self.resp_rx
            .recv()
            .await
            .expect("failed to wait worker resp");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use itertools::Itertools;
    use mockall::predicate;

    use super::*;
    use crate::{
        MockLocalReplay, MockReplayable, MockTraceReader, Record, StorageType, TraceError,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replay() {
        let mut mock_reader = MockTraceReader::new();
        let get_result = vec![54, 32, 198, 236, 24];
        let ingest_result = 536248723;
        let seal_checkpoint = true;
        let sync_id = 4561245432;
        let seal_id = 5734875243;

        let storage_type = StorageType::Local;
        let local_id1 = TraceLocalId::Actor(1);
        let local_id2 = TraceLocalId::Actor(2);
        let local_id3 = TraceLocalId::Executor(1);
        let table_id1 = 1;
        let table_id2 = 2;
        let table_id3 = 3;
        let actor_1 = vec![
            (
                0,
                Operation::get(vec![0, 1, 2, 3], 123, None, true, Some(12), table_id1),
            ),
            (
                0,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            ),
            (0, Operation::Finish),
            (
                3,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id1),
            ),
            (
                3,
                Operation::Result(OperationResult::Ingest(Some(ingest_result))),
            ),
            (3, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type, local_id1, record_id, op)));

        let actor_2 = vec![
            (
                1,
                Operation::get(vec![0, 1, 2, 3], 123, None, true, Some(12), table_id2),
            ),
            (
                1,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            ),
            (1, Operation::Finish),
            (
                2,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id2),
            ),
            (
                2,
                Operation::Result(OperationResult::Ingest(Some(ingest_result))),
            ),
            (2, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type, local_id2, record_id, op)));

        let actor_3 = vec![
            (
                4,
                Operation::get(vec![0, 1, 2, 3], 123, None, true, Some(12), table_id3),
            ),
            (
                4,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            ),
            (4, Operation::Finish),
            (
                5,
                Operation::ingest(vec![(vec![123], Some(vec![123]))], vec![], 4, table_id3),
            ),
            (
                5,
                Operation::Result(OperationResult::Ingest(Some(ingest_result))),
            ),
            (5, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type, local_id3, record_id, op)));

        let mut non_local: Vec<Result<Record>> = vec![
            (6, Operation::Seal(seal_id, seal_checkpoint)),
            (6, Operation::Finish),
            (7, Operation::Sync(sync_id)),
            (7, Operation::Result(OperationResult::Sync(Some(0)))),
            (7, Operation::Finish),
        ]
        .into_iter()
        .map(|(record_id, op)| Ok(Record::new(storage_type, TraceLocalId::None, record_id, op)))
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
            let mut mock_local = MockLocalReplay::new();

            mock_local
                .expect_get()
                .times(1)
                .returning(move |_, _, _, _, _, _| Ok(Some(vec![54, 32, 198, 236, 24])));

            mock_local
                .expect_ingest()
                .times(1)
                .returning(move |_, _, _, _| Ok(ingest_result));

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
