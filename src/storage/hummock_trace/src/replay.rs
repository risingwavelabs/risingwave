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

#[cfg(test)]
use mockall::automock;
use risingwave_common::hm_trace::TraceLocalId;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::read::TraceReader;
use crate::{Operation, OperationResult, Record, RecordId};

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
    ) -> Result<Option<Vec<u8>>>;
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
    ) -> Result<Box<dyn ReplayIter>>;
    async fn sync(&self, id: u64) -> Result<usize>;
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

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
        let mut worker_record_map = HashMap::new();
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
                    if let Some(id) = worker_record_map.remove(&record_id) {
                        if let Some(handler) = workers.get_mut(&id) {
                            handler.wait_resp().await;
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
                    worker_record_map.insert(record_id, worker_id);
                    total_ops += 1;
                    if total_ops % 10000 == 0 {
                        println!("replayed {} ops", total_ops);
                    }
                }
            };
        }

        for handler in workers.into_values() {
            handler.send_replay_req(ReplayRequest::Fin);
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

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
enum WorkerId {
    Actor(u64),
    Executor(u64),
    None(u64),
}

async fn replay_worker(
    mut rx: UnboundedReceiver<ReplayRequest>,
    mut res_rx: UnboundedReceiver<OperationResult>,
    tx: UnboundedSender<WorkerResponse>,
    replay: Arc<Box<dyn Replayable>>,
) {
    let mut iters_map = HashMap::new();
    loop {
        if let Some(msg) = rx.recv().await {
            match msg {
                ReplayRequest::Task(record_group) => {
                    for record in record_group {
                        handle_record(record, &replay, &mut res_rx, &mut iters_map).await;
                    }
                    tx.send(()).expect("failed to done task");
                }
                ReplayRequest::Fin => return,
            }
        }
    }
}

async fn handle_record(
    record: Record,
    replay: &Arc<Box<dyn Replayable>>,
    res_rx: &mut UnboundedReceiver<OperationResult>,
    iters_map: &mut HashMap<RecordId, Box<dyn ReplayIter>>,
) {
    let Record(_, record_id, op) = record;
    match op {
        Operation::Get(key, check_bloom_filter, epoch, table_id, retention_seconds) => {
            let actual = replay
                .get(key, check_bloom_filter, epoch, table_id, retention_seconds)
                .await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Get(expected) = res {
                assert_eq!(actual.ok(), expected, "get result wrong");
            }
        }
        Operation::Ingest(kv_pairs, epoch, table_id) => {
            let actual = replay.ingest(kv_pairs, epoch, table_id).await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Ingest(expected) = res {
                assert_eq!(actual.ok(), expected, "ingest result wrong");
            }
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
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Iter(expected) = res {
                if let Some(_) = expected {
                    iters_map.insert(record_id, iter.unwrap());
                } else {
                    assert!(iter.is_err());
                }
            }
        }
        Operation::Sync(epoch_id) => {
            let sync_result = replay.sync(epoch_id).await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Sync(expected) = res {
                let actual = sync_result.ok();
                assert_eq!(actual, expected, "sync failed");
            }
        }
        Operation::Seal(epoch_id, is_checkpoint) => {
            replay.seal_epoch(epoch_id, is_checkpoint).await;
        }
        Operation::IterNext(id) => {
            let iter = iters_map.get_mut(&id).expect("iter not in worker");
            let actual = iter.next().await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::IterNext(expected) = res {
                assert_eq!(actual, expected, "iter_next result wrong");
            }
        }
        Operation::MetaMessage(resp) => {
            let op = resp.0.operation();
            if let Some(info) = resp.0.info {
                replay.notify_hummock(info, op).await.unwrap();
            }
        }
        _ => unreachable!(),
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

type ReplayGroup = Vec<Record>;

type WorkerResponse = ();

#[derive(Debug)]
enum ReplayRequest {
    Task(ReplayGroup),
    Fin,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use mockall::predicate;

    use super::*;
    use crate::MockTraceReader;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replay() {
        let mut mock_reader = MockTraceReader::new();
        let get_result = vec![54, 32, 198, 236, 24];
        let ingest_result = 536248723;

        let sync_id = 4561245432;
        let seal_id = 5734875243;
        let seal_checkpoint = true;
        let mut records: VecDeque<Result<Record>> = VecDeque::from(vec![
            Ok(Record::new_local_none(
                0,
                Operation::Get(vec![0], true, 0, 0, None),
            )),
            Ok(Record::new_local_none(
                1,
                Operation::Get(vec![1], true, 0, 0, None),
            )),
            Ok(Record::new_local_none(
                2,
                Operation::Get(vec![0], true, 0, 0, None),
            )),
            Ok(Record::new_local_none(
                0,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            )),
            Ok(Record::new_local_none(
                0,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            )),
            Ok(Record::new_local_none(
                0,
                Operation::Result(OperationResult::Get(Some(Some(get_result.clone())))),
            )),
            Ok(Record::new_local_none(2, Operation::Finish)),
            Ok(Record::new_local_none(1, Operation::Finish)),
            Ok(Record::new_local_none(0, Operation::Finish)),
            Ok(Record::new_local_none(
                3,
                Operation::Ingest(vec![(vec![1], Some(vec![1]))], 0, 0),
            )),
            Ok(Record::new_local_none(
                0,
                Operation::Result(OperationResult::Ingest(Some(ingest_result))),
            )),
            Ok(Record::new_local_none(4, Operation::Sync(sync_id))),
            Ok(Record::new_local_none(
                5,
                Operation::Seal(seal_id, seal_checkpoint),
            )),
            Ok(Record::new_local_none(3, Operation::Finish)),
            Ok(Record::new_local_none(4, Operation::Finish)),
            Ok(Record::new_local_none(5, Operation::Finish)),
            Err(crate::TraceError::FinRecord(5)), // intentional error
        ]);

        let records_len = records.len();
        let f = move || records.pop_front().unwrap();

        mock_reader.expect_read().times(records_len).returning(f);

        let mut mock_replay = MockReplayable::new();

        mock_replay
            .expect_get()
            .times(3)
            .returning(move |_, _, _, _, _| Ok(Some(get_result.clone())));

        mock_replay
            .expect_ingest()
            .times(1)
            .returning(move |_, _, _| Ok(ingest_result));

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
