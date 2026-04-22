// Copyright 2026 RisingWave Labs
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

use anyhow::Context;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_rpc_client::CoordinatorStreamHandle;
use thiserror_ext::AsReport;

use super::CoordinatorStreamHandleInit;
use crate::executor::iceberg_with_pk_index::Payload;
use crate::executor::prelude::*;

/// Trait abstracting DV (Deletion Vector) file operations for testability.
///
/// Implementations are responsible for reading existing DVs, merging new
/// delete positions, writing DV files, and returning the metadata that should
/// be committed on the current barrier.
#[async_trait::async_trait]
pub trait DvHandler: Send + 'static {
    fn write(&mut self, path: &str, pos: i64) -> StreamExecutorResult<()>;

    fn add_partition_info(
        &mut self,
        path: &str,
        partition_info: Vec<u8>,
    ) -> StreamExecutorResult<()>;

    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>>;
}

/// DV Merger Executor for Iceberg V3 Sink without Equality Delete.
///
/// This stateless executor receives payload messages from the Writer Executor.
/// The upstream plan shards messages by `file_path`, so each actor only merges delete
/// positions for the files assigned to its shard.
///
/// On each barrier, the DV Merger:
/// 1. Groups accumulated delete positions by `file_path`
/// 2. For each file, reads the existing DV from object storage
/// 3. Merges the new positions with the existing DV
/// 4. Writes the merged DV file
/// 5. Reports the DV file metadata to meta
///
/// Input schema: [`file_path`: Varchar, payload: Bytea]
/// where `payload` is an encoded [`Payload`] emitted by the writer.
/// Output: Only barriers (terminal executor in the stream graph).
pub struct DvMergerExecutor<H>
where
    H: DvHandler,
{
    ctx: ActorContextRef,
    input: Option<Executor>,
    handler: H,
    /// Handle for sending commit metadata to the coordinator.
    coordinator_handle: Option<CoordinatorStreamHandle>,
    coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
}

impl<H> DvMergerExecutor<H>
where
    H: DvHandler,
{
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        handler: H,
        coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            handler,
            coordinator_handle: None,
            coordinator_handle_init,
        }
    }

    async fn initialize_coordinator_handle(&mut self) -> StreamExecutorResult<()> {
        let Some(init) = self.coordinator_handle_init.take() else {
            return Ok(());
        };

        // Retry coordinator connection to handle the case where the coordinator
        // is being (re)started (e.g., after snapshot backfill transition).
        let max_retries = 10;
        let mut last_err = None;
        for attempt in 0..=max_retries {
            match init.try_create_handle().await {
                Ok((handle, _log_store_rewind_start_epoch)) => {
                    self.coordinator_handle = Some(handle);
                    return Ok(());
                }
                Err(e) => {
                    if attempt < max_retries {
                        tracing::warn!(
                            attempt,
                            error = %e.as_report(),
                            "failed to create coordinator handle, retrying",
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap())
    }

    fn apply_payload(&mut self, file_path: &str, payload: Payload) -> StreamExecutorResult<()> {
        match payload {
            Payload::Position(position) => self.handler.write(file_path, position),
            Payload::PartitionInfo(partition) => {
                self.handler.add_partition_info(file_path, partition)
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        yield Message::Barrier(barrier);
        self.initialize_coordinator_handle().await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        debug_assert_eq!(op, risingwave_common::array::Op::Insert);
                        let file_path = row
                            .datum_at(0)
                            .map(|d| d.into_utf8())
                            .context("file_path should not be null")?;
                        let data = row
                            .datum_at(1)
                            .context("data should not be null")?
                            .into_bytea();
                        let payload = Payload::decode(data)?;
                        self.apply_payload(file_path, payload)?;
                    }
                }
                Message::Barrier(barrier) => {
                    let sink_metadata = self.handler.flush().await?.unwrap_or_default();
                    let epoch = barrier.epoch;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);

                    if barrier.is_stop(self.ctx.id)
                        && let Some(mut handle) = self.coordinator_handle.take()
                    {
                        let sink_metadata = sink_metadata.clone();
                        let defer_future = async move {
                            handle
                                .commit(epoch.curr, sink_metadata, None)
                                .await
                                .context("coordinator commit failed")?;
                            handle.stop().await.context("coordinator stop failed")?;
                            Ok::<(), anyhow::Error>(())
                        };

                        tokio::spawn(async move {
                            if let Err(e) = defer_future.await {
                                tracing::error!(error = %e.as_report(), "deferred coordinator commit/stop failed");
                            }
                        });
                    }

                    yield Message::Barrier(barrier);

                    if let Some(handle) = &mut self.coordinator_handle {
                        handle
                            .commit(epoch.curr, sink_metadata, None)
                            .await
                            .context("failed to send commit to coordinator")?;
                        if let Some(update_vnode_bitmap) = &update_vnode_bitmap {
                            handle
                                .update_vnode_bitmap(update_vnode_bitmap)
                                .await
                                .context("report vnode bitmap failed")?;
                        }
                    }
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }
}

impl<H> Execute for DvMergerExecutor<H>
where
    H: DvHandler,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::{Arc, Mutex};

    use risingwave_common::array::{Array, ArrayBuilder, BytesArrayBuilder, Op, Utf8ArrayBuilder};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    fn build_delete_position_chunk(positions: &[(&str, i64)]) -> StreamChunk {
        let len = positions.len();
        let mut file_path_builder = Utf8ArrayBuilder::new(len);
        let mut payload_builder = BytesArrayBuilder::new(len);

        for (path, offset) in positions {
            file_path_builder.append(Some(*path));
            let payload = Payload::encode_position(*offset).unwrap();
            payload_builder.append(Some(payload.as_slice()));
        }

        StreamChunk::from_parts(
            vec![Op::Insert; len],
            risingwave_common::array::DataChunk::new(
                vec![
                    file_path_builder.finish().into_ref(),
                    payload_builder.finish().into_ref(),
                ],
                len,
            ),
        )
    }

    #[derive(Clone)]
    struct DvHandlerMock {
        existing_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        pending_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        written_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
    }

    impl DvHandlerMock {
        fn new() -> Self {
            Self {
                existing_dvs: Arc::new(Mutex::new(HashMap::new())),
                pending_dvs: Arc::new(Mutex::new(HashMap::new())),
                written_dvs: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn with_existing_dv(self, file_path: &str, positions: BTreeSet<i64>) -> Self {
            self.existing_dvs
                .lock()
                .unwrap()
                .insert(file_path.to_string(), positions);
            self
        }
    }

    #[async_trait::async_trait]
    impl DvHandler for DvHandlerMock {
        fn write(&mut self, path: &str, pos: i64) -> StreamExecutorResult<()> {
            self.pending_dvs
                .lock()
                .unwrap()
                .entry(path.to_string())
                .or_default()
                .insert(pos);
            Ok(())
        }

        fn add_partition_info(
            &mut self,
            _path: &str,
            _partition_info: Vec<u8>,
        ) -> StreamExecutorResult<()> {
            Ok(())
        }

        async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>> {
            let pending = {
                let mut pending_dvs = self.pending_dvs.lock().unwrap();
                std::mem::take(&mut *pending_dvs)
            };
            if pending.is_empty() {
                return Ok(None);
            }

            let mut existing_dvs = self.existing_dvs.lock().unwrap();
            let mut written_dvs = self.written_dvs.lock().unwrap();
            for (file_path, positions) in pending {
                let mut merged = existing_dvs.get(&file_path).cloned().unwrap_or_default();
                merged.extend(positions);
                existing_dvs.insert(file_path.clone(), merged.clone());
                written_dvs.insert(file_path, merged);
            }
            Ok(None)
        }
    }

    fn input_schema() -> Schema {
        Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Bytea),
        ])
    }

    #[tokio::test]
    async fn test_dv_merger_basic() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[
            ("file1.parquet", 0),
            ("file1.parquet", 3),
            ("file2.parquet", 1),
        ]));
        tx.push_barrier(test_epoch(2), false);

        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let dvs = written_dvs.lock().unwrap();
        assert_eq!(dvs.get("file1.parquet").unwrap(), &BTreeSet::from([0, 3]));
        assert_eq!(dvs.get("file2.parquet").unwrap(), &BTreeSet::from([1]));
    }

    #[tokio::test]
    async fn test_dv_merger_merge_with_existing() {
        let handler =
            DvHandlerMock::new().with_existing_dv("file1.parquet", BTreeSet::from([0, 5, 10]));
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[
            ("file1.parquet", 3),
            ("file1.parquet", 5),
            ("file1.parquet", 7),
        ]));
        tx.push_barrier(test_epoch(2), false);

        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let dvs = written_dvs.lock().unwrap();
        assert_eq!(
            dvs.get("file1.parquet").unwrap(),
            &BTreeSet::from([0, 3, 5, 7, 10])
        );
    }

    #[tokio::test]
    async fn test_dv_merger_no_deletes() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_barrier(test_epoch(2), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert!(written_dvs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_dv_merger_multiple_epochs() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[("file1.parquet", 0)]));
        tx.push_barrier(test_epoch(2), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());
        assert_eq!(
            written_dvs.lock().unwrap().get("file1.parquet").unwrap(),
            &BTreeSet::from([0])
        );

        tx.push_chunk(build_delete_position_chunk(&[("file1.parquet", 2)]));
        tx.push_barrier(test_epoch(3), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert_eq!(
            written_dvs.lock().unwrap().get("file1.parquet").unwrap(),
            &BTreeSet::from([0, 2])
        );
    }
}
