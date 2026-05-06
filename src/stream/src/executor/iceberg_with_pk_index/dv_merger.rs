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

use crate::executor::prelude::*;

/// Trait abstracting DV (Deletion Vector) file operations for testability.
///
/// Implementations are responsible for reading existing DVs, merging new
/// delete positions, writing DV files, and returning the metadata that should
/// be committed on the current barrier.
#[async_trait::async_trait]
pub trait DvHandler: Send + 'static {
    fn write(&mut self, path: &str, pos: i64) -> StreamExecutorResult<()>;

    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>>;
}

/// DV Merger Executor for Iceberg V3 Sink without Equality Delete.
///
/// This stateless executor receives [`file_path`, `position`] messages from the Writer Executor,
/// merges them with historical DVs, and reports the merged DV metadata to meta on each barrier.
///
/// The upstream plan shards messages by `file_path`, so each actor only merges delete
/// positions for the files assigned to its shard.
///
/// Input schema: [`file_path`: Varchar, `position`: int64]
/// Output: Only barriers (terminal executor in the stream graph).
pub struct DvMergerExecutor<H>
where
    H: DvHandler,
{
    _ctx: ActorContextRef,
    input: Option<Executor>,
    handler: H,
}

impl<H> DvMergerExecutor<H>
where
    H: DvHandler,
{
    pub fn new(ctx: ActorContextRef, input: Executor, handler: H) -> Self {
        Self {
            _ctx: ctx,
            input: Some(input),
            handler,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        yield Message::Barrier(barrier);

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
                        let position = row
                            .datum_at(1)
                            .context("position should not be null")?
                            .into_int64();
                        self.handler.write(file_path, position)?;
                    }
                }
                Message::Barrier(barrier) => {
                    let _metadata = self.handler.flush().await?.unwrap_or_default();
                    // TODO: commit the DV metadata

                    yield Message::Barrier(barrier);
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

    use risingwave_common::array::{Array, ArrayBuilder, I64ArrayBuilder, Op, Utf8ArrayBuilder};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    fn build_delete_position_chunk(positions: &[(&str, i64)]) -> StreamChunk {
        let len = positions.len();
        let mut file_path_builder = Utf8ArrayBuilder::new(len);
        let mut position_builder = I64ArrayBuilder::new(len);

        for (path, offset) in positions {
            file_path_builder.append(Some(*path));
            position_builder.append(Some(*offset));
        }

        StreamChunk::from_parts(
            vec![Op::Insert; len],
            risingwave_common::array::DataChunk::new(
                vec![
                    file_path_builder.finish().into_ref(),
                    position_builder.finish().into_ref(),
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
            Field::unnamed(DataType::Int64),
        ])
    }

    #[tokio::test]
    async fn test_dv_merger_basic() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let mut executor = DvMergerExecutor::new(ActorContext::for_test(123), source, handler)
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

        let mut executor = DvMergerExecutor::new(ActorContext::for_test(123), source, handler)
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

        let mut executor = DvMergerExecutor::new(ActorContext::for_test(123), source, handler)
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

        let mut executor = DvMergerExecutor::new(ActorContext::for_test(123), source, handler)
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
