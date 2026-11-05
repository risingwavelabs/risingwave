// Copyright 2024 RisingWave Labs
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

use futures_async_stream::try_stream;
use itertools::Itertools;
use prometheus::core::Atomic;
use risingwave_common::array::{DataChunk, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding::{MemcmpEncoded, encode_chunk};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::error::{BatchError, Result};
use crate::executor::merge_sort::MergeSortExecutor;
use crate::monitor::BatchSpillMetrics;
use crate::spill::MIN_WORKING_SET_BYTES;
use crate::spill::spill_file::{READ_BUFFER_SIZE, SpillDir, SpillFile};
use crate::spill::spill_op::SpillBackend;
use crate::spill::spill_op::SpillBackend::Disk;

/// Most sorted runs read by one merge, which also bounds the files it keeps open.
const MAX_MERGE_FAN_IN: usize = 128;
/// About the estimated heap size of each chunk that a sorted run is written in. A merge holds up to
/// two chunks of each run it reads, so small chunks let it read many runs at once.
const RUN_CHUNK_BYTES: u64 = 256 << 10;

/// Sort Executor
///
/// Each row is encoded into a memcomparable key, and the keys are sorted by quicksort.
///
/// Before buffering a chunk, it checks that the memory the chunk takes is free, including the
/// growth of the buffer's vectors. When it is not and the buffered rows hold at least
/// [`MIN_WORKING_SET_BYTES`], it becomes an external merge sort: the buffered rows are first sorted
/// and written to disk as a sorted run, and the runs are merged at the end. A merge reads one chunk
/// of each run at a time, from as many runs as fit in the free memory, up to a quarter of the limit;
/// when more runs remain, groups of them are first merged into longer runs.
pub struct SortExecutor {
    child: BoxedExecutor,
    column_orders: Arc<Vec<ColumnOrder>>,
    identity: String,
    schema: Schema,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
}

impl Executor for SortExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl BoxedExecutorBuilder for SortExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let order_by_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::Sort)?;

        let column_orders = order_by_node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect_vec();

        let identity = source.plan_node().get_identity();
        Ok(Box::new(SortExecutor::new(
            child,
            Arc::new(column_orders),
            identity.clone(),
            source.context().get_config().developer.chunk_size,
            source.context().create_executor_mem_context(identity),
            if source.context().get_config().enable_spill {
                Some(Disk)
            } else {
                None
            },
            source.context().spill_metrics(),
        )))
    }
}

impl SortExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            child,
            column_orders,
            identity,
            schema,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
        } = *self;
        // A private context releases the remaining charges on completion, errors and cancellation.
        let mem_context = MemoryContext::new(Some(mem_context), TrAdderAtomic::new(0));
        let mut buffer = SortBuffer::new(&mem_context);
        let mut spill_dir = None;
        let mut runs = vec![];

        #[for_await]
        for chunk in child.execute() {
            let chunk = chunk?.compact_vis();
            if chunk.cardinality() == 0 {
                continue;
            }
            let keys = encode_chunk(&chunk, &column_orders)?;
            // Where to spill when the chunk does not fit in the free memory.
            let backend = if buffer.cost(&chunk, &keys) <= mem_context.free_bytes() {
                None
            } else {
                let Some(backend) = &spill_backend else {
                    return Err(BatchError::OutOfMemory(mem_context.mem_limit()));
                };
                Some(backend)
            };
            if let Some(backend) = backend
                && buffer.bytes() >= MIN_WORKING_SET_BYTES
            {
                let dir = ensure_spill_dir(&mut spill_dir, backend, &spill_metrics, &identity)?;
                runs.push(buffer.write_run(dir, &schema, chunk_size).await?);
            }
            buffer.push(chunk, keys);
            // A chunk taken beyond the free memory, to fill the minimum working set or because it
            // alone does not fit, is written out at once rather than held over the limit.
            if let Some(backend) = backend
                && buffer.bytes() >= MIN_WORKING_SET_BYTES
                && !mem_context.check_memory_usage()
            {
                let dir = ensure_spill_dir(&mut spill_dir, backend, &spill_metrics, &identity)?;
                runs.push(buffer.write_run(dir, &schema, chunk_size).await?);
            }
        }

        let Some(spill_dir) = spill_dir else {
            buffer.sort();
            let mut builder = DataChunkBuilder::new(schema.data_types(), chunk_size);
            for row in buffer.sorted_rows() {
                if let Some(chunk) = builder.append_one_row(row) {
                    yield chunk;
                }
            }
            if let Some(chunk) = builder.consume_all() {
                yield chunk;
            }
            return Ok(());
        };
        if !buffer.is_empty() {
            runs.push(buffer.write_run(&spill_dir, &schema, chunk_size).await?);
        }
        drop(buffer);

        let merge = |runs: &[SpillFile], chunk_size: usize| {
            merge_runs(
                runs,
                &schema,
                &column_orders,
                &identity,
                chunk_size,
                &mem_context,
            )
        };
        loop {
            // A merge takes at most a quarter of the limit, which leaves the rest to other operators.
            let budget = (mem_context.mem_limit() / 4).min(mem_context.free_bytes());
            let fan_in = merge_fan_in(budget, &runs);
            if fan_in >= runs.len() {
                break;
            }
            let group = runs.drain(..fan_in).collect_vec();
            let chunk_rows = run_chunk_rows(
                group.iter().map(|run| run.heap_size()).sum(),
                group.iter().map(|run| run.rows()).sum(),
                chunk_size,
            );
            let mut writer = spill_dir.create_file(&mem_context)?;
            #[for_await]
            for chunk in merge(&group, chunk_rows) {
                let chunk = chunk?;
                writer.write_chunk(&chunk).await?;
            }
            drop(group);
            runs.push(writer.finish().await?);
        }

        #[for_await]
        for chunk in merge(&runs, chunk_size) {
            yield chunk?;
        }
    }
}

/// Creates the spill directory when the sort first spills.
fn ensure_spill_dir<'a>(
    spill_dir: &'a mut Option<SpillDir>,
    spill_backend: &SpillBackend,
    spill_metrics: &Arc<BatchSpillMetrics>,
    identity: &str,
) -> Result<&'a SpillDir> {
    if spill_dir.is_none() {
        info!(identity, "batch sort executor starts to spill");
        *spill_dir = Some(SpillDir::create(
            spill_backend.clone(),
            spill_metrics.clone(),
        )?);
    }
    Ok(spill_dir.as_ref().unwrap())
}

/// Merges sorted runs into one sorted stream.
fn merge_runs(
    runs: &[SpillFile],
    schema: &Schema,
    column_orders: &Arc<Vec<ColumnOrder>>,
    identity: &str,
    chunk_size: usize,
    mem_context: &MemoryContext,
) -> BoxedDataChunkStream {
    let inputs = runs
        .iter()
        .map(|run| {
            Box::new(WrapStreamExecutor::new(
                schema.clone(),
                run.read(mem_context),
            )) as BoxedExecutor
        })
        .collect();
    Box::new(MergeSortExecutor::new(
        inputs,
        column_orders.clone(),
        schema.clone(),
        format!("{identity}-merge"),
        chunk_size,
        mem_context.clone(),
    ))
    .execute()
}

/// How many runs, from the first, one merge reads: as many as fit in `budget`, given that each holds
/// a read buffer and up to two of its chunks, but at least two and at most [`MAX_MERGE_FAN_IN`].
fn merge_fan_in(budget: u64, runs: &[SpillFile]) -> usize {
    let mut used = 0;
    runs.iter()
        .take(MAX_MERGE_FAN_IN)
        .take_while(|run| {
            used += READ_BUFFER_SIZE as u64 + 2 * run.max_chunk_heap_size();
            used <= budget
        })
        .count()
        .max(2)
}

/// Rows per chunk of a run whose rows have an estimated heap size of `heap_size` in all, so that
/// its chunks hold about [`RUN_CHUNK_BYTES`].
fn run_chunk_rows(heap_size: u64, rows: u64, chunk_size: usize) -> usize {
    let row_bytes = (heap_size / rows.max(1)).max(1);
    (RUN_CHUNK_BYTES / row_bytes).clamp(1, chunk_size as u64) as usize
}

/// Buffered input rows with their sort keys. Everything it holds is charged to its context.
struct SortBuffer {
    mem_context: MemoryContext,
    chunks: Vec<DataChunk, MonitoredGlobalAlloc>,
    /// The sort key, chunk index and row index of each buffered row.
    rows: Vec<(MemcmpEncoded, u32, u32), MonitoredGlobalAlloc>,
    /// Heap sizes of the chunks and the keys, which the allocator of the vectors does not see.
    heap_size: i64,
}

impl SortBuffer {
    fn new(mem_context: &MemoryContext) -> Self {
        Self {
            mem_context: mem_context.clone(),
            chunks: Vec::new_in(mem_context.global_allocator()),
            rows: Vec::new_in(mem_context.global_allocator()),
            heap_size: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Bytes of the buffered rows and their keys.
    fn bytes(&self) -> u64 {
        (self.heap_size as usize + self.rows.len() * size_of::<(MemcmpEncoded, u32, u32)>()) as u64
    }

    /// Memory that buffering a compacted chunk with its sort keys takes: their heap sizes, plus the
    /// new allocations of the vectors that have to grow, made while the old ones are still held.
    fn cost(&self, chunk: &DataChunk, keys: &[MemcmpEncoded]) -> u64 {
        let growth = growth_bytes(&self.chunks, 1) + growth_bytes(&self.rows, keys.len());
        (chunk_heap_size(chunk, keys) + growth) as u64
    }

    /// Buffers a compacted chunk with its sort keys. It is charged even if that exceeds the limit.
    fn push(&mut self, chunk: DataChunk, keys: Vec<MemcmpEncoded>) {
        let heap_size = chunk_heap_size(&chunk, &keys);
        if let Some(capacity) = grown_capacity(&self.chunks, 1) {
            self.chunks.reserve_exact(capacity - self.chunks.len());
        }
        if let Some(capacity) = grown_capacity(&self.rows, keys.len()) {
            self.rows.reserve_exact(capacity - self.rows.len());
        }
        let chunk_idx = u32::try_from(self.chunks.len()).unwrap();
        self.rows.extend(
            keys.into_iter()
                .enumerate()
                .map(|(row_idx, key)| (key, chunk_idx, u32::try_from(row_idx).unwrap())),
        );
        self.chunks.push(chunk);
        self.heap_size += heap_size as i64;
        self.mem_context.add_unchecked(heap_size as i64);
    }

    fn sort(&mut self) {
        self.rows.sort_unstable_by(|(a, ..), (b, ..)| a.cmp(b));
    }

    fn sorted_rows(&self) -> impl Iterator<Item = RowRef<'_>> {
        self.rows.iter().map(|(_, chunk_idx, row_idx)| {
            self.chunks[*chunk_idx as usize].row_at_unchecked_vis(*row_idx as usize)
        })
    }

    /// Writes the buffered rows to `dir` as a sorted run and empties the buffer.
    async fn write_run(
        &mut self,
        dir: &SpillDir,
        schema: &Schema,
        chunk_size: usize,
    ) -> Result<SpillFile> {
        self.sort();
        let heap_size = self
            .chunks
            .iter()
            .map(|chunk| chunk.estimated_heap_size() as u64)
            .sum();
        let chunk_rows = run_chunk_rows(heap_size, self.rows.len() as u64, chunk_size);
        let mut writer = dir.create_file(&self.mem_context)?;
        let mut builder = DataChunkBuilder::new(schema.data_types(), chunk_rows);
        for row in self.sorted_rows() {
            if let Some(chunk) = builder.append_one_row(row) {
                writer.write_chunk(&chunk).await?;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            writer.write_chunk(&chunk).await?;
        }
        let run = writer.finish().await?;
        self.clear();
        Ok(run)
    }

    fn clear(&mut self) {
        // Replaced rather than cleared, since their capacity is charged too.
        self.rows = Vec::new_in(self.mem_context.global_allocator());
        self.chunks = Vec::new_in(self.mem_context.global_allocator());
        self.mem_context.add_unchecked(-self.heap_size);
        self.heap_size = 0;
    }
}

fn chunk_heap_size(chunk: &DataChunk, keys: &[MemcmpEncoded]) -> usize {
    chunk.estimated_heap_size()
        + keys
            .iter()
            .map(|key| key.estimated_heap_size())
            .sum::<usize>()
}

/// The capacity `vec` grows to before it takes `additional` more items, if it has to. It doubles as
/// `Vec` does, but explicitly, so that [`SortBuffer::cost`] knows the growth before it happens.
fn grown_capacity<T>(vec: &Vec<T, MonitoredGlobalAlloc>, additional: usize) -> Option<usize> {
    let needed = vec.len() + additional;
    (needed > vec.capacity()).then(|| needed.max(2 * vec.capacity()))
}

fn growth_bytes<T>(vec: &Vec<T, MonitoredGlobalAlloc>, additional: usize) -> usize {
    grown_capacity(vec, additional).map_or(0, |capacity| capacity * size_of::<T>())
}

impl Drop for SortBuffer {
    fn drop(&mut self) {
        self.mem_context.add_unchecked(-self.heap_size);
    }
}

impl SortExecutor {
    pub fn new(
        child: BoxedExecutor,
        column_orders: Arc<Vec<ColumnOrder>>,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            column_orders,
            identity,
            schema,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::*;
    use risingwave_common::catalog::Field;
    use risingwave_common::row::Row;
    use risingwave_common::types::{
        DataType, Date, F32, Interval, Scalar, StructType, Time, Timestamp,
    };
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::{MockExecutor, PeakCounter};
    use crate::spill::spill_file::WRITE_BUFFER_SIZE;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_simple_order_by_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 3
             2 2
             3 1",
        ));
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];

        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor2".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.as_int32().value_at(0), Some(3));
            assert_eq!(col0.as_int32().value_at(1), Some(2));
            assert_eq!(col0.as_int32().value_at(2), Some(1));
        }
    }

    #[tokio::test]
    async fn test_encoding_for_float() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Float32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            " f    F
             -2.2  3.3
             -1.1  2.2
              1.1  1.1
              2.2 -1.1
              3.3 -2.2",
        ));
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor2".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Float32);
        assert_eq!(fields[1].data_type, DataType::Float64);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.as_float32().value_at(0), Some(3.3.into()));
            assert_eq!(col0.as_float32().value_at(1), Some(2.2.into()));
            assert_eq!(col0.as_float32().value_at(2), Some(1.1.into()));
            assert_eq!(col0.as_float32().value_at(3), Some((-1.1).into()));
            assert_eq!(col0.as_float32().value_at(4), Some((-2.2).into()));
        }
    }

    #[tokio::test]
    async fn test_bsc_for_string() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "T   T
             1.1 3.3
             2.2 2.2
             3.3 1.1",
        ));
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor2".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Varchar);
        assert_eq!(fields[1].data_type, DataType::Varchar);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.as_utf8().value_at(0), Some("3.3"));
            assert_eq!(col0.as_utf8().value_at(1), Some("2.2"));
            assert_eq!(col0.as_utf8().value_at(2), Some("1.1"));
        }
    }

    // TODO: write following tests in a more concise way
    #[tokio::test]
    async fn test_encoding_for_boolean_int32_float64() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Boolean),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Float64),
            ],
        };
        // f   3    .
        // t   3    .
        // .   .    3.5
        // .   .    -4.3
        // .   .    .
        let input_chunk = DataChunk::new(
            vec![
                BoolArray::from_iter([Some(false), Some(true), None, None, None]).into_ref(),
                I32Array::from_iter([Some(3), Some(3), None, None, None]).into_ref(),
                F64Array::from_iter([None, None, Some(3.5), Some(-4.3), None]).into_ref(),
            ],
            5,
        );
        // .   .   -4.3
        // .   .   3.5
        // .   .   .
        // f   3   .
        // t   3   .
        let output_chunk = DataChunk::new(
            vec![
                BoolArray::from_iter([None, None, None, Some(false), Some(true)]).into_ref(),
                I32Array::from_iter([None, None, None, Some(3), Some(3)]).into_ref(),
                F64Array::from_iter([Some(-4.3), Some(3.5), None, None, None]).into_ref(),
            ],
            5,
        );
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(input_chunk);
        let column_orders = vec![
            ColumnOrder {
                column_index: 2,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::descending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert_eq!(res.unwrap().unwrap(), output_chunk)
    }

    #[tokio::test]
    async fn test_encoding_for_decimal_date_varchar() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Decimal),
                Field::unnamed(DataType::Date),
            ],
        };
        // abc       .     123
        // b         -3    789
        // abc       .     456
        // abcdefgh  .     .
        // b         7     345
        let input_chunk = DataChunk::new(
            vec![
                Utf8Array::from_iter(["abc", "b", "abc", "abcdefgh", "b"]).into_ref(),
                DecimalArray::from_iter([None, Some((-3).into()), None, None, Some(7.into())])
                    .into_ref(),
                DateArray::from_iter([
                    Some(Date::with_days_since_ce(123).unwrap()),
                    Some(Date::with_days_since_ce(789).unwrap()),
                    Some(Date::with_days_since_ce(456).unwrap()),
                    None,
                    Some(Date::with_days_since_ce(345).unwrap()),
                ])
                .into_ref(),
            ],
            5,
        );
        // b         7     345
        // b         -3    789
        // abcdefgh  .     .
        // abc       .     123
        // abc       .     456
        let output_chunk = DataChunk::new(
            vec![
                Utf8Array::from_iter(["b", "b", "abcdefgh", "abc", "abc"]).into_ref(),
                DecimalArray::from_iter([Some(7.into()), Some((-3).into()), None, None, None])
                    .into_ref(),
                DateArray::from_iter([
                    Some(Date::with_days_since_ce(345).unwrap()),
                    Some(Date::with_days_since_ce(789).unwrap()),
                    None,
                    Some(Date::with_days_since_ce(123).unwrap()),
                    Some(Date::with_days_since_ce(456).unwrap()),
                ])
                .into_ref(),
            ],
            5,
        );
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(input_chunk);
        let column_orders = vec![
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::descending(),
            },
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::descending(),
            },
            ColumnOrder {
                column_index: 2,
                order_type: OrderType::ascending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert_eq!(res.unwrap().unwrap(), output_chunk)
    }

    #[tokio::test]
    async fn test_encoding_for_time_timestamp_interval() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Time),
                Field::unnamed(DataType::Timestamp),
                Field::unnamed(DataType::Interval),
            ],
        };
        // .     1:23  .
        // 4:56  4:56  1:2:3
        // .     7:89  .
        // 4:56  4:56  4:5:6
        // 7:89  .     .
        let input_chunk = DataChunk::new(
            vec![
                TimeArray::from_iter([
                    None,
                    Some(Time::with_secs_nano(4, 56).unwrap()),
                    None,
                    Some(Time::with_secs_nano(4, 56).unwrap()),
                    Some(Time::with_secs_nano(7, 89).unwrap()),
                ])
                .into_ref(),
                TimestampArray::from_iter([
                    Some(Timestamp::with_secs_nsecs(1, 23).unwrap()),
                    Some(Timestamp::with_secs_nsecs(4, 56).unwrap()),
                    Some(Timestamp::with_secs_nsecs(7, 89).unwrap()),
                    Some(Timestamp::with_secs_nsecs(4, 56).unwrap()),
                    None,
                ])
                .into_ref(),
                IntervalArray::from_iter([
                    None,
                    Some(Interval::from_month_day_usec(1, 2, 3)),
                    None,
                    Some(Interval::from_month_day_usec(4, 5, 6)),
                    None,
                ])
                .into_ref(),
            ],
            5,
        );
        // 4:56  4:56  4:5:6
        // 4:56  4:56  1:2:3
        // 7:89  .     .
        // .     1:23  .
        // .     7:89  .
        let output_chunk = DataChunk::new(
            vec![
                TimeArray::from_iter([
                    Some(Time::with_secs_nano(4, 56).unwrap()),
                    Some(Time::with_secs_nano(4, 56).unwrap()),
                    Some(Time::with_secs_nano(7, 89).unwrap()),
                    None,
                    None,
                ])
                .into_ref(),
                TimestampArray::from_iter([
                    Some(Timestamp::with_secs_nsecs(4, 56).unwrap()),
                    Some(Timestamp::with_secs_nsecs(4, 56).unwrap()),
                    None,
                    Some(Timestamp::with_secs_nsecs(1, 23).unwrap()),
                    Some(Timestamp::with_secs_nsecs(7, 89).unwrap()),
                ])
                .into_ref(),
                IntervalArray::from_iter([
                    Some(Interval::from_month_day_usec(4, 5, 6)),
                    Some(Interval::from_month_day_usec(1, 2, 3)),
                    None,
                    None,
                    None,
                ])
                .into_ref(),
            ],
            5,
        );
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(input_chunk);
        let column_orders = vec![
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 2,
                order_type: OrderType::descending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            column_orders.into(),
            "SortExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert_eq!(res.unwrap().unwrap(), output_chunk)
    }

    #[tokio::test]
    async fn test_encoding_for_struct_list() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(
                    StructType::unnamed(vec![DataType::Varchar, DataType::Float32]).into(),
                ),
                Field::unnamed(DataType::Int64.list()),
            ],
        };
        let mut struct_builder = StructArrayBuilder::with_type(
            0,
            DataType::Struct(StructType::unnamed(vec![
                DataType::Varchar,
                DataType::Float32,
            ])),
        );
        let mut list_builder = ListArrayBuilder::with_type(0, DataType::Int64.list());
        // {abcd, -1.2}   .
        // {c, 0}         [1, ., 3]
        // {c, .}         .
        // {c, 0}         [2]
        // {., 3.4}       .
        let input_chunk = DataChunk::new(
            vec![
                {
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("abcd".into()),
                            Some(F32::from(-1.2).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("c".into()),
                            Some(F32::from(0.0).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![Some("c".into()), None]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("c".into()),
                            Some(F32::from(0.0).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![None, Some(F32::from(3.4).to_scalar_value())]),
                    }));
                    struct_builder.finish().into_ref()
                },
                {
                    list_builder.append(None);
                    list_builder.append(Some(
                        ListValue::from_iter([Some(1i64), None, Some(3i64)]).as_scalar_ref(),
                    ));
                    list_builder.append(None);
                    list_builder.append(Some(ListValue::from_iter([2i64]).as_scalar_ref()));
                    list_builder.append(None);
                    list_builder.finish().into_ref()
                },
            ],
            5,
        );
        let mut struct_builder = StructArrayBuilder::with_type(
            0,
            DataType::Struct(StructType::unnamed(vec![
                DataType::Varchar,
                DataType::Float32,
            ])),
        );
        let mut list_builder = ListArrayBuilder::with_type(0, DataType::Int64.list());
        // {abcd, -1.2}   .
        // {c, 0}         [2]
        // {c, 0}         [1, ., 3]
        // {c, .}         .
        // {., 3.4}       .
        let output_chunk = DataChunk::new(
            vec![
                {
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("abcd".into()),
                            Some(F32::from(-1.2).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("c".into()),
                            Some(F32::from(0.0).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![
                            Some("c".into()),
                            Some(F32::from(0.0).to_scalar_value()),
                        ]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![Some("c".into()), None]),
                    }));
                    struct_builder.append(Some(StructRef::ValueRef {
                        val: &StructValue::new(vec![None, Some(F32::from(3.4).to_scalar_value())]),
                    }));
                    struct_builder.finish().into_ref()
                },
                {
                    list_builder.append(None);
                    list_builder.append(Some(ListValue::from_iter([2i64]).as_scalar_ref()));
                    list_builder.append(Some(
                        ListValue::from_iter([Some(1i64), None, Some(3i64)]).as_scalar_ref(),
                    ));
                    list_builder.append(None);
                    list_builder.append(None);
                    list_builder.finish().into_ref()
                },
            ],
            5,
        );
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(input_chunk);
        let column_orders = vec![
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::descending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ));

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert_eq!(res.unwrap().unwrap(), output_chunk)
    }

    #[tokio::test]
    async fn test_spill_out() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Float32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            " f    F
             -2.2  3.3
             -1.1  2.2
              1.1  1.1
              2.2 -1.1
              3.3 -2.2",
        ));
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let order_by_executor = Box::new(SortExecutor::new(
            Box::new(mock_executor),
            Arc::new(column_orders),
            "SortExecutor2".to_owned(),
            CHUNK_SIZE,
            MemoryContext::for_spill_test(),
            Some(SpillBackend::Memory),
            BatchSpillMetrics::for_test(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Float32);
        assert_eq!(fields[1].data_type, DataType::Float64);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.as_float32().value_at(0), Some(3.3.into()));
            assert_eq!(col0.as_float32().value_at(1), Some(2.2.into()));
            assert_eq!(col0.as_float32().value_at(2), Some(1.1.into()));
            assert_eq!(col0.as_float32().value_at(3), Some((-1.1).into()));
            assert_eq!(col0.as_float32().value_at(4), Some((-2.2).into()));
        }
    }

    /// A deterministic input with NULLs, duplicate keys and invisible rows.
    fn spill_test_input(chunks: usize, rows_per_chunk: usize, padding: usize) -> Vec<DataChunk> {
        let mut input = vec![];
        let mut state = 42u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 33
        };
        let padding = "x".repeat(padding);
        for chunk in 0..chunks {
            let (mut k1, mut k2, mut v, mut vis) = (vec![], vec![], vec![], vec![]);
            for row in 0..rows_per_chunk {
                let n = next();
                k1.push((n % 7 != 0).then_some((n % 50) as i32));
                k2.push((n % 11 != 0).then(|| format!("{}{padding}", n % 13)));
                v.push(Some((chunk * rows_per_chunk + row) as i64));
                vis.push(n % 5 != 0);
            }
            input.push(DataChunk::new(
                vec![
                    I32Array::from_iter(k1).into_ref(),
                    Utf8Array::from_iter(k2.iter().map(|s| s.as_deref())).into_ref(),
                    I64Array::from_iter(v).into_ref(),
                ],
                risingwave_common::bitmap::Bitmap::from_iter(vis),
            ));
        }
        input
    }

    fn schema_of(input: &[DataChunk]) -> Schema {
        Schema::new(
            input[0]
                .columns()
                .iter()
                .map(|column| Field::unnamed(column.data_type()))
                .collect(),
        )
    }

    fn row_count(input: &[DataChunk]) -> usize {
        input.iter().map(|chunk| chunk.cardinality()).sum()
    }

    fn mock_input(chunks: &[DataChunk]) -> BoxedExecutor {
        let mut executor = MockExecutor::new(schema_of(chunks));
        for chunk in chunks {
            executor.add(chunk.clone());
        }
        Box::new(executor)
    }

    fn sort_executor(
        child: BoxedExecutor,
        column_orders: &[ColumnOrder],
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
    ) -> BoxedExecutor {
        Box::new(SortExecutor::new(
            child,
            Arc::new(column_orders.to_vec()),
            "SortExecutor".to_owned(),
            CHUNK_SIZE,
            mem_context,
            spill_backend,
            BatchSpillMetrics::for_test(),
        ))
    }

    /// The sort keys in output order and the sorted output rows. `on_chunk` runs after each output
    /// chunk.
    async fn collect_sorted(
        executor: BoxedExecutor,
        column_orders: &[ColumnOrder],
        mut on_chunk: impl FnMut(),
    ) -> (Vec<Vec<u8>>, Vec<String>) {
        let (mut keys, mut rows) = (vec![], vec![]);
        let mut stream = executor.execute();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            keys.extend(
                encode_chunk(&chunk, column_orders)
                    .unwrap()
                    .into_iter()
                    .map(|key| key.as_ref().to_vec()),
            );
            rows.extend(chunk.rows().map(|row| format!("{:?}", row.to_owned_row())));
            on_chunk();
        }
        rows.sort();
        (keys, rows)
    }

    /// Sorts the `rows` rows of `child` with spilling, and checks that all of them come out in order.
    async fn check_spilled_sort(
        child: BoxedExecutor,
        rows: usize,
        column_orders: &[ColumnOrder],
        mem_context: MemoryContext,
        spill_backend: SpillBackend,
        on_chunk: impl FnMut(),
    ) {
        let sort = sort_executor(child, column_orders, mem_context, Some(spill_backend));
        let (keys, output) = collect_sorted(sort, column_orders, on_chunk).await;
        assert!(keys.is_sorted());
        assert_eq!(output.len(), rows);
    }

    #[tokio::test]
    async fn test_spill_matches_in_memory_sort() {
        for column_orders in [
            vec![
                ColumnOrder::new(0, OrderType::ascending()),
                ColumnOrder::new(1, OrderType::descending()),
            ],
            vec![
                ColumnOrder::new(0, OrderType::ascending_nulls_first()),
                ColumnOrder::new(2, OrderType::descending()),
            ],
            vec![ColumnOrder::new(1, OrderType::descending_nulls_last())],
        ] {
            let input = spill_test_input(40, 1024, 100);
            let sort = |mem_context, spill_backend| {
                sort_executor(
                    mock_input(&input),
                    &column_orders,
                    mem_context,
                    spill_backend,
                )
            };
            let expected =
                collect_sorted(sort(MemoryContext::none(), None), &column_orders, || {}).await;
            // With no memory every `MIN_WORKING_SET_BYTES` of input becomes a run, and the runs are
            // merged in passes.
            let actual = collect_sorted(
                sort(MemoryContext::for_spill_test(), Some(SpillBackend::Memory)),
                &column_orders,
                || {},
            )
            .await;
            assert_eq!(actual, expected);
        }
    }

    /// The sort stays within its limit, plus the write buffer of the run it writes, which grows to
    /// hold a chunk larger than it: about 16 MB of rows whose run chunks are about as large as the
    /// buffer, and narrow rows, whose chunks are far smaller and whose sort index takes about half
    /// their memory. The growth of the index is counted before it is allocated.
    #[tokio::test]
    async fn test_spill_memory_is_bounded() {
        let limit: u64 = 2 << 20;
        let write_buffer = WRITE_BUFFER_SIZE as u64;
        for (input, bound) in [
            (spill_test_input(40, 1024, 500), limit + 2 * write_buffer),
            (spill_test_input(200, 1024, 0), limit + write_buffer),
        ] {
            let counter = PeakCounter::default();
            let root = MemoryContext::root(counter.clone(), limit);
            check_spilled_sort(
                mock_input(&input),
                row_count(&input),
                &[ColumnOrder::new(2, OrderType::descending())],
                root.clone(),
                SpillBackend::Memory,
                || {},
            )
            .await;
            assert_eq!(root.get_bytes_used(), 0);
            assert!(
                counter.peak() <= bound as i64,
                "peak {} bytes",
                counter.peak()
            );
        }
    }

    fn distinct_keys(chunk: usize, rows: usize) -> ArrayRef {
        I64Array::from_iter((0..rows).map(|row| {
            Some(((chunk * rows + row) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) as i64)
        }))
        .into_ref()
    }

    /// Chunks of `rows` rows: a distinct key and 16 `KiB` of text.
    fn wide_rows(chunks: usize, rows: usize) -> Vec<DataChunk> {
        let wide = "x".repeat(16 << 10);
        (0..chunks)
            .map(|chunk| {
                DataChunk::new(
                    vec![
                        distinct_keys(chunk, rows),
                        Utf8Array::from_iter((0..rows).map(|_| Some(wide.as_str()))).into_ref(),
                    ],
                    rows,
                )
            })
            .collect()
    }

    /// Chunks of 1024 rows: a distinct key and 100 NULL int8 columns.
    fn null_columns(chunks: usize) -> Vec<DataChunk> {
        (0..chunks)
            .map(|chunk| {
                let mut columns = vec![distinct_keys(chunk, 1024)];
                columns.extend(
                    (0..100)
                        .map(|_| I64Array::from_iter((0..1024).map(|_| None::<i64>)).into_ref()),
                );
                DataChunk::new(columns, 1024)
            })
            .collect()
    }

    /// Yields `input`, and then releases the `limit` bytes that `others` holds.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn read_then_release(input: Vec<DataChunk>, others: Option<MemoryContext>, limit: u64) {
        for chunk in input {
            yield chunk;
        }
        if let Some(others) = others {
            others.add_unchecked(-(limit as i64));
        }
    }

    /// Sorts `input` by its first column under `limit` while other queries hold the whole limit,
    /// until the input is read or throughout. Returns the peak memory of the sort.
    async fn sort_beside_others(
        input: Vec<DataChunk>,
        limit: u64,
        hold_throughout: bool,
        spill_backend: SpillBackend,
    ) -> i64 {
        let root = MemoryContext::root(TrAdderAtomic::new(0), limit);
        let others = MemoryContext::new(Some(root.clone()), TrAdderAtomic::new(0));
        others.add_unchecked(limit as i64);
        let (schema, rows) = (schema_of(&input), row_count(&input));
        let release = (!hold_throughout).then(|| others.clone());
        let child = WrapStreamExecutor::new(schema, read_then_release(input, release, limit));
        let counter = PeakCounter::default();
        check_spilled_sort(
            Box::new(child),
            rows,
            &[ColumnOrder::new(0, OrderType::ascending())],
            MemoryContext::new(Some(root), counter.clone()),
            spill_backend,
            || {},
        )
        .await;
        counter.peak()
    }

    /// Rows that take much more memory than disk space, being wide or full of NULLs, are sorted
    /// while other queries hold the whole limit, so that every `MIN_WORKING_SET_BYTES` of input
    /// becomes a run. When the memory is free again once the input is read, the runs are merged in
    /// passes within a quarter of the limit. When it stays held, they are merged two at a time, each
    /// with a read buffer on disk: the sort holds no more than the minimum working set with the chunk
    /// that completes it, or two runs being merged, plus the write buffer of the run it writes.
    #[tokio::test]
    async fn test_spill_merge_memory_is_bounded() {
        let limit = 16 << 20;
        for input in [wide_rows(32, 64), null_columns(32)] {
            let peak = sort_beside_others(input, limit, false, SpillBackend::Memory).await;
            assert!(
                peak <= (limit / 4 + MIN_WORKING_SET_BYTES) as i64,
                "peak {peak} bytes"
            );
        }
        let peak = sort_beside_others(wide_rows(8, 64), limit, true, SpillBackend::Disk).await;
        assert!(
            peak <= (2 * MIN_WORKING_SET_BYTES + WRITE_BUFFER_SIZE as u64) as i64,
            "peak {peak} bytes without free memory"
        );
    }

    /// A chunk larger than the limit cannot be split, so the sort takes it, but it writes the chunk out
    /// at once rather than hold it over the limit while the output is read.
    #[tokio::test]
    async fn test_spill_oversized_chunk_is_written_out() {
        let limit: u64 = 2 << 20;
        let root = MemoryContext::root(TrAdderAtomic::new(0), limit);
        let input = wide_rows(1, 512);
        check_spilled_sort(
            mock_input(&input),
            row_count(&input),
            &[ColumnOrder::new(0, OrderType::ascending())],
            root.clone(),
            SpillBackend::Memory,
            || {
                let held = root.get_bytes_used();
                assert!(
                    held <= limit as i64,
                    "{held} bytes held while the output is read"
                );
            },
        )
        .await;
    }

    /// Writing a run releases all the buffer's memory, so that the next run gets the whole budget.
    #[tokio::test]
    async fn test_sort_buffer_releases_memory_after_run() {
        let ctx = MemoryContext::root(TrAdderAtomic::new(0), u64::MAX);
        let dir = SpillDir::create(SpillBackend::Memory, BatchSpillMetrics::for_test()).unwrap();
        let orders = [ColumnOrder::new(2, OrderType::ascending())];
        let input = spill_test_input(4, 1024, 0);
        let (schema, rows) = (schema_of(&input), row_count(&input));
        let mut buffer = SortBuffer::new(&ctx);
        for chunk in input {
            let chunk = chunk.compact_vis();
            let keys = encode_chunk(&chunk, &orders).unwrap();
            buffer.push(chunk, keys);
        }
        let run = buffer.write_run(&dir, &schema, CHUNK_SIZE).await.unwrap();
        assert_eq!(run.rows(), rows as u64);
        assert_eq!(ctx.get_bytes_used(), 0);
    }
}
