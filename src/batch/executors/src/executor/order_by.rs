// Copyright 2025 RisingWave Labs
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

use bytes::Bytes;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding::encode_chunk;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::Message;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::data::DataChunk as PbDataChunk;

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::error::{BatchError, Result};
use crate::executor::merge_sort::MergeSortExecutor;
use crate::monitor::BatchSpillMetrics;
use crate::spill::spill_op::SpillBackend::Disk;
use crate::spill::spill_op::{
    DEFAULT_SPILL_PARTITION_NUM, SPILL_AT_LEAST_MEMORY, SpillBackend, SpillOp,
};

/// Sort Executor
///
/// High-level idea:
/// 1. Load data chunks from child executor
/// 2. Serialize each row into memcomparable format
/// 3. Sort the serialized rows by quicksort
/// 4. Build and yield data chunks according to the row order
pub struct SortExecutor {
    child: BoxedExecutor,
    column_orders: Arc<Vec<ColumnOrder>>,
    identity: String,
    schema: Schema,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    /// The upper bound of memory usage for this executor.
    memory_upper_bound: Option<u64>,
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
        let child_schema = self.child.schema().clone();
        let mut need_to_spill = false;
        // If the memory upper bound is less than 1MB, we don't need to check memory usage.
        let check_memory = match self.memory_upper_bound {
            Some(upper_bound) => upper_bound > SPILL_AT_LEAST_MEMORY,
            None => true,
        };

        let mut chunk_builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        let mut chunks = Vec::new_in(self.mem_context.global_allocator());

        let mut input_stream = self.child.execute();
        #[for_await]
        for chunk in &mut input_stream {
            let chunk = chunk?.compact();
            let chunk_estimated_heap_size = chunk.estimated_heap_size();
            chunks.push(chunk);
            if !self.mem_context.add(chunk_estimated_heap_size as i64) && check_memory {
                if self.spill_backend.is_some() {
                    need_to_spill = true;
                    break;
                } else {
                    Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                }
            }
        }

        let mut encoded_rows =
            Vec::with_capacity_in(chunks.len(), self.mem_context.global_allocator());

        for chunk in &chunks {
            let encoded_chunk = encode_chunk(chunk, &self.column_orders)?;
            let chunk_estimated_heap_size = encoded_chunk
                .iter()
                .map(|x| x.estimated_heap_size())
                .sum::<usize>();
            encoded_rows.extend(
                encoded_chunk
                    .into_iter()
                    .enumerate()
                    .map(|(row_id, row)| (chunk.row_at_unchecked_vis(row_id), row)),
            );
            if !self.mem_context.add(chunk_estimated_heap_size as i64) && check_memory {
                if self.spill_backend.is_some() {
                    need_to_spill = true;
                    break;
                } else {
                    Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                }
            }
        }

        if need_to_spill {
            // A spilling version of sort, a.k.a. external sort.
            // When SortExecutor told memory is insufficient, SortSpillManager will start to partition the sort buffer and spill to disk.
            // After spilling the sort buffer, SortSpillManager will consume all chunks from its input executor.
            // Finally, we would get e.g. 20 partitions. Each partition should contain a portion of the original input data.
            // A sub SortExecutor would be used to sort each partition respectively and then a MergeSortExecutor would be used to merge all sorted partitions.
            // If memory is still not enough in the sub SortExecutor, it will spill its inputs recursively.
            info!("batch sort executor {} starts to spill out", &self.identity);
            let mut sort_spill_manager = SortSpillManager::new(
                self.spill_backend.clone().unwrap(),
                &self.identity,
                DEFAULT_SPILL_PARTITION_NUM,
                child_schema.data_types(),
                self.chunk_size,
                self.spill_metrics.clone(),
            )?;
            sort_spill_manager.init_writers().await?;

            // Release memory
            drop(encoded_rows);

            // Spill buffer
            for chunk in chunks {
                sort_spill_manager.write_input_chunk(chunk).await?;
            }

            // Spill input chunks.
            #[for_await]
            for chunk in input_stream {
                let chunk: DataChunk = chunk?;
                sort_spill_manager.write_input_chunk(chunk).await?;
            }

            sort_spill_manager.close_writers().await?;

            let partition_num = sort_spill_manager.partition_num;
            // Merge sorted-partitions
            let mut sorted_inputs: Vec<BoxedExecutor> = Vec::with_capacity(partition_num);
            for i in 0..partition_num {
                let partition_size = sort_spill_manager.estimate_partition_size(i).await?;

                let input_stream = sort_spill_manager.read_input_partition(i).await?;

                let sub_sort_executor: SortExecutor = SortExecutor::new_inner(
                    Box::new(WrapStreamExecutor::new(child_schema.clone(), input_stream)),
                    self.column_orders.clone(),
                    format!("{}-sub{}", self.identity.clone(), i),
                    self.chunk_size,
                    self.mem_context.clone(),
                    self.spill_backend.clone(),
                    self.spill_metrics.clone(),
                    Some(partition_size),
                );

                debug!(
                    "create sub_sort {} for sort {} to spill",
                    sub_sort_executor.identity, self.identity
                );

                sorted_inputs.push(Box::new(sub_sort_executor));
            }

            let merge_sort = MergeSortExecutor::new(
                sorted_inputs,
                self.column_orders.clone(),
                self.schema.clone(),
                format!("{}-merge-sort", self.identity.clone()),
                self.chunk_size,
                self.mem_context.clone(),
            );

            #[for_await]
            for chunk in Box::new(merge_sort).execute() {
                yield chunk?;
            }
        } else {
            encoded_rows.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

            for (row, _) in encoded_rows {
                if let Some(spilled) = chunk_builder.append_one_row(row) {
                    yield spilled
                }
            }

            if let Some(spilled) = chunk_builder.consume_all() {
                yield spilled
            }
        }
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
        Self::new_inner(
            child,
            column_orders,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            None,
        )
    }

    fn new_inner(
        child: BoxedExecutor,
        column_orders: Arc<Vec<ColumnOrder>>,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        memory_upper_bound: Option<u64>,
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
            memory_upper_bound,
        }
    }
}

/// `SortSpillManager` is used to manage how to write spill data file and read them back.
/// The spill data first need to be partitioned in a round-robin way. Each partition contains 1 file: `input_chunks_file`
/// The spill file consume a data chunk and serialize the chunk into a protobuf bytes.
/// Finally, spill file content will look like the below.
/// The file write pattern is append-only and the read pattern is sequential scan.
/// This can maximize the disk IO performance.
///
/// ```text
/// [proto_len]
/// [proto_bytes]
/// ...
/// [proto_len]
/// [proto_bytes]
/// ```
struct SortSpillManager {
    op: SpillOp,
    partition_num: usize,
    round_robin_idx: usize,
    input_writers: Vec<opendal::Writer>,
    input_chunk_builders: Vec<DataChunkBuilder>,
    child_data_types: Vec<DataType>,
    spill_chunk_size: usize,
    spill_metrics: Arc<BatchSpillMetrics>,
}

impl SortSpillManager {
    fn new(
        spill_backend: SpillBackend,
        agg_identity: &String,
        partition_num: usize,
        child_data_types: Vec<DataType>,
        spill_chunk_size: usize,
        spill_metrics: Arc<BatchSpillMetrics>,
    ) -> Result<Self> {
        let suffix_uuid = uuid::Uuid::new_v4();
        let dir = format!("/{}-{}/", agg_identity, suffix_uuid);
        let op = SpillOp::create(dir, spill_backend)?;
        let input_writers = Vec::with_capacity(partition_num);
        let input_chunk_builders = Vec::with_capacity(partition_num);
        Ok(Self {
            op,
            partition_num,
            input_writers,
            input_chunk_builders,
            round_robin_idx: 0,
            child_data_types,
            spill_chunk_size,
            spill_metrics,
        })
    }

    async fn init_writers(&mut self) -> Result<()> {
        for i in 0..self.partition_num {
            let partition_file_name = format!("input-chunks-p{}", i);
            let w = self.op.writer_with(&partition_file_name).await?;
            self.input_writers.push(w);
            self.input_chunk_builders.push(DataChunkBuilder::new(
                self.child_data_types.clone(),
                self.spill_chunk_size,
            ));
        }
        Ok(())
    }

    async fn write_input_chunk(&mut self, chunk: DataChunk) -> Result<()> {
        for row in chunk.rows() {
            let partition = self.round_robin_idx;
            if let Some(chunk) = self.input_chunk_builders[partition].append_one_row(row) {
                let chunk_pb: PbDataChunk = chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.input_writers[partition].write(len_bytes).await?;
                self.input_writers[partition].write(buf).await?;
            }
            self.round_robin_idx = (self.round_robin_idx + 1) % self.partition_num;
        }
        Ok(())
    }

    async fn close_writers(&mut self) -> Result<()> {
        for partition in 0..self.partition_num {
            if let Some(output_chunk) = self.input_chunk_builders[partition].consume_all() {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.input_writers[partition].write(len_bytes).await?;
                self.input_writers[partition].write(buf).await?;
            }
        }

        for mut w in self.input_writers.drain(..) {
            w.close().await?;
        }
        Ok(())
    }

    async fn read_input_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        let r = self.op.reader_with(&input_partition_file_name).await?;
        Ok(SpillOp::read_stream(r, self.spill_metrics.clone()))
    }

    async fn estimate_partition_size(&self, partition: usize) -> Result<u64> {
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        let input_size = self
            .op
            .stat(&input_partition_file_name)
            .await?
            .content_length();
        Ok(input_size)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::*;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{Date, F32, Interval, Scalar, StructType, Time, Timestamp};
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

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
                Field::unnamed(DataType::List(Box::new(DataType::Int64))),
            ],
        };
        let mut struct_builder = StructArrayBuilder::with_type(
            0,
            DataType::Struct(StructType::unnamed(vec![
                DataType::Varchar,
                DataType::Float32,
            ])),
        );
        let mut list_builder =
            ListArrayBuilder::with_type(0, DataType::List(Box::new(DataType::Int64)));
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
        let mut list_builder =
            ListArrayBuilder::with_type(0, DataType::List(Box::new(DataType::Int64)));
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
}
