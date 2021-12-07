use async_trait::async_trait;

use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use std::cmp::Reverse;

use risingwave_common::util::sort_util::{HeapElem, OrderPair};

use std::sync::Arc;

use crate::stream_op::PKVec;
use crate::stream_op::{Executor, Message, SimpleExecutor, StreamChunk};
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::catalog::Schema;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use std::collections::BinaryHeap;

pub type HashKey = Vec<Datum>;

pub type LowerUpperHeaps = (BinaryHeap<Reverse<HeapElem>>, BinaryHeap<Reverse<HeapElem>>);

/// If the input contains only append, `AppendOnlyTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
pub struct AppendOnlyTopNExecutor {
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// The ordering
    order_pairs: Arc<Vec<OrderPair>>,
    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,
    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,
    /// The primary key indices of the `AppendOnlyTopNExecutor`
    pk_indices: PKVec,
    /// We are only interested in which element is in the range of `[offset, offset+limit)`(right
    /// open interval) but not the rank of such element
    ///
    /// We keep two heaps. One heap stores the elements in the range of `[0, offset)`, and
    /// another heap stores the elements in the range of `[offset, offset+limit)`.
    heaps: LowerUpperHeaps,
}

impl AppendOnlyTopNExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Arc<Vec<OrderPair>>,
        limit: Option<usize>,
        offset: usize,
        pk_indices: PKVec,
    ) -> Self {
        Self {
            input,
            order_pairs,
            limit,
            offset,
            heaps: (BinaryHeap::new(), BinaryHeap::new()),
            pk_indices,
        }
    }
}

#[async_trait]
impl Executor for AppendOnlyTopNExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }
}

impl SimpleExecutor for AppendOnlyTopNExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            // Ops is useless as we have assumed the input is append-only.
            ops: _ops,
            columns,
            visibility,
        } = chunk;

        let mut data_chunk: DataChunk = DataChunk::builder().columns(columns.to_vec()).build();
        if let Some(vis_map) = &visibility {
            data_chunk = data_chunk.with_visibility(vis_map.clone()).compact()?;
        }
        let data_chunk = Arc::new(data_chunk);

        let num_need_to_keep = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];
        let (lower_heap, upper_heap) = &mut self.heaps;

        for row_idx in 0..data_chunk.capacity() {
            // As we have already compacted the data chunk with visibility map,
            // we don't check visibility here anymore.
            // We don't compact ops as they are always "Insert"s.
            let elem = HeapElem {
                order_pairs: self.order_pairs.clone(),
                chunk: data_chunk.clone(),
                // used in olap. useless here
                chunk_idx: 0usize,
                elem_idx: row_idx,
                encoded_chunk: None,
            };

            if lower_heap.len() < self.offset {
                // `elem` is in the range of `[0, offset)`,
                // we ignored it for now as it is not in the result set.
                lower_heap.push(Reverse(elem));
                continue;
            }

            // If elem should be in `[0, offset)`, then the maximum element of `lower_heap`
            // has a chance to be in the `upper_heap`.
            // Otherwise, the current `elem` will be used.
            let elem_to_compare_with_upper = if *lower_heap.peek().unwrap() > Reverse(elem.clone())
            {
                let max_elem_from_lower = lower_heap.pop().unwrap().0;
                lower_heap.push(Reverse(elem));
                max_elem_from_lower
            } else {
                elem
            };

            if upper_heap.len() < num_need_to_keep {
                upper_heap.push(Reverse(elem_to_compare_with_upper.clone()));
                new_ops.push(Op::Insert);
                new_rows.push(elem_to_compare_with_upper);
            } else if *upper_heap.peek().unwrap() > Reverse(elem_to_compare_with_upper.clone()) {
                let row_to_pop = upper_heap.pop().unwrap().0;
                new_ops.push(Op::Delete);
                new_rows.push(row_to_pop);
                new_ops.push(Op::Insert);
                new_rows.push(elem_to_compare_with_upper.clone());
                upper_heap.push(Reverse(elem_to_compare_with_upper));
            }
        }

        if !new_rows.is_empty() {
            let mut data_chunk_builder =
                DataChunkBuilder::new_with_default_size(self.schema().data_types_clone());
            for elem in new_rows {
                let data_chunk = elem.chunk;
                let row_idx = elem.elem_idx;
                data_chunk_builder.append_one_row_ref(data_chunk.row_at(row_idx)?.0)?;
            }
            // since `new_rows` is not empty, we unwrap directly
            let new_data_chunk = data_chunk_builder.consume_all()?.unwrap();
            let new_stream_chunk =
                StreamChunk::new(new_ops, new_data_chunk.columns().to_vec(), None);
            Ok(Message::Chunk(new_stream_chunk))
        } else {
            Ok(Message::Chunk(StreamChunk::new(vec![], vec![], None)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_op::test_utils::MockSource;
    use crate::stream_op::top_n_appendonly::AppendOnlyTopNExecutor;
    use crate::stream_op::{Executor, Message, StreamChunk};
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::Int64Type;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_appendonly_top_n_executor() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert; 6],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 3, 10, 9, 8] }],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert; 4],
            columns: vec![column_nonnull! { I64Array, Int64Type, [7, 3, 1, 9] }],
            visibility: None,
        };
        let schema = Schema {
            fields: vec![Field {
                data_type: Int64Type::create(false),
            }],
        };
        let input_ref_1 = InputRefExpression::new(Int64Type::create(false), 0usize);
        let order_pairs = Arc::new(vec![OrderPair {
            order: Box::new(input_ref_1),
            order_type: OrderType::Ascending,
        }]);
        let source = Box::new(MockSource::with_chunks(schema, vec![chunk1, chunk2]));
        let mut top_n_executor = AppendOnlyTopNExecutor::new(
            source as Box<dyn Executor>,
            order_pairs,
            Some(4),
            3,
            vec![],
        );
        let res = top_n_executor.next().await.unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(10), Some(9), Some(8)];
            let expected_ops = vec![Op::Insert; 3];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops, expected_ops);
        }
        let res = top_n_executor.next().await.unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(10), Some(3), Some(9), Some(3)];
            let expected_ops = vec![Op::Insert, Op::Delete, Op::Insert, Op::Delete, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops, expected_ops);
        }
    }
}
