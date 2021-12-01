use crate::stream_op::{Executor, Message, SimpleExecutor, StreamChunk};
use async_trait::async_trait;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::{HeapElem, OrderPair};
use std::cmp::{Ordering, Reverse};
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

type MinHeapElem = Reverse<HeapElem>;

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub struct TopNExecutor {
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// The ordering
    order_pairs: Arc<Vec<OrderPair>>,
    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,
    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// We are interested in which element is in the range of [offset, offset+limit).
    /// Here we use A BTreeSet to store all received elements, offset/limit elem refer
    /// to current offset/limit element.
    inner: BTreeSet<MinHeapElem>,
    offset_elem: Option<MinHeapElem>,
    limit_elem: Option<MinHeapElem>,
}

impl TopNExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Arc<Vec<OrderPair>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        Self {
            input,
            order_pairs,
            limit,
            offset,
            inner: BTreeSet::new(),
            offset_elem: None,
            limit_elem: None,
        }
    }
}

#[async_trait]
impl Executor for TopNExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}

impl SimpleExecutor for TopNExecutor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let chunk = chunk.compact()?;

        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };
        let data_chunk = Arc::new(data_chunk);

        let num_limit = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];

        for (row_idx, op) in (0..data_chunk.capacity()).zip(ops.iter()) {
            let elem = Reverse(HeapElem {
                // Here we assume the row_id is already included in order_pairs here.
                order_pairs: self.order_pairs.clone(),
                chunk: data_chunk.clone(),
                // used in olap. useless here
                chunk_idx: 0usize,
                elem_idx: row_idx,
                encoded_chunk: None,
            });

            match op {
                // All elements consumed are unique (either contains `row_id` or record unique
                // already), here we don't consider the condition `elem` equals to
                // `offset_elem` or `limit_elem`.
                Op::Insert | Op::UpdateInsert => {
                    self.inner.insert(elem.clone());

                    // after inserted `elem`, the whole elements in the `inner` map are in the range
                    // of `[0, offset)`, continue without doing anything.
                    if self.inner.len() <= self.offset {
                        continue;
                    }

                    // set the `offset_elem` when `inner` map grows to size `offset`.
                    if self.inner.len() - 1 == self.offset {
                        let offset_elem = self.inner.last().unwrap();
                        self.offset_elem = Some(offset_elem.clone());
                        new_ops.push(Op::Insert);
                        new_rows.push(offset_elem.0.clone());
                        continue;
                    }

                    // `elem` is in the range of `(offset+limit, ..)`,
                    // continue without doing anything.
                    if self.limit_elem.is_some() && elem > self.limit_elem.clone().unwrap() {
                        continue;
                    }

                    if self.inner.len() - 1 - self.offset >= num_limit {
                        // set the `limit_elem` when `inner` map size grows to or greater than
                        // `offset` + `limit`.
                        let limit_elem = match self.limit_elem.clone() {
                            None => self.inner.last().unwrap(),
                            Some(limit_elem) => self.inner.range(..limit_elem).last().unwrap(),
                        };
                        self.limit_elem = Some(limit_elem.clone());
                        if limit_elem.clone() == elem {
                            // `elem` inserted as `limit_elem`.
                            continue;
                        }
                        new_ops.push(Op::Delete);
                        new_rows.push(limit_elem.0.clone());
                    }

                    if elem < self.offset_elem.clone().unwrap() {
                        // `elem` is in the range of `[0, offset)`, update new `offset` elem into
                        // result set and set the `offset_elem`.
                        let new_offset =
                            self.inner.range(..self.offset_elem.clone().unwrap()).last();
                        self.offset_elem = new_offset.cloned();
                        new_ops.push(Op::Insert);
                        new_rows.push(new_offset.unwrap().0.clone());
                    } else {
                        // `elem` is in the range of `[offset, offset+limit)`, insert it into result
                        // set.
                        new_ops.push(Op::Insert);
                        new_rows.push(elem.0.clone());
                    }
                }

                Op::Delete | Op::UpdateDelete => {
                    if !self.inner.remove(&elem.clone()) {
                        continue;
                    }
                    // after removed `elem`, the elements in the `inner` map are in the range of
                    // `[0, offset)` and count less than `offset`, continue
                    // without doing anything.
                    if self.inner.len() < self.offset {
                        continue;
                    }

                    // after removed `elem`, the elements in the `inner` map are in the range of
                    // `[0, offset)` and count equal to `offset`, which means
                    // `offset_elem` not belong to the result set anymore.
                    if self.inner.len() == self.offset {
                        new_ops.push(Op::Delete);
                        new_rows.push(self.offset_elem.clone().unwrap().0.clone());
                        self.offset_elem = None;
                        continue;
                    }

                    // `elem` is in the range of `[offset+limit, ..)`, remove it and update
                    // `limit_equal` when equal to `limit_elem`, continue.
                    if self.limit_elem.is_some() {
                        match self.limit_elem.clone().unwrap().cmp(&elem) {
                            Ordering::Equal => {
                                self.limit_elem = self
                                    .inner
                                    .range(self.limit_elem.clone().unwrap()..)
                                    .next()
                                    .cloned();
                                continue;
                            }
                            Ordering::Less => continue,
                            _ => {}
                        };
                    }

                    if elem <= self.offset_elem.clone().unwrap() {
                        // `elem` is in the range of `[0, offset)`, delete old `offset` elem into
                        // result set and set the `offset_elem`.
                        new_rows.push(self.offset_elem.clone().unwrap().0.clone());
                        new_ops.push(Op::Delete);
                        self.offset_elem = self
                            .inner
                            .range((Excluded(self.offset_elem.clone().unwrap()), Unbounded))
                            .next()
                            .cloned();
                    } else {
                        // `elem` is in the range of `[offset, offset+limit)`, delete it from result
                        // set.
                        new_ops.push(Op::Delete);
                        new_rows.push(elem.0.clone());
                    }

                    if self.limit_elem.is_some() {
                        new_ops.push(Op::Insert);
                        new_rows.push(self.limit_elem.clone().unwrap().0.clone());
                        self.limit_elem = self
                            .inner
                            .range((Excluded(self.limit_elem.clone().unwrap()), Unbounded))
                            .next()
                            .cloned();
                    }
                }
            };
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

    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_op::test_utils::MockSource;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::Field;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::Int64Type;
    use risingwave_common::util::sort_util::OrderType;

    #[tokio::test]
    async fn test_top_n_executor() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert; 6],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 3, 10, 9, 8] }],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![
                Op::Insert,
                Op::Delete,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ],
            columns: vec![column_nonnull! { I64Array, Int64Type, [7, 3, 1, 5, 2, 11] }],
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
        let mut top_n_executor =
            TopNExecutor::new(source as Box<dyn Executor>, order_pairs, Some(3), 2);

        let res = top_n_executor.next().await.unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(3), Some(10), Some(9), Some(10), Some(8)];
            let expected_ops = vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete, Op::Insert];
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
            let expected_values = vec![
                Some(9),
                Some(7),
                Some(3),
                Some(9),
                Some(7),
                Some(10),
                Some(10),
                Some(7),
                Some(7),
                Some(10),
            ];
            let expected_ops = vec![
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
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
