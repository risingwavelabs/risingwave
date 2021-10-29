use std::cmp::min;

use itertools::Itertools;
use protobuf::Message;

use risingwave_proto::plan::{LimitNode as LimitProto, PlanNode_PlanNodeType};

use crate::array::DataChunk;
use crate::catalog::Schema;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::executor::ExecutorResult::{Batch, Done};

use super::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult};

/// Limit executor.
pub(super) struct LimitExecutor {
    child: BoxedExecutor,
    /// limit parameter
    limit: usize,
    /// offset parameter
    offset: usize,
    /// the number of rows have been skipped due to offset
    skipped: usize,
    /// the number of rows have been returned as execute result
    returned: usize,
}

impl BoxedExecutorBuilder for LimitExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::LIMIT);
        ensure!(source.plan_node().get_children().len() == 1);
        let limit_node = LimitProto::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let limit = limit_node.get_limit() as usize;
        let offset = limit_node.get_offset() as usize;

        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build()?;
            return Ok(Box::new(Self {
                child,
                limit,
                offset,
                skipped: 0,
                returned: 0,
            }));
        }
        Err(InternalError("Limit must have one child".to_string()).into())
    }
}
impl LimitExecutor {
    fn process_chunk(&mut self, chunk: DataChunk) -> Result<DataChunk> {
        let mut new_vis;
        if let Some(old_vis) = chunk.visibility() {
            new_vis = old_vis.iter().collect_vec();
            for vis in new_vis.iter_mut().filter(|x| **x) {
                if self.skipped < self.offset {
                    self.skipped += 1;
                    *vis = false;
                } else if self.returned < self.limit {
                    self.returned += 1;
                } else {
                    *vis = false;
                }
            }
        } else {
            let chunk_size = chunk.capacity();
            new_vis = vec![false; chunk_size];
            let l = self.offset - self.skipped;
            let r = min(l + self.limit - self.returned, chunk_size);
            new_vis[l..r].fill(true);
            self.returned += r - l;
            self.skipped += l;
        }
        let chunk = chunk.with_visibility(new_vis.try_into()?).compact()?;
        Ok(chunk)
    }
}

#[async_trait::async_trait]
impl Executor for LimitExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.returned == self.limit {
            return Ok(Done);
        }
        while let Batch(chunk) = self.child.execute().await? {
            let cardinality = chunk.cardinality();
            if cardinality + self.skipped <= self.offset {
                self.skipped += cardinality;
                continue;
            }
            if self.skipped == self.offset && cardinality + self.returned <= self.limit {
                self.returned += cardinality;
                return Ok(Batch(chunk));
            }
            let chunk = self.process_chunk(chunk)?;
            return Ok(Batch(chunk));
        }
        Ok(Done)
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::vec;

    use crate::array::column::Column;
    use crate::array::{Array, BoolArray, DataChunk, PrimitiveArray};
    use crate::catalog::{Field, Schema};
    use crate::executor::test_utils::MockExecutor;
    use crate::types::{BoolType, DataTypeKind, Int32Type};

    use super::*;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Arc::new(Int32Type::new(false));
        Ok(Column::new(array, data_type))
    }

    async fn test_limit_all_visable(
        row_num: usize,
        chunk_size: usize,
        limit: usize,
        offset: usize,
    ) {
        let col = create_column(
            (0..row_num)
                .into_iter()
                .map(|x| Some(x as i32))
                .collect_vec()
                .as_slice(),
        )
        .unwrap();
        let schema = Schema {
            fields: vec![Field {
                data_type: Int32Type::create(false),
            }],
        };
        let mut mock_executor = MockExecutor::new(schema);

        let data_chunk = DataChunk::builder().columns([col].to_vec()).build();

        DataChunk::rechunk(&[Arc::new(data_chunk)], chunk_size)
            .unwrap()
            .into_iter()
            .for_each(|x| mock_executor.add(x));
        let mut limit_executor = LimitExecutor {
            child: Box::new(mock_executor),
            limit,
            offset,
            skipped: 0,
            returned: 0,
        };
        let fields = &limit_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        let mut results = vec![];
        while let Batch(chunk) = limit_executor.execute().await.unwrap() {
            results.push(Arc::new(chunk));
        }
        let chunks =
            DataChunk::rechunk(results.into_iter().collect_vec().as_slice(), row_num).unwrap();
        assert_eq!(chunks.len(), 1);
        let result = chunks.into_iter().next().unwrap();
        let col = result.column_at(0).unwrap();
        assert_eq!(result.cardinality(), min(limit, row_num - offset));
        for i in 0..result.cardinality() {
            assert_eq!(
                col.array().as_int32().value_at(i),
                Some((offset + i) as i32)
            );
        }
    }

    pub(crate) struct MockLimitIter {
        tot_row: usize,
        limit: usize,
        offset: usize,
        visable: Vec<bool>,
        returned: usize,
        skipped: usize,
        cur_row: usize,
    }

    impl MockLimitIter {
        fn new(tot_row: usize, limit: usize, offset: usize, visable: Vec<bool>) -> Self {
            assert_eq!(tot_row, visable.len());
            let mut cur_row = 0;
            while cur_row != tot_row && !visable[cur_row] {
                cur_row += 1;
            }
            Self {
                tot_row,
                limit,
                offset,
                visable,
                returned: 0,
                skipped: 0,
                cur_row,
            }
        }

        fn next_visable(&mut self) {
            self.cur_row += 1;
            while self.cur_row != self.tot_row && !self.visable[self.cur_row] {
                self.cur_row += 1;
            }
        }
    }

    impl Iterator for MockLimitIter {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            if self.cur_row == self.tot_row {
                return None;
            }
            if self.returned == self.limit {
                return None;
            }
            while self.skipped < self.offset {
                self.next_visable();
                if self.cur_row == self.tot_row {
                    return None;
                }
                self.skipped += 1;
            }
            let ret = self.cur_row;
            self.next_visable();
            self.returned += 1;
            Some(ret)
        }
    }

    async fn test_limit_with_visibility(
        row_num: usize,
        chunk_size: usize,
        limit: usize,
        offset: usize,
        visable: Vec<bool>,
    ) {
        assert_eq!(visable.len(), row_num);
        let col0 = create_column(
            (0..row_num)
                .into_iter()
                .map(|x| Some(x as i32))
                .collect_vec()
                .as_slice(),
        )
        .unwrap();

        let visable_array = BoolArray::from_slice(
            visable
                .clone()
                .into_iter()
                .map(Some)
                .collect_vec()
                .as_slice(),
        )
        .unwrap();

        let col1 = Column::new(
            Arc::new(visable_array.into()),
            Arc::new(BoolType::new(false)),
        );
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: BoolType::create(false),
                },
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);

        let data_chunk = DataChunk::builder().columns([col0, col1].to_vec()).build();

        DataChunk::rechunk(&[Arc::new(data_chunk)], chunk_size)
            .unwrap()
            .into_iter()
            .for_each(|x| {
                mock_executor.add(
                    x.with_visibility(
                        (x.column_at(1).unwrap().array_ref().as_bool())
                            .try_into()
                            .unwrap(),
                    ),
                )
            });

        let mut limit_executor = LimitExecutor {
            child: Box::new(mock_executor),
            limit,
            offset,
            skipped: 0,
            returned: 0,
        };
        let mut results = vec![];
        while let Batch(chunk) = limit_executor.execute().await.unwrap() {
            results.push(Arc::new(chunk.compact().unwrap()));
        }
        let chunks =
            DataChunk::rechunk(results.into_iter().collect_vec().as_slice(), row_num).unwrap();
        // let _for_debug = MockLimitIter::new(row_num, limit, offset, visable).collect_vec();

        if chunks.is_empty() {
            assert_eq!(
                MockLimitIter::new(row_num, limit, offset, visable).count(),
                0
            );
            return;
        }
        assert_eq!(chunks.len(), 1);
        let result = chunks.into_iter().next().unwrap();
        let col0 = result.column_at(0).unwrap();
        let col1 = result.column_at(1).unwrap();
        assert_eq!(
            MockLimitIter::new(row_num, limit, offset, visable.clone()).count(),
            result.cardinality()
        );
        MockLimitIter::new(row_num, limit, offset, visable)
            .zip_eq(0..result.cardinality())
            .for_each(|(expect, chunk_idx)| {
                assert_eq!(col1.array().as_bool().value_at(chunk_idx), Some(true));
                assert_eq!(
                    col0.array().as_int32().value_at(chunk_idx),
                    Some(expect as i32)
                );
            })
    }

    #[tokio::test]
    async fn test_limit_executor() {
        test_limit_all_visable(18, 18, 11, 0).await;
        test_limit_all_visable(18, 3, 9, 0).await;
        test_limit_all_visable(18, 3, 10, 0).await;
        test_limit_all_visable(18, 3, 11, 0).await;
    }

    #[tokio::test]
    async fn test_limit_executor_large() {
        test_limit_all_visable(1024, 1024, 512, 0).await;
        test_limit_all_visable(1024, 33, 512, 0).await;
        test_limit_all_visable(1024, 33, 515, 0).await;
    }

    #[tokio::test]
    async fn test_limit_executor_with_offset() {
        for limit in 9..12 {
            for offset in 3..6 {
                test_limit_all_visable(18, 3, limit, offset).await;
            }
        }
    }

    #[tokio::test]
    async fn test_limit_executor_with_visibility() {
        let tot_row = 6;
        for mask in 0..(1 << tot_row) {
            let mut visibility = vec![];
            for i in 0..tot_row {
                visibility.push((mask >> i) & 1 == 1);
            }
            test_limit_with_visibility(tot_row, 2, 2, 2, visibility).await;
        }
    }
}
