use std::cmp::min;

use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

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
    /// Identity string of the executor
    identity: String,
}

impl BoxedExecutorBuilder for LimitExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);

        let limit_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::Limit)?;

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
                identity: source.plan_node().get_identity().clone(),
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
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.returned == self.limit {
            return Ok(None);
        }
        while let Some(chunk) = self.child.next().await? {
            let cardinality = chunk.cardinality();
            if cardinality + self.skipped <= self.offset {
                self.skipped += cardinality;
                continue;
            }
            if self.skipped == self.offset && cardinality + self.returned <= self.limit {
                self.returned += cardinality;
                return Ok(Some(chunk));
            }
            let chunk = self.process_chunk(chunk)?;
            return Ok(Some(chunk));
        }
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::vec;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, BoolArray, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    async fn test_limit_all_visible(
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
            fields: vec![Field::unnamed(DataType::Int32)],
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
            identity: "LimitExecutor".to_string(),
        };
        let fields = &limit_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        let mut results = vec![];
        while let Some(chunk) = limit_executor.next().await.unwrap() {
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
        visible: Vec<bool>,
        returned: usize,
        skipped: usize,
        cur_row: usize,
    }

    impl MockLimitIter {
        fn new(tot_row: usize, limit: usize, offset: usize, visible: Vec<bool>) -> Self {
            assert_eq!(tot_row, visible.len());
            let mut cur_row = 0;
            while cur_row != tot_row && !visible[cur_row] {
                cur_row += 1;
            }
            Self {
                tot_row,
                limit,
                offset,
                visible,
                returned: 0,
                skipped: 0,
                cur_row,
            }
        }

        fn next_visible(&mut self) {
            self.cur_row += 1;
            while self.cur_row != self.tot_row && !self.visible[self.cur_row] {
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
                self.next_visible();
                if self.cur_row == self.tot_row {
                    return None;
                }
                self.skipped += 1;
            }
            let ret = self.cur_row;
            self.next_visible();
            self.returned += 1;
            Some(ret)
        }
    }

    async fn test_limit_with_visibility(
        row_num: usize,
        chunk_size: usize,
        limit: usize,
        offset: usize,
        visible: Vec<bool>,
    ) {
        assert_eq!(visible.len(), row_num);
        let col0 = create_column(
            (0..row_num)
                .into_iter()
                .map(|x| Some(x as i32))
                .collect_vec()
                .as_slice(),
        )
        .unwrap();

        let visible_array = BoolArray::from_slice(
            visible
                .clone()
                .into_iter()
                .map(Some)
                .collect_vec()
                .as_slice(),
        )
        .unwrap();

        let col1 = Column::new(Arc::new(visible_array.into()));
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Boolean),
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
            identity: "LimitExecutor".to_string(),
        };
        limit_executor.open().await.unwrap();

        let mut results = vec![];
        while let Some(chunk) = limit_executor.next().await.unwrap() {
            results.push(Arc::new(chunk.compact().unwrap()));
        }
        let chunks =
            DataChunk::rechunk(results.into_iter().collect_vec().as_slice(), row_num).unwrap();
        // let _for_debug = MockLimitIter::new(row_num, limit, offset, visible).collect_vec();

        if chunks.is_empty() {
            assert_eq!(
                MockLimitIter::new(row_num, limit, offset, visible).count(),
                0
            );
            return;
        }
        assert_eq!(chunks.len(), 1);
        let result = chunks.into_iter().next().unwrap();
        let col0 = result.column_at(0).unwrap();
        let col1 = result.column_at(1).unwrap();
        assert_eq!(
            MockLimitIter::new(row_num, limit, offset, visible.clone()).count(),
            result.cardinality()
        );
        MockLimitIter::new(row_num, limit, offset, visible)
            .zip_eq(0..result.cardinality())
            .for_each(|(expect, chunk_idx)| {
                assert_eq!(col1.array().as_bool().value_at(chunk_idx), Some(true));
                assert_eq!(
                    col0.array().as_int32().value_at(chunk_idx),
                    Some(expect as i32)
                );
            });

        limit_executor.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_limit_executor() {
        test_limit_all_visible(18, 18, 11, 0).await;
        test_limit_all_visible(18, 3, 9, 0).await;
        test_limit_all_visible(18, 3, 10, 0).await;
        test_limit_all_visible(18, 3, 11, 0).await;
    }

    #[tokio::test]
    async fn test_limit_executor_large() {
        test_limit_all_visible(1024, 1024, 512, 0).await;
        test_limit_all_visible(1024, 33, 512, 0).await;
        test_limit_all_visible(1024, 33, 515, 0).await;
    }

    #[tokio::test]
    async fn test_limit_executor_with_offset() {
        for limit in 9..12 {
            for offset in 3..6 {
                test_limit_all_visible(18, 3, limit, offset).await;
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
