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

use std::cmp::min;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Limit executor.
pub struct LimitExecutor {
    child: BoxedExecutor,
    /// limit parameter
    limit: usize,
    /// offset parameter
    offset: usize,
    /// Identity string of the executor
    identity: String,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for LimitExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let limit_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::Limit)?;

        let limit = limit_node.get_limit() as usize;
        let offset = limit_node.get_offset() as usize;

        Ok(Box::new(Self::new(
            child,
            limit,
            offset,
            source.plan_node().get_identity().clone(),
        )))
    }
}

impl LimitExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        // the number of rows have been skipped due to offset
        let mut skipped = 0;
        // the number of rows have been returned as execute result
        let mut returned = 0;

        #[for_await]
        for data_chunk in self.child.execute() {
            if returned == self.limit {
                break;
            }
            let data_chunk = data_chunk?;
            let cardinality = data_chunk.cardinality();
            if cardinality + skipped <= self.offset {
                skipped += cardinality;
                continue;
            }

            if skipped == self.offset && cardinality + returned <= self.limit {
                returned += cardinality;
                yield data_chunk;
                continue;
            }
            // process chunk
            let mut new_vis;
            if let Some(old_vis) = data_chunk.visibility() {
                new_vis = old_vis.iter().collect_vec();
                for vis in new_vis.iter_mut().filter(|x| **x) {
                    if skipped < self.offset {
                        skipped += 1;
                        *vis = false;
                    } else if returned < self.limit {
                        returned += 1;
                    } else {
                        *vis = false;
                    }
                }
            } else {
                let chunk_size = data_chunk.capacity();
                new_vis = vec![false; chunk_size];
                let l = self.offset - skipped;
                let r = min(l + self.limit - returned, chunk_size);
                new_vis[l..r].fill(true);
                returned += r - l;
                skipped += l;
            }
            yield data_chunk
                .with_visibility(new_vis.into_iter().collect())
                .compact();
        }
    }
}

impl Executor for LimitExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl LimitExecutor {
    pub fn new(child: BoxedExecutor, limit: usize, offset: usize, identity: String) -> Self {
        Self {
            child,
            limit,
            offset,
            identity,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::vec;

    use futures_async_stream::for_await;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, BoolArray, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_column(vec: &[Option<i32>]) -> Column {
        PrimitiveArray::from_slice(vec).into()
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
        );
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let mut mock_executor = MockExecutor::new(schema);

        let data_chunk = DataChunk::new([col].to_vec(), row_num);

        DataChunk::rechunk(&[data_chunk], chunk_size)
            .unwrap()
            .into_iter()
            .for_each(|x| mock_executor.add(x));
        let limit_executor = Box::new(LimitExecutor {
            child: Box::new(mock_executor),
            limit,
            offset,
            identity: "LimitExecutor2".to_string(),
        });
        let fields = &limit_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        let mut results = vec![];
        let stream = limit_executor.execute();
        #[for_await]
        for chunk in stream {
            let chunk = chunk.unwrap();
            results.push(chunk);
        }
        let chunks =
            DataChunk::rechunk(results.into_iter().collect_vec().as_slice(), row_num).unwrap();
        assert_eq!(chunks.len(), 1);
        let result = chunks.into_iter().next().unwrap();
        let col = result.column_at(0);
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
        );

        let visible_array = BoolArray::from_slice(
            visible
                .clone()
                .into_iter()
                .map(Some)
                .collect_vec()
                .as_slice(),
        );

        let col1 = visible_array.into();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Boolean),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);

        let data_chunk = DataChunk::new([col0, col1].to_vec(), row_num);

        DataChunk::rechunk(&[data_chunk], chunk_size)
            .unwrap()
            .into_iter()
            .for_each(|x| {
                mock_executor
                    .add(x.with_visibility((x.column_at(1).array_ref().as_bool()).iter().collect()))
            });

        let limit_executor = Box::new(LimitExecutor {
            child: Box::new(mock_executor),
            limit,
            offset,
            identity: "LimitExecutor2".to_string(),
        });

        let mut results = vec![];
        let stream = limit_executor.execute();
        #[for_await]
        for chunk in stream {
            results.push(chunk.unwrap().compact());
        }
        let chunks =
            DataChunk::rechunk(results.into_iter().collect_vec().as_slice(), row_num).unwrap();

        if chunks.is_empty() {
            assert_eq!(
                MockLimitIter::new(row_num, limit, offset, visible).count(),
                0
            );
            return;
        }
        assert_eq!(chunks.len(), 1);
        let result = chunks.into_iter().next().unwrap();
        let col0 = result.column_at(0);
        let col1 = result.column_at(1);
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
