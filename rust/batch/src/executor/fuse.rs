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

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;

use crate::executor::Executor;

/// [`FusedExecutor`] is a wrapper around a Executor. After wrapping, once a call to
/// `next` returns `Ok(None)`, all subsequent calls to `next` will return an
/// error.
pub struct FusedExecutor<T: Executor> {
    /// The underlying executor.
    inner: T,
    /// Whether the underlying executor should return `Err` or not.
    invalid: bool,
}

impl<T: Executor> FusedExecutor<T> {
    pub fn new(executor: T) -> FusedExecutor<T> {
        FusedExecutor {
            inner: executor,
            invalid: false,
        }
    }
}

#[async_trait::async_trait]
impl<T: Executor> Executor for FusedExecutor<T> {
    async fn open(&mut self) -> Result<()> {
        self.inner.open().await
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.invalid {
            // The executor is invalid now, so we simply return an error.
            return Err(InternalError("Polling an already finished executor".to_string()).into());
        }
        match self.inner.next().await? {
            res @ Some(_) => Ok(res),
            None => {
                // Once the underlying executor returns `Ok(None)`,
                // subsequence calls will return `Err`.
                self.invalid = true;
                Ok(None)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use assert_matches::assert_matches;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn test_fused_executor() {
        let col1 = create_column(&[Some(2), Some(2)]).unwrap();
        let col2 = create_column(&[Some(1), Some(2)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col1, col2].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::with_chunk(data_chunk, schema).fuse();
        let fields = &mock_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        mock_executor.open().await.unwrap();
        let res = mock_executor.next().await.unwrap();
        assert_matches!(res, Some(_));
        if let Some(res) = res {
            let col1 = res.column_at(0);
            let array = col1.array();
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 2);
            assert_eq!(col1.value_at(0), Some(2));
            assert_eq!(col1.value_at(1), Some(2));
            let col2 = res.column_at(1);
            let array = col2.array();
            let col2 = array.as_int32();
            assert_eq!(col2.len(), 2);
            assert_eq!(col2.value_at(0), Some(1));
            assert_eq!(col2.value_at(1), Some(2));
        }
        let res = mock_executor.next().await.unwrap();
        assert_matches!(res, None);
        let res = mock_executor.next().await;
        assert_matches!(res, Err(_));
        mock_executor.close().await.unwrap();
    }

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }
}
