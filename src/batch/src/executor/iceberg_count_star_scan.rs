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

use std::mem;
use std::sync::Arc;

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::array::I64Array;
use risingwave_common::catalog::Schema;

use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};

pub struct IcebergCountStarExecutor {
    schema: Schema,
    identity: String,
    record_counts: Vec<u64>,
}

impl Executor for IcebergCountStarExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergCountStarExecutor {
    pub fn new(schema: Schema, identity: String, record_counts: Vec<u64>) -> Self {
        Self {
            schema,
            identity,
            record_counts,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let record_count = mem::take(&mut self.record_counts).into_iter().sum::<u64>() as i64;
        let chunk = DataChunk::new(
            vec![Arc::new(I64Array::from_iter([record_count]).into())],
            1,
        );
        yield chunk;
    }
}
