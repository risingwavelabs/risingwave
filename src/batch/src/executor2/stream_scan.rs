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

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};

use crate::executor2::{
    BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2, ExecutorBuilder,
};
// TODO: Remove this when Java frontend is completely removed.
pub struct StreamScanExecutor2 {}

impl BoxedExecutor2Builder for StreamScanExecutor2 {
    fn new_boxed_executor2(_: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        unreachable!()
    }
}
impl Executor2 for StreamScanExecutor2 {
    fn schema(&self) -> &Schema {
        unreachable!()
    }

    fn identity(&self) -> &str {
        unreachable!()
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        unreachable!()
    }
}

impl StreamScanExecutor2 {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let _ = self;
        unreachable!()
    }
}
