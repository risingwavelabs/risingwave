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

use futures::StreamExt;
use futures_async_stream::try_stream;
use num_traits::CheckedSub;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, StreamChunk, Vis};

use super::error::StreamExecutorError;
use super::{ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message};
use crate::common::InfallibleExpression;




pub struct WatermarkFilterExecutor {

}

impl WatermarkFilterExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: BoxedExecutor,
        info: ExecutorInfo,
        time_col_idx: usize,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            ctx,
            input,
            info,
            time_col_idx,
            window_slide,
            window_size,
            output_indices,
        }
    }
}

impl Executor for HopWindowExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl HopWindowExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        
    }
}

#[cfg(test)]
mod tests {
    
}
