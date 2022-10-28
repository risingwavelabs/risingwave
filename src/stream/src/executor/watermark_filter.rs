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
use futures_async_stream::{try_stream, for_await};
use risingwave_common::bail;
use risingwave_common::types::Datum;
use risingwave_expr::expr::BoxedExpression;

use super::error::StreamExecutorError;
use super::filter::SimpleFilterExecutor;
use super::simple::SimpleExecutor;
use super::{ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message, watermark, StreamExecutorResult};
use crate::common::InfallibleExpression;

pub struct WatermarkFilterExecutor {
    input: BoxedExecutor,
    watermark_expr: BoxedExpression,
    init_watermark: Datum,
    event_time_col_idx: usize,
    ctx: ActorContextRef,
    info: ExecutorInfo,
}

impl WatermarkFilterExecutor {
    pub fn new(
        input: BoxedExecutor,
        watermark_expr: BoxedExpression,
        init_watermark: Datum,
        event_time_col_idx: usize,
        ctx: ActorContextRef,
        info: ExecutorInfo,
    ) -> Self {
        Self {
            input,
            watermark_expr,
            init_watermark,
            event_time_col_idx,
            info,
            ctx,
        }
    }
}

impl Executor for WatermarkFilterExecutor {
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

impl WatermarkFilterExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            input,
            event_time_col_idx,
            watermark_expr,
            init_watermark: current_watermark,
            ctx,
            info,
        } = *self;
        
        #[for_await]
        for msg in input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    // let (data_chunk, _) = chunk.into_parts();
                    let watermark_array = watermark_expr.eval_infallible(&chunk.data_chunk(), |err| {
                        ctx.on_compute_error(err, &info.identity)
                    });
                    
                    
                    SimpleFilterExecutor::filter(chunk, &watermark_expr, &ctx, &info);
                },
                Message::Watermark(watermark) => {
                    if watermark.col_idx != event_time_col_idx {
                        yield Message::Watermark(watermark)
                    } else {
                        bail!("WatermarkFilterExecutor should not receive a watermark on the event it is filtering.")
                    }
                },
                Message::Barrier(_) => {
                    yield msg;
                }
            
            }
        }
        
    }
}



#[cfg(test)]
mod tests {}
