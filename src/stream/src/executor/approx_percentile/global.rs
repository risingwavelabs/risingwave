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

use super::global_state::GlobalApproxPercentileState;
use crate::executor::prelude::*;

pub struct GlobalApproxPercentileExecutor<S: StateStore> {
    _ctx: ActorContextRef,
    pub input: Executor,
    pub quantile: f64,
    pub base: f64,
    pub chunk_size: usize,
    pub state: GlobalApproxPercentileState<S>,
}

impl<S: StateStore> GlobalApproxPercentileExecutor<S> {
    pub fn new(
        _ctx: ActorContextRef,
        input: Executor,
        quantile: f64,
        base: f64,
        chunk_size: usize,
        bucket_state_table: StateTable<S>,
        count_state_table: StateTable<S>,
    ) -> Self {
        let global_state =
            GlobalApproxPercentileState::new(quantile, base, bucket_state_table, count_state_table);
        Self {
            _ctx,
            input,
            quantile,
            base,
            chunk_size,
            state: global_state,
        }
    }

    /// TODO(kwannoel): Include cache later.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        // Initialize state
        let mut input_stream = self.input.execute();
        let first_barrier = expect_first_barrier(&mut input_stream).await?;
        let first_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);
        let mut state = self.state;
        state.init(first_epoch).await?;

        // Get row count state, and row_count.
        #[for_await]
        for message in input_stream {
            match message? {
                Message::Chunk(chunk) => {
                    state.apply_chunk(chunk)?;
                }
                Message::Barrier(barrier) => {
                    if let Some(output) = state.get_output() {
                        yield Message::Chunk(output);
                    }
                    state.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(_) => {}
            }
        }
    }
}

impl<S: StateStore> Execute for GlobalApproxPercentileExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
