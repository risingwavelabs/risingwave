// Copyright 2023 RisingWave Labs
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

use crate::executor::prelude::*;

/// No-op executor directly forwards the input stream. Currently used to break the multiple edges in
/// the fragment graph.
pub struct NoOpExecutor {
    _ctx: ActorContextRef,
    input: Executor,
}

impl NoOpExecutor {
    pub fn new(ctx: ActorContextRef, input: Executor) -> Self {
        Self { _ctx: ctx, input }
    }
}

impl Execute for NoOpExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.input.execute()
    }
}
