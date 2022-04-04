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

use std::collections::{HashSet, VecDeque};

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::error::TracedStreamExecutorError;
use super::{Barrier, Executor, Message, Mutation, PkIndices, StreamChunk};

pub struct MockSource {
    schema: Schema,
    pk_indices: PkIndices,
    msgs: VecDeque<Message>,

    /// Whether to send a `Stop` barrier on stream finish.
    stop_on_finish: bool,
}

impl std::fmt::Debug for MockSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockSource")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl MockSource {
    #[allow(dead_code)]
    pub fn new(schema: Schema, pk_indices: PkIndices) -> Self {
        Self {
            schema,
            pk_indices,
            msgs: VecDeque::default(),
            stop_on_finish: true,
        }
    }

    #[allow(dead_code)]
    pub fn with_messages(schema: Schema, pk_indices: PkIndices, msgs: Vec<Message>) -> Self {
        Self {
            schema,
            pk_indices,
            msgs: msgs.into(),
            stop_on_finish: true,
        }
    }

    pub fn with_chunks(schema: Schema, pk_indices: PkIndices, chunks: Vec<StreamChunk>) -> Self {
        Self {
            schema,
            pk_indices,
            msgs: chunks.into_iter().map(Message::Chunk).collect(),
            stop_on_finish: true,
        }
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn stop_on_finish(self, stop_on_finish: bool) -> Self {
        Self {
            stop_on_finish,
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn push_chunks(&mut self, chunks: impl Iterator<Item = StreamChunk>) {
        self.msgs.extend(chunks.map(Message::Chunk));
    }

    #[allow(dead_code)]
    pub fn push_barrier(&mut self, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_mutation(Mutation::Stop(HashSet::default()));
        }
        self.msgs.push_back(Message::Barrier(barrier));
    }
}

impl MockSource {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let mut epoch = 0;

        for msg in self.msgs {
            epoch += 1;
            yield msg
        }

        if self.stop_on_finish {
            yield Message::Barrier(
                Barrier::new_test_barrier(epoch).with_mutation(Mutation::Stop(HashSet::default())),
            );
        }
    }
}

impl Executor for MockSource {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "MockSource"
    }
}
