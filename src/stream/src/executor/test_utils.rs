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
use risingwave_common::catalog::{Schema, TableId};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;
use tokio::sync::mpsc;

use super::error::StreamExecutorError;
use super::{Barrier, Executor, Message, PkIndices, StreamChunk};

pub struct MockSource {
    schema: Schema,
    pk_indices: PkIndices,
    rx: mpsc::UnboundedReceiver<Message>,

    /// Whether to send a `Stop` barrier on stream finish.
    stop_on_finish: bool,
}

/// A wrapper around `Sender<Message>`.
pub struct MessageSender(mpsc::UnboundedSender<Message>);

impl MessageSender {
    #[allow(dead_code)]
    pub fn push_chunk(&mut self, chunk: StreamChunk) {
        self.0.send(Message::Chunk(chunk)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_barrier(&mut self, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_stop();
        }
        self.0.send(Message::Barrier(barrier)).unwrap();
    }
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
    pub fn channel(schema: Schema, pk_indices: PkIndices) -> (MessageSender, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        let source = Self {
            schema,
            pk_indices,
            rx,
            stop_on_finish: true,
        };
        (MessageSender(tx), source)
    }

    #[allow(dead_code)]
    pub fn with_messages(schema: Schema, pk_indices: PkIndices, msgs: Vec<Message>) -> Self {
        let (tx, source) = Self::channel(schema, pk_indices);
        for msg in msgs {
            tx.0.send(msg).unwrap();
        }
        source
    }

    pub fn with_chunks(schema: Schema, pk_indices: PkIndices, chunks: Vec<StreamChunk>) -> Self {
        let (tx, source) = Self::channel(schema, pk_indices);
        for chunk in chunks {
            tx.0.send(Message::Chunk(chunk)).unwrap();
        }
        source
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn stop_on_finish(self, stop_on_finish: bool) -> Self {
        Self {
            stop_on_finish,
            ..self
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        let mut epoch = 0;

        while let Some(msg) = self.rx.recv().await {
            epoch += 1;
            yield msg;
        }

        if self.stop_on_finish {
            yield Message::Barrier(Barrier::new_test_barrier(epoch).with_stop());
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

pub fn create_in_memory_keyspace() -> Keyspace<MemoryStateStore> {
    Keyspace::table_root(MemoryStateStore::new(), &TableId::from(0x2333))
}

/// `row_nonnull` builds a `Row` with concrete values.
/// TODO: add macro row!, which requires a new trait `ToScalarValue`.
#[macro_export]
macro_rules! row_nonnull {
    [$( $value:expr ),*] => {
        {
            use risingwave_common::types::Scalar;
            use risingwave_common::array::Row;
            Row(vec![$(Some($value.to_scalar_value()), )*])
        }
    };
}

/// Create a vector of memory keyspace with len `num_ks`.
pub fn create_in_memory_keyspace_agg(num_ks: usize) -> Vec<Keyspace<MemoryStateStore>> {
    let mut returned_vec = vec![];
    let mem_state = MemoryStateStore::new();
    for idx in 0..num_ks {
        returned_vec.push(Keyspace::table_root(
            mem_state.clone(),
            &TableId::new(idx as u32),
        ));
    }
    returned_vec
}

pub mod global_simple_agg {
    use itertools::Itertools;
    use risingwave_storage::{Keyspace, StateStore};

    use crate::executor::aggregation::{generate_agg_schema, generate_state_table, AggCall};
    use crate::executor::{BoxedExecutor, Executor, PkIndices, SimpleAggExecutor};

    pub fn new_boxed_simple_agg_executor(
        keyspace: Vec<Keyspace<impl StateStore>>,
        input: BoxedExecutor,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Box<dyn Executor> {
        let agg_schema = generate_agg_schema(input.as_ref(), &agg_calls, Some(&key_indices));
        let state_tables = keyspace
            .iter()
            .zip_eq(agg_calls.iter())
            .map(|(ks, agg_call)| {
                generate_state_table(
                    ks.clone(),
                    agg_call,
                    &key_indices,
                    &pk_indices,
                    &agg_schema,
                    input.as_ref(),
                )
            })
            .collect();

        Box::new(
            SimpleAggExecutor::new(
                input,
                agg_calls,
                pk_indices,
                executor_id,
                key_indices,
                state_tables,
            )
            .unwrap(),
        )
    }
}
