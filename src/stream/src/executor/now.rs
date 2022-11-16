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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    Barrier, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef, StreamExecutorError,
};
use crate::common::table::state_table::StateTable;

pub struct NowExecutor<S: StateStore> {
    /// Receiver of barrier channel.
    barrier_receiver: UnboundedReceiver<Barrier>,

    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    state_table: StateTable<S>,
}

impl<S: StateStore> NowExecutor<S> {
    pub fn new(
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> Self {
        let schema = Schema::new(vec![Field {
            data_type: DataType::Timestamp,
            name: String::from("now"),
            sub_fields: vec![],
            type_name: String::default(),
        }]);
        Self {
            barrier_receiver,
            pk_indices: vec![],
            identity: format!("NowExecutor {:X}", executor_id),
            schema,
            state_table,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(self) {
        #[allow(unused_variables)]
        let Self {
            barrier_receiver,
            state_table,
            schema,
            ..
        } = self;

        todo!("https://github.com/risingwavelabs/risingwave/pull/6408");
    }
}

impl<S: StateStore> Executor for NowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}
