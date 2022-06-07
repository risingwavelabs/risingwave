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
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::{MySQLSink, Sink};

use super::error::StreamExecutorError;
use super::{BoxedExecutor, Executor, Message};

pub struct SinkExecutor<S: Sink> {
    child: BoxedExecutor,
    external_sink: S,
    identity: String,
}

impl<S: Sink> SinkExecutor<S> {
    pub fn new(materialize_executor: BoxedExecutor, external_sink: S) -> Self {
        Self {
            child: materialize_executor,
            external_sink,
            identity: "SinkExecutor".to_string(),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let input = self.child.execute();
        #[for_await]
        for msg in input {
            let msg = msg?;

            self.external_sink.write_batch();
        }
    }
}

impl<S: Sink + 'static + Send> Executor for SinkExecutor<S> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.child.schema()
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        // &self.info.pk_indices
        todo!();
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Row, StreamChunk};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::cell_based_table::CellBasedTable;
    use risingwave_storage::Keyspace;

    use super::{SinkExecutor, *};
    use crate::executor::test_utils::*;
    use crate::executor::{Barrier, Message, PkIndices, *};
    use crate::task::LocalBarrierManager;

    // TODO: This basic test for the most part is a copy-paste from the `MaterializeExecutor`
    // fn test_materialize_executor(). Should be replaced by a better test eventually
    #[tokio::test]
    async fn test_basic() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::default()),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::default()),
            ],
        );

        let keyspace = Keyspace::table_root(memory_state_store.clone(), &table_id);
        let materialize_executor = Box::new(MaterializeExecutor::new(
            Box::new(source),
            keyspace,
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids,
            1,
            vec![0],
        ));

        let mysqlsink = MySQLSink {
            endpoint: String::from("127.0.0.1:3306"),
            table: String::from("<table_name>"),
            database: String::from("<database_name>"),
            user: String::from("<user_name>"),
            password: String::from("<password>"),
        };

        let sink = Box::new(SinkExecutor::new(materialize_executor, mysqlsink));

        sink.execute();
    }
}
