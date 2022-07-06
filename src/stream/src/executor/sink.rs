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

use std::collections::HashMap;

use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::{Sink, SinkConfig, SinkImpl};
use risingwave_pb::common::worker_node::State;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{BoxedExecutor, Executor, Message};
use crate::task::ActorId;

pub struct SinkExecutor<S: StateStore> {
    input: BoxedExecutor,
    store: S,
    properties: HashMap<String, String>,
    identity: String,
}

async fn build_sink(config: SinkConfig) -> Box<SinkImpl> {
    todo!()
}

impl<S: StateStore> SinkExecutor<S> {
    pub fn new(
        materialize_executor: BoxedExecutor,
        store: S,
        properties: HashMap<String, String>,
        executor_id: u64,
    ) -> Self {
        Self {
            input: materialize_executor,
            store,
            properties: HashMap::new(),
            identity: format!("SinkExecutor_{:?}", executor_id),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let sink_config = SinkConfig::from_hashmap(self.properties.clone());
        let input = self.input.execute();
        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let visible_chunk = chunk.clone().compact()?;

                    yield Message::Chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl<S: StateStore> Executor for SinkExecutor<S> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        todo!();
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod test {

    use risingwave_connector::sink::mysql::{MySQLConfig, MySQLSink};

    use super::*;
    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[test]
    fn test_mysqlsink() {
        let cfg = MySQLConfig {
            endpoint: String::from("127.0.0.1:3306"),
            table: String::from("<table_name>"),
            database: Some(String::from("<database_name>")),
            user: Some(String::from("<user_name>")),
            password: Some(String::from("<password>")),
        };

        let mysql_sink = MySQLSink::new(cfg);

        // Mock `child`
        let mock = MockSource::with_messages(Schema::default(), PkIndices::new(), vec![]);

        // let _sink_executor = SinkExecutor::_new(Box::new(mock), mysql_sink);
    }
}
