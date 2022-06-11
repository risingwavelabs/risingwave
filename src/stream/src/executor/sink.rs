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
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::Sink;

use super::error::StreamExecutorError;
use super::{BoxedExecutor, Executor, Message};

pub struct SinkExecutor<S: Sink> {
    child: BoxedExecutor,
    _external_sink: S,
    identity: String,
}

impl<S: Sink> SinkExecutor<S> {
    pub fn _new(materialize_executor: BoxedExecutor, _external_sink: S) -> Self {
        Self {
            child: materialize_executor,
            _external_sink,
            identity: "SinkExecutor".to_string(),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        todo!()
    }
}

impl<S: Sink + 'static + Send> Executor for SinkExecutor<S> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
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

    use risingwave_connector::sink::MySQLSink;

    use super::*;
    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[test]
    fn test_mysqlsink() {
        let mysql_sink = MySQLSink::new(
            String::from("127.0.0.1:3306"),
            String::from("<table_name>"),
            Some(String::from("<database_name>")),
            Some(String::from("<user_name>")),
            Some(String::from("<password>")),
        );

        // Mock `child`
        let mock = MockSource::with_messages(Schema::default(), PkIndices::new(), vec![]);

        let _sink_executor = SinkExecutor::_new(Box::new(mock), mysql_sink);
    }
}
