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

use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::kafka::KAFKA_SINK;
use risingwave_connector::sink::{SinkConfig, DOWNSTREAM_SINK_KEY};
use risingwave_pb::stream_plan::SinkNode;

use super::*;
use crate::common::log_store::BoundedInMemLogStoreFactory;
use crate::executor::{SinkExecutor, StreamExecutorError};

pub struct SinkExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for SinkExecutorBuilder {
    type Node = SinkNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input_executor]: [_; 1] = params.input.try_into().unwrap();

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_type = SinkType::from_proto(sink_desc.get_sink_type().unwrap());
        let sink_id = sink_desc.get_id().into();
        let mut properties = sink_desc.get_properties().clone();
        let pk_indices = sink_desc
            .downstream_pk
            .iter()
            .map(|i| *i as usize)
            .collect_vec();
        let schema = sink_desc.columns.iter().map(Into::into).collect();
        // This field can be used to distinguish a specific actor in parallelism to prevent
        // transaction execution errors
        if let Some(connector) = properties.get(DOWNSTREAM_SINK_KEY) && connector == KAFKA_SINK {
            properties.insert(
                "identifier".to_string(),
                format!("sink-{:?}", params.executor_id),
            );
        }
        let config = SinkConfig::from_hashmap(properties).map_err(StreamExecutorError::from)?;

        // TODO: For sink executor with a state table, a kv log store should be created.
        let factory = BoundedInMemLogStoreFactory::new(1);

        Ok(Box::new(
            SinkExecutor::new(
                input_executor,
                stream.streaming_metrics.clone(),
                config,
                params.executor_id,
                params.env.connector_params(),
                schema,
                pk_indices,
                sink_type,
                sink_id,
                params.actor_context,
                factory,
            )
            .await?,
        ))
    }
}
