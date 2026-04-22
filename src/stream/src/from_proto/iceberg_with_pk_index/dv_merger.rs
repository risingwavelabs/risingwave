// Copyright 2026 RisingWave Labs
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

use anyhow::anyhow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::SinkMetaClient;
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_pb::connector_service::coordinate_request::CoordinationRole;
use risingwave_pb::id::SinkId;
use risingwave_pb::stream_plan::IcebergWithPkIndexDvMergerNode;
use risingwave_storage::StateStore;

use super::super::sink::build_sink_param;
use crate::error::StreamResult;
use crate::executor::{
    CoordinatorStreamHandleInit, DvHandlerImpl, DvMergerExecutor, Executor, StreamExecutorError,
};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct IcebergWithPkIndexDvMergerExecutorBuilder;

impl ExecutorBuilder for IcebergWithPkIndexDvMergerExecutorBuilder {
    type Node = IcebergWithPkIndexDvMergerNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();

        let properties_with_secret = LocalSecretManager::global().fill_secrets(
            sink_desc.get_properties().clone(),
            sink_desc.get_secret_refs().clone(),
        )?;
        let config = IcebergConfig::from_btreemap(properties_with_secret.clone())
            .map_err(|err| StreamExecutorError::from((err, sink_id)))?;
        let handler = DvHandlerImpl::new(config, params.actor_context.id, sink_id).await?;

        let (sink_param, _columns) =
            build_sink_param(sink_desc, properties_with_secret, ICEBERG_SINK)?;

        let meta_client = params
            .env
            .meta_client()
            .ok_or_else(|| anyhow!("meta client is required for Iceberg DV merger"))?;
        let coordination_client = SinkMetaClient::MetaClient(meta_client)
            .sink_coordinate_client()
            .await;
        let vnode_bitmap = params
            .vnode_bitmap
            .clone()
            .ok_or_else(|| anyhow!("Iceberg DV merger executor should have a vnode bitmap"))?;
        let coordinator_handle_init = CoordinatorStreamHandleInit {
            coordination_client,
            sink_param,
            vnode_bitmap,
            role: CoordinationRole::DvMerger,
        };

        let exec = DvMergerExecutor::new(
            params.actor_context,
            input,
            handler,
            Some(coordinator_handle_init),
        );
        Ok((params.info, exec).into())
    }
}
