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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::catalog::{SinkFormatDesc, SinkId, SinkType};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, SINK_TYPE_OPTION, SinkMetaClient, SinkParam};
use risingwave_pb::connector_service::coordinate_request::CoordinationRole;
use risingwave_pb::stream_plan::IcebergWithPkIndexWriterNode;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::iceberg_with_pk_index::{
    CoordinatorStreamHandleInit, IcebergWriterImpl, WriterExecutor,
};
use crate::executor::{Executor, StreamExecutorError};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct IcebergWithPkIndexWriterExecutorBuilder;

impl ExecutorBuilder for IcebergWithPkIndexWriterExecutorBuilder {
    type Node = IcebergWithPkIndexWriterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input_executor]: [_; 1] = params.input.try_into().unwrap();

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();
        let sink_name = sink_desc.get_name().to_owned();
        let db_name = sink_desc.get_db_name().into();
        let sink_from_name = sink_desc.get_sink_from_name().into();
        let properties = sink_desc.get_properties().clone();
        let secret_refs = sink_desc.get_secret_refs().clone();
        let downstream_pk = if sink_desc.downstream_pk.is_empty() {
            None
        } else {
            Some(
                (sink_desc.downstream_pk.iter())
                    .map(|idx| *idx as usize)
                    .collect_vec(),
            )
        };
        let columns = sink_desc
            .column_catalogs
            .clone()
            .into_iter()
            .map(ColumnCatalog::from)
            .collect_vec();

        let properties_with_secret =
            LocalSecretManager::global().fill_secrets(properties, secret_refs)?;

        let connector = properties_with_secret
            .get(CONNECTOR_TYPE_KEY)
            .cloned()
            .unwrap_or_default();

        let format_desc = match &sink_desc.format_desc {
            Some(f) => Some(
                f.clone()
                    .try_into()
                    .map_err(|e| StreamExecutorError::from((e, sink_id)))?,
            ),
            None => match properties_with_secret.get(SINK_TYPE_OPTION) {
                Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)
                    .map_err(|e| StreamExecutorError::from((e, sink_id)))?,
                None => None,
            },
        };

        let format_desc = SinkParam::fill_secret_for_format_desc(format_desc)
            .map_err(|e| StreamExecutorError::from((e, sink_id)))?;

        let sink_type = SinkType::from_proto(sink_desc.get_sink_type().unwrap());
        let ignore_delete = sink_desc.ignore_delete();

        let sink_param = SinkParam {
            sink_id,
            sink_name,
            properties: properties_with_secret.clone(),
            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: downstream_pk.clone(),
            sink_type,
            ignore_delete,
            format_desc,
            db_name,
            sink_from_name,
        };

        // Build the PK index state table.
        let pk_index_table = node.pk_index_table.as_ref().unwrap();
        let vnodes = params.vnode_bitmap.clone().map(Arc::new);
        let pk_index_state_table = StateTableBuilder::new(pk_index_table, store, vnodes)
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;

        // Build IcebergConfig from properties.
        let iceberg_config = IcebergConfig::from_btreemap(properties_with_secret)
            .map_err(|e| StreamExecutorError::from((e, sink_id)))?;

        // Load the Iceberg table after the state table is registered, so the writer actor
        // does not miss the initial barrier while waiting on remote Iceberg metadata.
        let table = iceberg_config
            .load_table()
            .await
            .map_err(|e| StreamExecutorError::from((e, sink_id)))?;

        // Build the real Iceberg writer.
        let writer =
            IcebergWriterImpl::build(&iceberg_config, &table, params.actor_context.id, sink_id)?;

        // Extract pk indices from the sink desc's downstream_pk.
        let pk_indices: Vec<usize> = downstream_pk.unwrap_or_default();

        // Build the coordinator stream handle for committing data file metadata.
        let meta_client = params
            .env
            .meta_client()
            .ok_or_else(|| anyhow!("meta client is required for Iceberg writer"))?;
        let meta_client = SinkMetaClient::MetaClient(meta_client);
        let coordination_client = meta_client.sink_coordinate_client().await;
        let vnode_bitmap = params
            .vnode_bitmap
            .clone()
            .ok_or_else(|| anyhow!("writer executor should have vnode bitmap"))?;
        let coordinator_handle_init = CoordinatorStreamHandleInit {
            coordination_client,
            sink_param: sink_param.clone(),
            vnode_bitmap,
            role: CoordinationRole::Unspecified,
        };

        let exec = WriterExecutor::new(
            params.actor_context.clone(),
            input_executor,
            pk_indices,
            pk_index_state_table,
            writer,
            Some(coordinator_handle_init),
        );

        Ok((params.info, exec).into())
    }
}
