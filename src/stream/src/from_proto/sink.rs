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

use std::sync::Arc;

use risingwave_common::catalog::ColumnCatalog;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::{SinkParam, SinkWriterParam};
use risingwave_pb::stream_plan::{SinkLogStoreType, SinkNode};
use risingwave_storage::dispatch_state_store;

use super::*;
use crate::common::log_store::in_mem::BoundedInMemLogStoreFactory;
use crate::common::log_store::kv_log_store::KvLogStoreFactory;
use crate::executor::SinkExecutor;

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
        let db_name = sink_desc.get_db_name().into();
        let sink_from_name = sink_desc.get_sink_from_name().into();
        let properties = sink_desc.get_properties().clone();
        let downstream_pk = sink_desc
            .downstream_pk
            .iter()
            .map(|i| *i as usize)
            .collect_vec();
        let columns = sink_desc
            .column_catalogs
            .clone()
            .into_iter()
            .map(ColumnCatalog::from)
            .collect_vec();
        let sink_param = SinkParam {
            sink_id,
            properties,
            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk,
            sink_type,
            db_name,
            sink_from_name,
        };

        match node.log_store_type() {
            // Default value is the normal in memory log store to be backward compatible with the
            // previously unset value
            SinkLogStoreType::InMemoryLogStore | SinkLogStoreType::Unspecified => {
                let factory = BoundedInMemLogStoreFactory::new(1);
                Ok(Box::new(
                    SinkExecutor::new(
                        input_executor,
                        stream.streaming_metrics.clone(),
                        SinkWriterParam {
                            connector_params: params.env.connector_params(),
                            executor_id: params.executor_id,
                            vnode_bitmap: params.vnode_bitmap,
                            meta_client: params.env.meta_client(),
                        },
                        sink_param,
                        columns,
                        params.actor_context,
                        factory,
                        params.pk_indices,
                    )
                    .await?,
                ))
            }
            SinkLogStoreType::KvLogStore => {
                dispatch_state_store!(params.env.state_store(), state_store, {
                    let factory = KvLogStoreFactory::new(
                        state_store,
                        node.table.as_ref().unwrap().clone(),
                        params.vnode_bitmap.clone().map(Arc::new),
                        0,
                    );

                    Ok(Box::new(
                        SinkExecutor::new(
                            input_executor,
                            stream.streaming_metrics.clone(),
                            SinkWriterParam {
                                connector_params: params.env.connector_params(),
                                executor_id: params.executor_id,
                                vnode_bitmap: params.vnode_bitmap,
                                meta_client: params.env.meta_client(),
                            },
                            sink_param,
                            columns,
                            params.actor_context,
                            factory,
                            params.pk_indices,
                        )
                        .await?,
                    ))
                })
            }
        }
    }
}
