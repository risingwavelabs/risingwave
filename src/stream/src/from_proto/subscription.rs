// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::{TableId, TableOption};
use risingwave_pb::stream_plan::SubscriptionNode;
use risingwave_storage::store::{NewLocalOptions, OpConsistencyLevel};

use super::ExecutorBuilder;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V1_INFO;
use crate::common::log_store_impl::subscription_log_store::SubscriptionLogStoreWriter;
use crate::error::StreamResult;
use crate::executor::{BoxedExecutor, SubscriptionExecutor};

pub struct SubscriptionExecutorBuilder;

impl ExecutorBuilder for SubscriptionExecutorBuilder {
    type Node = SubscriptionNode;

    async fn new_boxed_executor(
        params: crate::task::ExecutorParams,
        node: &Self::Node,
        state_store: impl risingwave_storage::StateStore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let table_id = TableId::new(node.log_store_table.as_ref().unwrap().id);
        let local_state_store = state_store
            .new_local(NewLocalOptions {
                table_id: TableId {
                    table_id: node.log_store_table.as_ref().unwrap().id,
                },
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
            })
            .await;

        let vnodes = std::sync::Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for subscription"),
        );
        let serde = LogStoreRowSerde::new(
            node.log_store_table.as_ref().unwrap(),
            Some(vnodes.clone()),
            // TODO: Use V2 after pr #14599
            &KV_LOG_STORE_V1_INFO,
        );
        let log_store_identity = format!(
            "subscription[{}]-executor[{}]",
            node.subscription_catalog.as_ref().unwrap().id,
            params.executor_id
        );
        let log_store =
            SubscriptionLogStoreWriter::new(table_id, local_state_store, serde, log_store_identity);
        Ok(Box::new(
            SubscriptionExecutor::new(
                params.actor_context,
                params.info,
                input,
                log_store,
                node.subscription_catalog
                    .as_ref()
                    .unwrap()
                    .properties
                    .clone(),
            )
            .await?,
        ))
    }
}
