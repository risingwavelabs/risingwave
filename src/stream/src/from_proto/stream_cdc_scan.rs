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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::external::{CdcTableType, SchemaTableName};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::stream_plan::StreamCdcScanNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::{CdcBackfillExecutor, ExternalStorageTable};

pub struct StreamCdcScanExecutorBuilder;

impl ExecutorBuilder for StreamCdcScanExecutorBuilder {
    type Node = StreamCdcScanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let table_desc: &ExternalTableDesc = node.get_cdc_table_desc()?;

        let table_schema: Schema = table_desc.columns.iter().map(Into::into).collect();
        assert_eq!(output_indices, (0..table_schema.len()).collect_vec());
        assert_eq!(table_schema.data_types(), params.info.schema.data_types());

        let properties: HashMap<String, String> = table_desc
            .connect_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let table_type = CdcTableType::from_properties(&properties);
        let table_reader = table_type
            .create_table_reader(properties.clone(), table_schema.clone())
            .await?;

        let table_pk_order_types = table_desc
            .pk
            .iter()
            .map(|desc| OrderType::from_protobuf(desc.get_order_type().unwrap()))
            .collect_vec();
        let table_pk_indices = table_desc
            .pk
            .iter()
            .map(|k| k.column_index as usize)
            .collect_vec();

        let schema_table_name = SchemaTableName::from_properties(&properties);
        let external_table = ExternalStorageTable::new(
            TableId::new(table_desc.table_id),
            schema_table_name,
            table_reader,
            table_schema,
            table_pk_order_types,
            table_pk_indices,
            output_indices.clone(),
        );

        let vnodes = params.vnode_bitmap.map(Arc::new);
        // cdc backfill should be singleton, so vnodes must be None.
        assert_eq!(None, vnodes);
        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, state_store, vnodes).await;

        // TODO(kwannoel): Should we apply flow control here as well?
        Ok(CdcBackfillExecutor::new(
            params.actor_context.clone(),
            params.info,
            external_table,
            upstream,
            output_indices,
            None,
            params.executor_stats,
            state_table,
            params.env.config().developer.chunk_size,
        )
        .boxed())
    }
}
