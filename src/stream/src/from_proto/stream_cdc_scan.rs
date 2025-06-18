// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::cdc::external::{
    CdcTableType, ExternalTableConfig, SchemaTableName,
};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::stream_plan::StreamCdcScanNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::{CdcBackfillExecutor, CdcScanOptions, ExternalStorageTable};

pub struct StreamCdcScanExecutorBuilder;

impl ExecutorBuilder for StreamCdcScanExecutorBuilder {
    type Node = StreamCdcScanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let table_desc: &ExternalTableDesc = node.get_cdc_table_desc()?;

        let output_schema: Schema = table_desc.columns.iter().map(Into::into).collect();
        assert_eq!(output_indices, (0..output_schema.len()).collect_vec());
        assert_eq!(output_schema.data_types(), params.info.schema.data_types());

        let properties = table_desc.connect_properties.clone();

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

        let scan_options = node
            .options
            .as_ref()
            .map(CdcScanOptions::from_proto)
            .unwrap_or(CdcScanOptions {
                disable_backfill: node.disable_backfill,
                ..Default::default()
            });
        let table_type = CdcTableType::from_properties(&properties);

        // Filter out additional columns to construct the external table schema
        let table_schema: Schema = table_desc
            .columns
            .iter()
            .filter(|col| {
                col.additional_column
                    .as_ref()
                    .is_none_or(|a_col| a_col.column_type.is_none())
            })
            .map(Into::into)
            .collect();

        let schema_table_name = SchemaTableName::from_properties(&properties);
        let table_config = ExternalTableConfig::try_from_btreemap(
            properties.clone(),
            table_desc.secret_refs.clone(),
        )
        .context("failed to parse external table config")?;

        let database_name = table_config.database.clone();

        let external_table = ExternalStorageTable::new(
            TableId::new(table_desc.table_id),
            schema_table_name,
            database_name,
            table_config,
            table_type,
            table_schema,
            table_pk_order_types,
            table_pk_indices,
        );

        let vnodes = params.vnode_bitmap.map(Arc::new);
        // cdc backfill should be singleton, so vnodes must be None.
        assert_eq!(None, vnodes);
        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, state_store, vnodes).await;

        let output_columns = table_desc.columns.iter().map(Into::into).collect_vec();
        let exec = CdcBackfillExecutor::new(
            params.actor_context.clone(),
            external_table,
            upstream,
            output_indices,
            output_columns,
            None,
            params.executor_stats,
            state_table,
            node.rate_limit,
            scan_options,
            node.is_for_etl,
        );
        Ok((params.info, exec).into())
    }
}
