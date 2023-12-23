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

use risingwave_common::catalog::{default_key_column_name_version_mapping, ColumnId, TableId};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::SourceCtrlOpts;
use risingwave_pb::data::data_type::TypeName as PbTypeName;
use risingwave_pb::plan_common::additional_column::ColumnType;
use risingwave_pb::plan_common::{
    AdditionalColumn, AdditionalColumnKey, ColumnDescVersion, FormatType, PbEncodeType,
};
use risingwave_pb::stream_plan::SourceBackfillNode;

use super::*;
use crate::executor::kafka_backfill_executor::{
    KafkaBackfillExecutor, KafkaBackfillExecutorWrapper,
};
use crate::executor::source::StreamSourceCore;
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::BackfillStateTableHandler;

pub struct KafkaBackfillExecutorBuilder;

impl ExecutorBuilder for KafkaBackfillExecutorBuilder {
    type Node = SourceBackfillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        // let (sender, barrier_receiver) = unbounded_channel();
        // stream
        //     .context
        //     .barrier_manager()
        //     .register_sender(params.actor_context.id, sender);
        let system_params = params.env.system_params_manager_ref().get_params();

        let source_id = TableId::new(node.source_id);
        let source_name = node.source_name.clone();
        let source_info = node.get_info()?;

        let mut source_columns = node.columns.clone();

        {
            // compatible code: introduced in https://github.com/risingwavelabs/risingwave/pull/13707
            // for upsert and (avro | protobuf) overwrite the `_rw_key` column's ColumnDesc.additional_column_type to Key
            if source_info.format() == FormatType::Upsert
                && (source_info.row_encode() == PbEncodeType::Avro
                    || source_info.row_encode() == PbEncodeType::Protobuf)
            {
                let _ = source_columns.iter_mut().map(|c| {
                    let _ = c.column_desc.as_mut().map(|desc| {
                        let is_bytea = desc
                            .get_column_type()
                            .map(|col_type| col_type.type_name == PbTypeName::Bytea as i32)
                            .unwrap();
                        if desc.name == default_key_column_name_version_mapping(
                            &desc.version()
                        )
                            && is_bytea
                            // the column is from a legacy version
                            && desc.version == ColumnDescVersion::Unspecified as i32
                        {
                            desc.additional_column = Some(AdditionalColumn {
                                column_type: Some(ColumnType::Key(AdditionalColumnKey {})),
                            });
                        }
                    });
                });
            }
        }
        let source_desc_builder = SourceDescBuilder::new(
            source_columns.clone(),
            params.env.source_metrics(),
            node.row_id_index.map(|x| x as _),
            node.with_properties.clone(),
            source_info.clone(),
            params.env.connector_params(),
            params.env.config().developer.connector_message_buffer_size,
            // `pk_indices` is used to ensure that a message will be skipped instead of parsed
            // with null pk when the pk column is missing.
            //
            // Currently pk_indices for source is always empty since pk information is not
            // passed via `StreamSource` so null pk may be emitted to downstream.
            //
            // TODO: use the correct information to fill in pk_dicies.
            // We should consdier add back the "pk_column_ids" field removed by #8841 in
            // StreamSource
            params.info.pk_indices.clone(),
        );

        let source_ctrl_opts = SourceCtrlOpts {
            chunk_size: params.env.config().developer.chunk_size,
        };

        let source_column_ids: Vec<_> = source_columns
            .iter()
            .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
            .collect();

        // FIXME: remove this. It's wrong
        let state_table_handler = SourceStateTableHandler::from_table_catalog(
            node.state_table.as_ref().unwrap(),
            store.clone(),
        )
        .await;
        let backfill_state_table = BackfillStateTableHandler::from_table_catalog(
            node.state_table.as_ref().unwrap(),
            store.clone(),
        )
        .await;
        let stream_source_core = StreamSourceCore::new(
            source_id,
            source_name,
            source_column_ids,
            source_desc_builder,
            state_table_handler,
        );

        let exec = KafkaBackfillExecutor::new(
            params.actor_context.clone(),
            params.info.clone(),
            stream_source_core,
            params.executor_stats.clone(),
            // barrier_receiver,
            system_params,
            source_ctrl_opts.clone(),
            params.env.connector_params(),
            backfill_state_table,
        );
        Ok(KafkaBackfillExecutorWrapper { inner: exec, input }.boxed())
    }
}
