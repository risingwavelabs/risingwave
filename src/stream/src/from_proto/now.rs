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

use anyhow::Context;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::value_encoding::DatumFromProtoExt;
use risingwave_pb::stream_plan::now_node::PbMode as PbNowMode;
use risingwave_pb::stream_plan::{NowNode, PbNowModeGenerateSeries};
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::{Executor, NowExecutor, NowMode};
use crate::task::ExecutorParams;

pub struct NowExecutorBuilder;

impl ExecutorBuilder for NowExecutorBuilder {
    type Node = NowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &NowNode,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);

        let mode = if let Ok(pb_mode) = node.get_mode() {
            match pb_mode {
                PbNowMode::UpdateCurrent(_) => NowMode::UpdateCurrent,
                PbNowMode::GenerateSeries(PbNowModeGenerateSeries {
                    start_timestamp,
                    interval,
                }) => {
                    let start_timestamp = Datum::from_protobuf(
                        start_timestamp.as_ref().unwrap(),
                        &DataType::Timestamptz,
                    )
                    .context("`start_timestamp` field is not decodable")?
                    .context("`start_timestamp` field should not be NULL")?
                    .into_timestamptz();
                    let interval =
                        Datum::from_protobuf(interval.as_ref().unwrap(), &DataType::Interval)
                            .context("`interval` field is not decodable")?
                            .context("`interval` field should not be NULL")?
                            .into_interval();
                    NowMode::GenerateSeries {
                        start_timestamp,
                        interval,
                    }
                }
            }
        } else {
            // default to `UpdateCurrent` for backward-compatibility
            NowMode::UpdateCurrent
        };

        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, store, None).await;
        let barrier_interval_ms = params
            .env
            .system_params_manager_ref()
            .get_params()
            .load()
            .barrier_interval_ms();
        let progress_ratio = params.env.config().developer.now_progress_ratio;
        let exec = NowExecutor::new(
            params.info.schema.data_types(),
            mode,
            params.eval_error_report,
            barrier_receiver,
            state_table,
            progress_ratio,
            barrier_interval_ms,
        );
        Ok((params.info, exec).into())
    }
}
