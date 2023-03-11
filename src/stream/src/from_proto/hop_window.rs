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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::stream_plan::HopWindowNode;

use super::*;
use crate::executor::HopWindowExecutor;

pub struct HopWindowExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for HopWindowExecutorBuilder {
    type Node = HopWindowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let ExecutorParams {
            actor_context,
            input,
            pk_indices,
            executor_id,
            ..
        } = params;

        let input = input.into_iter().next().unwrap();
        // TODO: reuse the schema derivation with frontend.
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let window_start_exprs: Vec<_> = node
            .get_window_start_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;
        let window_end_exprs: Vec<_> = node
            .get_window_end_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;

        let time_col = node.get_time_col() as usize;
        let time_col_data_type = input.schema().fields()[time_col].data_type();
        let output_type = DataType::window_of(&time_col_data_type).unwrap();
        let original_schema: Schema = input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        let actual_schema: Schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();
        let info = ExecutorInfo {
            schema: actual_schema,
            identity: format!("HopWindowExecutor {:X}", executor_id),
            pk_indices,
        };
        let window_slide = node.get_window_slide()?.into();
        let window_size = node.get_window_size()?.into();
        let window_offset = node.get_window_offset()?.into();

        Ok(HopWindowExecutor::new(
            actor_context,
            input,
            info,
            time_col,
            window_slide,
            window_size,
            window_offset,
            window_start_exprs,
            window_end_exprs,
            output_indices,
        )
        .boxed())
    }
}
