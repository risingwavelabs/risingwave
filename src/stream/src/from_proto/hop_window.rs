// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::stream_node;

use super::*;
use crate::executor::HopWindowExecutor;

pub struct HopWindowExecutorBuilder;

impl ExecutorBuilder for HopWindowExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let ExecutorParams {
            input,
            pk_indices,
            executor_id,
            ..
        } = params;

        let input = input.into_iter().next().unwrap();
        // TODO: reuse the schema deriviation with frontend.
        let schema = input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(DataType::Timestamp, "window_start"),
                Field::with_name(DataType::Timestamp, "window_end"),
            ])
            .collect();
        let info = ExecutorInfo {
            schema,
            identity: format!("HopWindowExecutor {:X}", executor_id),
            pk_indices,
        };
        let Some(stream_node::NodeBody::HopWindow(node)) = &node.node_body else {
            unreachable!();
        };
        let time_col = node.get_time_col()?.column_idx as usize;
        let window_slide = node.get_window_slide()?.into();
        let window_size = node.get_window_size()?.into();
        Ok(HopWindowExecutor::new(input, info, time_col, window_slide, window_size).boxed())
    }
}
