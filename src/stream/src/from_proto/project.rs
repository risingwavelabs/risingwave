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

use multimap::MultiMap;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::expr::expr_node;
use risingwave_pb::stream_plan::ProjectNode;

use super::*;
use crate::executor::ProjectExecutor;

pub struct ProjectExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for ProjectExecutorBuilder {
    type Node = ProjectNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let project_exprs: Vec<_> = node
            .get_select_list()
            .iter()
            .map(build_from_prost)
            .try_collect()?;

        let watermark_derivations = MultiMap::from_iter(
            node.get_watermark_input_key()
                .iter()
                .map(|key| *key as usize)
                .zip_eq_fast(
                    node.get_watermark_output_key()
                        .iter()
                        .map(|key| *key as usize),
                ),
        );
        let extremely_light = node.get_select_list().iter().all(|expr| {
            let expr_type = expr.get_expr_type().unwrap();
            expr_type == expr_node::Type::InputRef || expr_type == expr_node::Type::ConstantValue
        });
        let materialize_selectivity_threshold = if extremely_light { 0.0 } else { 0.5 };
        Ok(ProjectExecutor::new(
            params.actor_context,
            input,
            params.pk_indices,
            project_exprs,
            params.executor_id,
            watermark_derivations,
            materialize_selectivity_threshold,
        )
        .boxed())
    }
}
