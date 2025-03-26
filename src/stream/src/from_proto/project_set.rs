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

use multimap::MultiMap;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::stream_plan::ProjectSetNode;

use super::*;
use crate::executor::project::{ProjectSetExecutor, ProjectSetSelectItem};

pub struct ProjectSetExecutorBuilder;

impl ExecutorBuilder for ProjectSetExecutorBuilder {
    type Node = ProjectSetNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let select_list: Vec<_> = node
            .get_select_list()
            .iter()
            .map(|proto| {
                ProjectSetSelectItem::from_prost(
                    proto,
                    params.eval_error_report.clone(),
                    params.env.config().developer.chunk_size,
                )
            })
            .try_collect()?;
        let watermark_derivations = MultiMap::from_iter(
            node.get_watermark_input_cols()
                .iter()
                .map(|idx| *idx as usize)
                .zip_eq_fast(
                    node.get_watermark_expr_indices()
                        .iter()
                        .map(|idx| *idx as usize),
                ),
        );
        let nondecreasing_expr_indices = node
            .get_nondecreasing_exprs()
            .iter()
            .map(|idx| *idx as usize)
            .collect();

        let chunk_size = params.env.config().developer.chunk_size;
        let exec = ProjectSetExecutor::new(
            params.actor_context,
            input,
            select_list,
            chunk_size,
            watermark_derivations,
            nondecreasing_expr_indices,
            params.eval_error_report,
        );
        Ok((params.info, exec).into())
    }
}
