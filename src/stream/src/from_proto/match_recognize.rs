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

use risingwave_pb::stream_plan::MatchRecognizeNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::{
    CompiledDefine, CompiledMeasure, Executor, MatchRecognizeExecutor, MatchRecognizeExecutorArgs,
    Nfa, SkipMode, parse_pattern,
};
use crate::task::ExecutorParams;

pub struct MatchRecognizeExecutorBuilder;

impl_stream_node_body!(MatchRecognize(MatchRecognizeNode) => MatchRecognizeExecutorBuilder);

impl ExecutorBuilder for MatchRecognizeExecutorBuilder {
    type Node = MatchRecognizeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &MatchRecognizeNode,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let partition_key_indices = node.partition_by.iter().map(|&i| i as usize).collect();
        let order_key_indices = node.order_by.iter().map(|&i| i as usize).collect();

        let defines = node
            .defines
            .iter()
            .map(|d| CompiledDefine::from_protobuf(d, params.eval_error_report.clone()))
            .collect::<crate::executor::StreamExecutorResult<Vec<_>>>()?;
        let measures = node
            .measures
            .iter()
            .map(|m| CompiledMeasure::from_protobuf(m, params.eval_error_report.clone()))
            .collect::<crate::executor::StreamExecutorResult<Vec<_>>>()?;

        let pattern = parse_pattern(&node.pattern)
            .map_err(|e| anyhow::anyhow!("invalid MATCH_RECOGNIZE pattern: {e}"))?;
        let nfa = Nfa::compile(&pattern);

        let skip = match node.after_match_skip.as_str() {
            "to_next_row" => SkipMode::ToNextRow,
            s if s.starts_with("to_first:") => SkipMode::ToFirst(s["to_first:".len()..].to_owned()),
            s if s.starts_with("to_last:") => SkipMode::ToLast(s["to_last:".len()..].to_owned()),
            _ => SkipMode::PastLastRow,
        };

        let input_arity = input.schema().len();

        let state_table =
            StateTableBuilder::new(node.get_state_table().as_ref().unwrap(), store, None)
                .forbid_preload_all_rows()
                .build()
                .await;

        let exec = MatchRecognizeExecutor::new(MatchRecognizeExecutorArgs {
            ctx: params.actor_context,
            input,
            schema: params.info.schema.clone(),
            chunk_size: params.config.developer.chunk_size,
            partition_key_indices,
            order_key_indices,
            measures,
            defines,
            nfa,
            skip,
            input_arity,
            state_table,
        });

        Ok((params.info, exec).into())
    }
}
