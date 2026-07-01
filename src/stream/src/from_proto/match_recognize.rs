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

use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::MatchRecognizeNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::{
    CompiledDefine, CompiledMeasure, Executor, MatchRecognizeExecutor, MatchRecognizeExecutorArgs,
    Nfa, SkipMode, pattern_from_protobuf,
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
        // ORDER BY is carried as `ColumnOrder`. v1 only supports the default ascending order (the
        // binder rejects anything else); assert it here too so a non-ascending plan fails fast
        // rather than being silently sorted ascending by the executor.
        let order_key_indices = node
            .order_by
            .iter()
            .map(|c| {
                let co = ColumnOrder::from_protobuf(c);
                if co.order_type != OrderType::ascending() {
                    return Err(anyhow::anyhow!(
                        "MATCH_RECOGNIZE only supports the default ascending ORDER BY, got {:?}",
                        co.order_type
                    )
                    .into());
                }
                Ok(co.column_index)
            })
            .collect::<StreamResult<Vec<usize>>>()?;

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

        let pattern_node = node
            .pattern_node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE node missing pattern"))?;
        let pattern = pattern_from_protobuf(pattern_node)
            .map_err(|e| anyhow::anyhow!("invalid MATCH_RECOGNIZE pattern: {e}"))?;
        let nfa = Nfa::compile(&pattern);

        // Accept only the encodings the frontend produces; fail fast on anything else rather than
        // silently defaulting to PAST LAST ROW, which would mask a corrupt plan or a version skew.
        let skip = match node.after_match_skip.as_str() {
            "past_last_row" => SkipMode::PastLastRow,
            "to_next_row" => SkipMode::ToNextRow,
            s if s.starts_with("to_first:") => SkipMode::ToFirst(s["to_first:".len()..].to_owned()),
            s if s.starts_with("to_last:") => SkipMode::ToLast(s["to_last:".len()..].to_owned()),
            other => {
                return Err(anyhow::anyhow!(
                    "invalid MATCH_RECOGNIZE after_match_skip encoding: {other:?}"
                )
                .into());
            }
        };

        let within = node
            .within
            .as_ref()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .transpose()?;
        let within_deadline = node
            .within_deadline
            .as_ref()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .transpose()?;

        let input_arity = input.schema().len();

        let vnode_bitmap = params.vnode_bitmap.clone().map(std::sync::Arc::new);
        let state_table = StateTableBuilder::new(
            node.get_state_table().as_ref().unwrap(),
            store.clone(),
            vnode_bitmap.clone(),
        )
        .forbid_preload_all_rows()
        .build()
        .await;
        // Wakeup frontier: distributed by partition (same as the buffer table), so they re-shard
        // together on rescale. High cardinality → do not preload all rows.
        let frontier_meta_table = StateTableBuilder::new(
            node.get_frontier_meta_table().as_ref().unwrap(),
            store.clone(),
            vnode_bitmap.clone(),
        )
        .forbid_preload_all_rows()
        .build()
        .await;
        let frontier_index_table = StateTableBuilder::new(
            node.get_frontier_index_table().as_ref().unwrap(),
            store,
            vnode_bitmap,
        )
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
            within,
            nfa,
            skip,
            within_deadline,
            input_arity,
            state_table,
            frontier_meta_table,
            frontier_index_table,
        });

        Ok((params.info, exec).into())
    }
}
