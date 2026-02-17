// Copyright 2022 RisingWave Labs
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

use risingwave_common::config::streaming::JoinEncodingType;
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{NonStrictExpression, build_non_strict_from_prost};
use risingwave_pb::plan_common::JoinType as JoinTypeProto;
use risingwave_pb::stream_plan::{
    HashJoinNode, HashJoinWatermarkHandleDesc, JoinKeyWatermarkIndex,
};

use super::*;
use crate::common::table::state_table::{StateTable, StateTableBuilder};
use crate::executor::hash_join::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, CpuEncoding, JoinType, MemoryEncoding};
use crate::task::AtomicU64Ref;

pub struct HashJoinExecutorBuilder;

impl ExecutorBuilder for HashJoinExecutorBuilder {
    type Node = HashJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let is_append_only = node.is_append_only;
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for hash join"));

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let table_l = node.get_left_table()?;
        let degree_table_l = node.get_left_degree_table()?;

        let table_r = node.get_right_table()?;
        let degree_table_r = node.get_right_degree_table()?;

        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_left_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_right_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let null_safe = node.get_null_safe().clone();
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(build_non_strict_from_prost(
                cond_prost,
                params.eval_error_report.clone(),
            )?),
            Err(_) => None,
        };
        trace!("Join non-equi condition: {:?}", condition);
        let mut inequality_pairs = vec![];
        if let Some(desc) = node.watermark_handle_desc.as_ref() {
            inequality_pairs = desc
                .inequality_pairs
                .iter()
                .map(|inequality_pair| InequalityPairInfo {
                    left_idx: inequality_pair.left_idx as usize,
                    right_idx: inequality_pair.right_idx as usize,
                    clean_left_state: inequality_pair.clean_left_state,
                    clean_right_state: inequality_pair.clean_right_state,
                    op: inequality_pair.op(),
                })
                .collect();
        }

        let watermark_indices_in_jk = resolve_clean_watermark_indices_in_jk(
            node.watermark_handle_desc.as_ref(),
            params_l.join_key_indices.len(),
        );

        let join_key_data_types = params_l
            .join_key_indices
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();

        let state_table_l = StateTableBuilder::new(table_l, store.clone(), Some(vnodes.clone()))
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;
        let degree_state_table_l =
            StateTableBuilder::new(degree_table_l, store.clone(), Some(vnodes.clone()))
                .enable_preload_all_rows_by_config(&params.config)
                .build()
                .await;

        let state_table_r = StateTableBuilder::new(table_r, store.clone(), Some(vnodes.clone()))
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;
        let degree_state_table_r = StateTableBuilder::new(degree_table_r, store, Some(vnodes))
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;

        // Previously, the `join_encoding_type` is persisted in the plan node.
        // Now it's always `Unspecified` and we should refer to the job's config override.
        #[allow(deprecated)]
        let join_encoding_type = node
            .get_join_encoding_type()
            .map_or(params.config.developer.join_encoding_type, Into::into);

        let args = HashJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            info: params.info.clone(),
            source_l,
            source_r,
            params_l,
            params_r,
            null_safe,
            output_indices,
            cond: condition,
            inequality_pairs,
            state_table_l,
            degree_state_table_l,
            state_table_r,
            degree_state_table_r,
            lru_manager: params.watermark_epoch,
            is_append_only,
            metrics: params.executor_stats,
            join_type_proto: node.get_join_type()?,
            join_key_data_types,
            chunk_size: params.config.developer.chunk_size,
            high_join_amplification_threshold: (params.config.developer)
                .high_join_amplification_threshold,
            join_encoding_type,
            watermark_indices_in_jk,
        };

        let exec = args.dispatch()?;
        Ok((params.info, exec).into())
    }
}

struct HashJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    source_l: Executor,
    source_r: Executor,
    params_l: JoinParams,
    params_r: JoinParams,
    null_safe: Vec<bool>,
    output_indices: Vec<usize>,
    cond: Option<NonStrictExpression>,
    inequality_pairs: Vec<InequalityPairInfo>,
    state_table_l: StateTable<S>,
    degree_state_table_l: StateTable<S>,
    state_table_r: StateTable<S>,
    degree_state_table_r: StateTable<S>,
    lru_manager: AtomicU64Ref,
    is_append_only: bool,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
    chunk_size: usize,
    high_join_amplification_threshold: usize,
    join_encoding_type: JoinEncodingType,
    watermark_indices_in_jk: Vec<(usize, bool)>,
}

fn resolve_clean_watermark_indices_in_jk(
    desc: Option<&HashJoinWatermarkHandleDesc>,
    join_key_len: usize,
) -> Vec<(usize, bool)> {
    if let Some(desc) = desc {
        return desc
            .watermark_indices_in_jk
            .iter()
            .map(
                |JoinKeyWatermarkIndex {
                     index,
                     do_state_cleaning,
                 }| (*index as usize, *do_state_cleaning),
            )
            .collect_vec();
    }
    // For backward compatibility, if there are no `watermark_indices_in_jk`,
    // we assume the first join key can be used for state cleaning.
    if join_key_len > 0 {
        vec![(0, true)]
    } else {
        vec![]
    }
}

impl<S: StateStore> HashKeyDispatcher for HashJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident, $join_encoding:ident) => {
                Ok(
                    HashJoinExecutor::<K, S, { JoinType::$join_type }, $join_encoding>::new(
                        self.ctx,
                        self.info,
                        self.source_l,
                        self.source_r,
                        self.params_l,
                        self.params_r,
                        self.null_safe,
                        self.output_indices,
                        self.cond,
                        self.inequality_pairs,
                        self.state_table_l,
                        self.degree_state_table_l,
                        self.state_table_r,
                        self.degree_state_table_r,
                        self.lru_manager,
                        self.is_append_only,
                        self.metrics,
                        self.chunk_size,
                        self.high_join_amplification_threshold,
                        self.watermark_indices_in_jk.clone(),
                    )
                    .boxed(),
                )
            };
        }

        macro_rules! build_match {
            ($($join_type:ident),*) => {
                match (self.join_type_proto, self.join_encoding_type) {
                    (JoinTypeProto::AsofInner, _)
                    | (JoinTypeProto::AsofLeftOuter, _)
                    | (JoinTypeProto::Unspecified, _) => unreachable!(),
                    $(
                        (JoinTypeProto::$join_type, JoinEncodingType::Memory) => build!($join_type, MemoryEncoding),
                        (JoinTypeProto::$join_type, JoinEncodingType::Cpu) => build!($join_type, CpuEncoding),
                    )*
                }
            };
        }
        build_match! {
            Inner,
            LeftOuter,
            RightOuter,
            FullOuter,
            LeftSemi,
            LeftAnti,
            RightSemi,
            RightAnti
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::{HashJoinWatermarkHandleDesc, JoinKeyWatermarkIndex};

    use super::resolve_clean_watermark_indices_in_jk;

    #[test]
    fn test_resolve_clean_watermark_indices_in_jk_from_desc() {
        let desc = HashJoinWatermarkHandleDesc {
            watermark_indices_in_jk: vec![
                JoinKeyWatermarkIndex {
                    index: 2,
                    do_state_cleaning: true,
                },
                JoinKeyWatermarkIndex {
                    index: 1,
                    do_state_cleaning: false,
                },
            ],
            inequality_pairs: vec![],
        };
        assert_eq!(
            resolve_clean_watermark_indices_in_jk(Some(&desc), 3),
            vec![(2, true), (1, false)]
        );
    }

    #[test]
    fn test_resolve_clean_watermark_indices_in_jk_default_first() {
        assert_eq!(
            resolve_clean_watermark_indices_in_jk(None, 2),
            vec![(0, true)]
        );
    }

    #[test]
    fn test_resolve_clean_watermark_indices_in_jk_empty_on_no_keys() {
        assert_eq!(resolve_clean_watermark_indices_in_jk(None, 0), vec![]);
    }
}
