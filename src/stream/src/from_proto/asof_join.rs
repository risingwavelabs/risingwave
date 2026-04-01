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

use std::sync::Arc;

use risingwave_common::config::streaming::JoinEncodingType;
use risingwave_pb::plan_common::AsOfJoinType as JoinTypeProto;
use risingwave_pb::stream_plan::AsOfJoinNode;

use super::*;
use crate::common::table::state_table::StateTableBuilder;
use crate::executor::asof_join::*;
use crate::executor::{AsOfCpuEncoding, AsOfDesc, AsOfJoinType, AsOfMemoryEncoding, JoinType};

pub struct AsOfJoinExecutorBuilder;

impl ExecutorBuilder for AsOfJoinExecutorBuilder {
    type Node = AsOfJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        // This assert is to make sure AsOf join can use `JoinChunkBuilder` as Hash join.
        assert_eq!(AsOfJoinType::Inner, JoinType::Inner);
        assert_eq!(AsOfJoinType::LeftOuter, JoinType::LeftOuter);
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for AsOf join"));

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let table_l = node.get_left_table()?;
        let table_r = node.get_right_table()?;

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
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let state_table_l = StateTableBuilder::new(table_l, store.clone(), Some(vnodes.clone()))
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;

        let state_table_r = StateTableBuilder::new(table_r, store.clone(), Some(vnodes.clone()))
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;

        let join_type_proto = node.get_join_type()?;
        let as_of_desc_proto = node.get_asof_desc()?;
        let asof_desc = AsOfDesc::from_protobuf(as_of_desc_proto)?;

        let null_safe = node.get_null_safe().clone();
        let use_cache = node.use_cache.unwrap_or(true);

        // Previously, the `join_encoding_type` is persisted in the plan node.
        // Now it's always `Unspecified` and we should refer to the job's config override.
        #[allow(deprecated)]
        let join_encoding_type = node
            .get_join_encoding_type()
            .map_or(params.config.developer.join_encoding_type, Into::into);

        macro_rules! build {
            ($join_type:ident, $encoding:ident) => {
                AsOfJoinExecutor::<_, { AsOfJoinType::$join_type }, $encoding>::new(
                    params.actor_context,
                    params.info.clone(),
                    source_l,
                    source_r,
                    params_l,
                    params_r,
                    null_safe,
                    output_indices,
                    state_table_l,
                    state_table_r,
                    params.watermark_epoch,
                    params.executor_stats,
                    params.config.developer.chunk_size,
                    asof_desc,
                    use_cache,
                    params.config.developer.high_join_amplification_threshold,
                )
                .boxed()
            };
        }

        let exec = match (join_type_proto, join_encoding_type) {
            (JoinTypeProto::Inner, JoinEncodingType::Memory) => {
                build!(Inner, AsOfMemoryEncoding)
            }
            (JoinTypeProto::Inner, JoinEncodingType::Cpu) => {
                build!(Inner, AsOfCpuEncoding)
            }
            (JoinTypeProto::LeftOuter, JoinEncodingType::Memory) => {
                build!(LeftOuter, AsOfMemoryEncoding)
            }
            (JoinTypeProto::LeftOuter, JoinEncodingType::Cpu) => {
                build!(LeftOuter, AsOfCpuEncoding)
            }
            (JoinTypeProto::Unspecified, _) => unreachable!(),
        };
        Ok((params.info, exec).into())
    }
}
