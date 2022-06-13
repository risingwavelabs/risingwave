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

use risingwave_common::catalog::{Field, Schema};

use super::*;
use crate::executor::receiver::ReceiverExecutor;
use crate::executor::MergeExecutor;

pub struct MergeExecutorBuilder;

impl ExecutorBuilder for MergeExecutorBuilder {
    fn new_boxed_executor<S: StateStoreProxy>(
        params: ExecutorParams,
        x_node: &StreamNode,
        _store: S,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(x_node.get_node_body().unwrap(), NodeBody::Merge)?;
        let upstreams = node.get_upstream_actor_id();
        let fields = node.fields.iter().map(Field::from).collect();
        let schema = Schema::new(fields);
        let mut rxs = stream.get_receive_message(params.actor_id, upstreams)?;
        let actor_context = params.actor_context;

        if upstreams.len() == 1 {
            Ok(ReceiverExecutor::new(
                schema,
                params.pk_indices,
                rxs.remove(0),
                actor_context,
                x_node.operator_id,
            )
            .boxed())
        } else {
            Ok(MergeExecutor::new(
                schema,
                params.pk_indices,
                params.actor_id,
                rxs,
                actor_context,
                x_node.operator_id,
            )
            .boxed())
        }
    }
}
