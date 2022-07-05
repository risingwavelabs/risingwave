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

use std::fmt;

use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanBase, PlanRef, ToStreamProst};

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct StreamSink {
    pub base: PlanBase,
    input: PlanRef,
}

impl StreamSink {
    fn derive_plan_base(input: &PlanRef) -> Result<PlanBase> {
        let ctx = input.ctx();

        let schema = input.schema().clone();
        let pk_indices = input.pk_indices();

        Ok(PlanBase::new_stream(
            ctx,
            schema,
            pk_indices.to_vec(),
            input.distribution().clone(),
            input.append_only(),
        ))
    }

    #[must_use]
    pub fn new(input: PlanRef) -> Self {
        let base = Self::derive_plan_base(&input).unwrap();
        Self { base, input }
    }
}

impl_plan_tree_node_for_leaf! { StreamSink }

impl fmt::Display for StreamSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!();
    }
}

impl ToStreamProst for StreamSink {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        todo!();
    }
}
