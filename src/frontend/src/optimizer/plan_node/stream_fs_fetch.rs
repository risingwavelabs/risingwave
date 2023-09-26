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

use fixedbitset::FixedBitSet;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::{PlanBase, PlanRef, PlanTreeNodeUnary};
use crate::optimizer::plan_node::utils::{childless_record, Distill};
use crate::optimizer::plan_node::{generic, ExprRewritable, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFsFetch {
    pub base: PlanBase,
    input: PlanRef,
    source: generic::Source,
}

impl PlanTreeNodeUnary for StreamFsFetch {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.source.clone())
    }
}
impl_plan_tree_node_for_unary! { StreamFsFetch }

impl StreamFsFetch {
    pub fn new(input: PlanRef, source: generic::Source) -> Self {
        let base = PlanBase::new_stream_with_logical(
            &source,
            Distribution::SomeShard,
            source.catalog.as_ref().map_or(true, |s| s.append_only),
            false,
            FixedBitSet::with_capacity(source.column_catalog.len()),
        );

        Self {
            base,
            input,
            source,
        }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.source
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }
}

impl Distill for StreamFsFetch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let columns = self
            .get_columns()
            .iter()
            .map(|ele| Pretty::from(ele.to_string()))
            .collect();
        let col = Pretty::Array(columns);
        childless_record("StreamFsFetch", vec![("columns", col)])
    }
}

impl ExprRewritable for StreamFsFetch {}

impl StreamNode for StreamFsFetch {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        todo!()
    }
}
