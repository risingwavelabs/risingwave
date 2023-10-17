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

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamFsFetch, StreamFsFetchNode};

use super::{PlanBase, PlanRef, PlanTreeNodeUnary};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::utils::{childless_record, Distill};
use crate::optimizer::plan_node::{generic, ExprRewritable, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFsFetch {
    pub base: PlanBase,
    input: PlanRef,
    logical: generic::Source,
}

impl PlanTreeNodeUnary for StreamFsFetch {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.logical.clone())
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
            logical: source,
        }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.logical
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.logical.catalog.clone()
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
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        // `StreamFsFetch` is same as source in proto def, so the following code is the same as `StreamSource`
        let source_catalog = self.source_catalog();
        let source_inner = source_catalog.map(|source_catalog| PbStreamFsFetch {
            source_id: source_catalog.id,
            source_name: source_catalog.name.clone(),
            state_table: Some(
                generic::Source::infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(source_catalog.info.clone()),
            row_id_index: self.logical.row_id_index.map(|index| index as _),
            columns: self
                .logical
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            properties: source_catalog.properties.clone().into_iter().collect(),
            rate_limit: self
                .base
                .ctx()
                .session_ctx()
                .config()
                .get_streaming_rate_limit(),
        });
        NodeBody::StreamFsFetch(StreamFsFetchNode {
            node_inner: source_inner,
        })
    }
}
