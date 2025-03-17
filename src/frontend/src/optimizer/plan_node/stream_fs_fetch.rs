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

use std::rc::Rc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamFsFetch, StreamFsFetchNode};

use super::stream::prelude::*;
use super::{PlanBase, PlanRef, PlanTreeNodeUnary};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{ExprRewritable, StreamNode, generic};
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFsFetch {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    core: generic::Source,
}

impl PlanTreeNodeUnary for StreamFsFetch {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.core.clone())
    }
}
impl_plan_tree_node_for_unary! { StreamFsFetch }

impl StreamFsFetch {
    pub fn new(input: PlanRef, source: generic::Source) -> Self {
        let base = PlanBase::new_stream_with_core(
            &source,
            Distribution::SomeShard,
            source.catalog.as_ref().is_none_or(|s| s.append_only),
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Self {
            base,
            input,
            core: source,
        }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.core
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
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

impl ExprVisitable for StreamFsFetch {}

impl StreamNode for StreamFsFetch {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        // `StreamFsFetch` is same as source in proto def, so the following code is the same as `StreamSource`
        let source_catalog = self.source_catalog();

        let source_inner = source_catalog.map(|source_catalog| {
            let (with_properties, secret_refs) =
                source_catalog.with_properties.clone().into_parts();
            PbStreamFsFetch {
                source_id: source_catalog.id,
                source_name: source_catalog.name.clone(),
                state_table: Some(
                    generic::Source::infer_internal_table_catalog(true)
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                info: Some(source_catalog.info.clone()),
                row_id_index: self.core.row_id_index.map(|index| index as _),
                columns: self
                    .core
                    .column_catalog
                    .iter()
                    .map(|c| c.to_protobuf())
                    .collect_vec(),
                with_properties,
                rate_limit: source_catalog.rate_limit,
                secret_refs,
            }
        });
        NodeBody::StreamFsFetch(Box::new(StreamFsFetchNode {
            node_inner: source_inner,
        }))
    }
}
