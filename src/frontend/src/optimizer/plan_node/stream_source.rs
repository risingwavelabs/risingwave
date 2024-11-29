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

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{PbStreamSource, SourceNode};

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, StreamNode};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::property::{Distribution, MonotonicityMap};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSource {
    pub base: PlanBase<Stream>,
    pub(crate) core: generic::Source,
}

impl StreamSource {
    pub fn new(core: generic::Source) -> Self {
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::SomeShard,
            core.catalog.as_ref().map_or(true, |s| s.append_only),
            false,
            FixedBitSet::with_capacity(core.column_catalog.len()),
            MonotonicityMap::new(),
        );
        Self { base, core }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }
}

impl_plan_tree_node_for_leaf! { StreamSource }

impl Distill for StreamSource {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            let col = column_names_pretty(self.schema());
            vec![("source", src), ("columns", col)]
        } else {
            vec![]
        };
        childless_record("StreamSource", fields)
    }
}

impl StreamNode for StreamSource {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let source_catalog = self.source_catalog();
        let source_inner = source_catalog.map(|source_catalog| {
            let (with_properties, secret_refs) =
                source_catalog.with_properties.clone().into_parts();
            PbStreamSource {
                source_id: source_catalog.id,
                source_name: source_catalog.name.clone(),
                state_table: Some(
                    generic::Source::infer_internal_table_catalog(false)
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
        PbNodeBody::Source(SourceNode { source_inner })
    }
}

impl ExprRewritable for StreamSource {}

impl ExprVisitable for StreamSource {}
