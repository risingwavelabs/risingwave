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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::batch_plan::SourceNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

/// [`BatchSource`] represents a table/connector source at the very beginning of the graph.
///
/// For supported batch connectors, see [`crate::scheduler::plan_fragmenter::SourceScanInfo`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSource {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,
}

impl BatchSource {
    pub fn new(core: generic::Source) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );

        Self { base, core }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self
            .base
            .clone_with_new_distribution(Distribution::SomeShard);
        Self {
            base,
            core: self.core.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { BatchSource }

impl Distill for BatchSource {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let mut fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
        ];
        if let Some(as_of) = &self.core.as_of {
            fields.push(("as_of", Pretty::debug(as_of)));
        }
        childless_record("BatchSource", fields)
    }
}

impl ToLocalBatch for BatchSource {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchSource {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchSource {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.source_catalog().unwrap();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        NodeBody::Source(SourceNode {
            source_id: source_catalog.id,
            info: Some(source_catalog.info.clone()),
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            with_properties,
            split: vec![],
            secret_refs,
        })
    }
}

impl ExprRewritable for BatchSource {}

impl ExprVisitable for BatchSource {}
