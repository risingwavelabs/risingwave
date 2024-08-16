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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::source_node::IcebergSourceType;
use risingwave_pb::batch_plan::SourceNode;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{childless_record, column_names_pretty, Distill};
use super::{
    generic, BatchIcebergScan, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch,
    ToLocalBatch,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::Distribution;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchIcebergCountStarScan {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,
}

impl BatchIcebergCountStarScan {
    pub fn new_with_batch_iceberg_scan(batch_iceberg_scan: &BatchIcebergScan) -> Self {
        let mut core = batch_iceberg_scan.core.clone();
        core.column_catalog = vec![ColumnCatalog::visible(ColumnDesc::named(
            "count",
            ColumnId::first_user_column(),
            DataType::Int64,
        ))];
        let base = PlanBase::new_batch_with_core(
            &core,
            batch_iceberg_scan.base.distribution().clone(),
            batch_iceberg_scan.base.order().clone(),
        );
        Self { base, core }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_core(&self, core: generic::Source) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            self.base.distribution().clone(),
            self.base.order().clone(),
        );
        Self { base, core }
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

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }
}

impl_plan_tree_node_for_leaf! { BatchIcebergCountStarScan }

impl Distill for BatchIcebergCountStarScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
        ];
        childless_record("BatchIcebergCountStarScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergCountStarScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchIcebergCountStarScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchIcebergCountStarScan {
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
            iceberg_source_type: IcebergSourceType::CountStar.into(),
        })
    }
}

impl ExprRewritable for BatchIcebergCountStarScan {}

impl ExprVisitable for BatchIcebergCountStarScan {}
