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

use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_connector::source::iceberg::IcebergFileScanTask;
use risingwave_pb::batch_plan::IcebergScanNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, ToBatchPb, ToDistributedBatch, ToLocalBatch,
    generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BatchIcebergScan {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,
    pub iceberg_file_scan_task: Arc<IcebergFileScanTask>,
    pub snapshot_id: Option<i64>,
}

impl Eq for BatchIcebergScan {}

impl BatchIcebergScan {
    pub fn new(
        core: generic::Source,
        iceberg_file_scan_task: Arc<IcebergFileScanTask>,
        snapshot_id: Option<i64>,
    ) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );

        Self {
            base,
            core,
            iceberg_file_scan_task,
            snapshot_id,
        }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self
            .base
            .clone_with_new_distribution(Distribution::SomeShard);
        Self {
            base,
            core: self.core.clone(),
            iceberg_file_scan_task: self.iceberg_file_scan_task.clone(),
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }
}

impl_plan_tree_node_for_leaf! { Batch, BatchIcebergScan }

impl Distill for BatchIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
            (
                "iceberg_scan_type",
                Pretty::debug(&self.iceberg_file_scan_task.get_iceberg_scan_type()),
            ),
        ];
        childless_record("BatchIcebergScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchIcebergScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchIcebergScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.source_catalog().unwrap();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        NodeBody::IcebergScan(IcebergScanNode {
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            with_properties,
            split: vec![],
            secret_refs,
            iceberg_scan_type: self.iceberg_file_scan_task.get_iceberg_scan_type() as i32,
        })
    }
}

impl ExprRewritable<Batch> for BatchIcebergScan {}

impl ExprVisitable for BatchIcebergScan {}
