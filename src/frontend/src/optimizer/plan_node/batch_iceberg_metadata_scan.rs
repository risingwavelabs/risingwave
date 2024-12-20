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

use std::hash::{Hash, Hasher};

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::IcebergMetadataScanNode;

use super::batch::prelude::*;
use super::utils::{childless_record, column_names_pretty, Distill};
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone)]
pub struct BatchIcebergMetadataScan {
    pub base: PlanBase<Batch>,
    pub core: generic::IcebergMetadataScan,
}

impl PartialEq for BatchIcebergMetadataScan {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.core == other.core
    }
}

impl Eq for BatchIcebergMetadataScan {}

impl Hash for BatchIcebergMetadataScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.base.hash(state);
        self.core.hash(state);
    }
}

impl BatchIcebergMetadataScan {
    pub fn new(core: generic::IcebergMetadataScan) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());

        Self { base, core }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }
}

impl_plan_tree_node_for_leaf! { BatchIcebergMetadataScan }

impl Distill for BatchIcebergMetadataScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            ("type", self.core.table_type.as_str_name().into()),
            ("columns", column_names_pretty(self.schema())),
        ];
        childless_record("BatchIcebergMetadataScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergMetadataScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ToDistributedBatch for BatchIcebergMetadataScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ToBatchPb for BatchIcebergMetadataScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let (with_properties, secret_refs) = self.core.iceberg_properties.clone().into_parts();
        NodeBody::IcebergMetadataScan(IcebergMetadataScanNode {
            table_type: self.core.table_type.into(),
            with_properties,
            secret_refs,
        })
    }
}

impl ExprRewritable for BatchIcebergMetadataScan {}

impl ExprVisitable for BatchIcebergMetadataScan {}
