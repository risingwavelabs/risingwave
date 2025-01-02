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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::file_scan_node::{FileFormat, StorageType};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::FileScanNode;

use super::batch::prelude::*;
use super::utils::{childless_record, column_names_pretty, Distill};
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchFileScan {
    pub base: PlanBase<Batch>,
    pub core: generic::FileScan,
}

impl BatchFileScan {
    pub fn new(core: generic::FileScan) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());

        Self { base, core }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
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

impl_plan_tree_node_for_leaf! { BatchFileScan }

impl Distill for BatchFileScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("BatchFileScan", fields)
    }
}

impl ToLocalBatch for BatchFileScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchFileScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchFileScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::FileScan(FileScanNode {
            columns: self
                .core
                .columns()
                .into_iter()
                .map(|col| col.to_protobuf())
                .collect(),
            file_format: match self.core.file_format {
                generic::FileFormat::Parquet => FileFormat::Parquet as i32,
            },
            storage_type: match self.core.storage_type {
                generic::StorageType::S3 => StorageType::S3 as i32,
            },
            s3_region: self.core.s3_region.clone(),
            s3_access_key: self.core.s3_access_key.clone(),
            s3_secret_key: self.core.s3_secret_key.clone(),
            file_location: self.core.file_location.clone(),
        })
    }
}

impl ExprRewritable for BatchFileScan {}

impl ExprVisitable for BatchFileScan {}
