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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::file_scan_node::{FileFormat, StorageType};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{AzblobFileScanNode, FileScanNode, GcsFileScanNode};

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchFileScan {
    pub base: PlanBase<Batch>,
    pub core: generic::FileScanBackend,
}

impl BatchFileScan {
    pub fn new(core: generic::FileScanBackend) -> Self {
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
        match &self.core {
            generic::FileScanBackend::FileScan(file_scan) => NodeBody::FileScan(FileScanNode {
                columns: file_scan
                    .columns()
                    .into_iter()
                    .map(|col| col.to_protobuf())
                    .collect(),
                file_format: match file_scan.file_format {
                    generic::FileFormat::Parquet => FileFormat::Parquet as i32,
                },
                storage_type: StorageType::S3 as i32,

                s3_region: file_scan.s3_region.clone(),
                s3_access_key: file_scan.s3_access_key.clone(),
                s3_secret_key: file_scan.s3_secret_key.clone(),
                file_location: file_scan.file_location.clone(),
                s3_endpoint: file_scan.s3_endpoint.clone(),
            }),
            generic::FileScanBackend::GcsFileScan(gcs_file_scan) => {
                NodeBody::GcsFileScan(GcsFileScanNode {
                    columns: gcs_file_scan
                        .columns()
                        .into_iter()
                        .map(|col| col.to_protobuf())
                        .collect(),
                    file_format: match gcs_file_scan.file_format {
                        generic::FileFormat::Parquet => FileFormat::Parquet as i32,
                    },
                    credential: gcs_file_scan.credential.clone(),
                    file_location: gcs_file_scan.file_location.clone(),
                })
            }

            generic::FileScanBackend::AzblobFileScan(azblob_file_scan) => {
                NodeBody::AzblobFileScan(AzblobFileScanNode {
                    columns: azblob_file_scan
                        .columns()
                        .into_iter()
                        .map(|col| col.to_protobuf())
                        .collect(),
                    file_format: match azblob_file_scan.file_format {
                        generic::FileFormat::Parquet => FileFormat::Parquet as i32,
                    },
                    account_name: azblob_file_scan.account_name.clone(),
                    account_key: azblob_file_scan.account_key.clone(),
                    endpoint: azblob_file_scan.endpoint.clone(),
                    file_location: azblob_file_scan.file_location.clone(),
                })
            }
        }
    }
}

impl ExprRewritable for BatchFileScan {}

impl ExprVisitable for BatchFileScan {}
