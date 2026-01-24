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

use educe::Educe;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};

use super::GenericPlanNode;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileFormat {
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StorageType {
    S3,
    Gcs,
    Azblob,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub enum FileScanBackend {
    FileScan(FileScan),
    GcsFileScan(GcsFileScan),
    AzblobFileScan(AzblobFileScan),
}

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct GcsFileScan {
    pub schema: Schema,
    pub file_format: FileFormat,
    pub storage_type: StorageType,
    pub credential: String,
    pub file_location: Vec<String>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for GcsFileScan {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct AzblobFileScan {
    pub schema: Schema,
    pub file_format: FileFormat,
    pub storage_type: StorageType,
    pub account_name: String,
    pub account_key: String,
    pub endpoint: String,
    pub file_location: Vec<String>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for AzblobFileScan {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl FileScan {
    pub fn columns(&self) -> Vec<ColumnDesc> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnDesc::named(f.name.clone(), ColumnId::new(i as i32), f.data_type.clone())
            })
            .collect()
    }
}
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FileScan {
    pub schema: Schema,
    pub file_format: FileFormat,
    pub storage_type: StorageType,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub file_location: Vec<String>,
    pub s3_endpoint: String,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for FileScan {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl GcsFileScan {
    pub fn columns(&self) -> Vec<ColumnDesc> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnDesc::named(f.name.clone(), ColumnId::new(i as i32), f.data_type.clone())
            })
            .collect()
    }
}

impl AzblobFileScan {
    pub fn columns(&self) -> Vec<ColumnDesc> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnDesc::named(f.name.clone(), ColumnId::new(i as i32), f.data_type.clone())
            })
            .collect()
    }
}

impl GenericPlanNode for FileScanBackend {
    fn schema(&self) -> Schema {
        match self {
            FileScanBackend::FileScan(file_scan) => file_scan.schema(),
            FileScanBackend::GcsFileScan(gcs_file_scan) => gcs_file_scan.schema(),

            FileScanBackend::AzblobFileScan(azblob_file_scan) => azblob_file_scan.schema(),
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        match self {
            FileScanBackend::FileScan(file_scan) => file_scan.stream_key(),
            FileScanBackend::GcsFileScan(gcs_file_scan) => gcs_file_scan.stream_key(),

            FileScanBackend::AzblobFileScan(azblob_file_scan) => azblob_file_scan.stream_key(),
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        match self {
            FileScanBackend::FileScan(file_scan) => file_scan.ctx(),
            FileScanBackend::GcsFileScan(gcs_file_scan) => gcs_file_scan.ctx(),

            FileScanBackend::AzblobFileScan(azblob_file_scan) => azblob_file_scan.ctx(),
        }
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        match self {
            FileScanBackend::FileScan(file_scan) => file_scan.functional_dependency(),
            FileScanBackend::GcsFileScan(gcs_file_scan) => gcs_file_scan.functional_dependency(),

            FileScanBackend::AzblobFileScan(azblob_file_scan) => {
                azblob_file_scan.functional_dependency()
            }
        }
    }
}

impl FileScanBackend {
    pub fn file_location(&self) -> Vec<String> {
        match self {
            FileScanBackend::FileScan(file_scan) => file_scan.file_location.clone(),
            FileScanBackend::GcsFileScan(gcs_file_scan) => gcs_file_scan.file_location.clone(),

            FileScanBackend::AzblobFileScan(azblob_file_scan) => {
                azblob_file_scan.file_location.clone()
            }
        }
    }
}
