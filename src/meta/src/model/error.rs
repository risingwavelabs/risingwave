// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::ProstFieldNotFound;
use thiserror::Error;

use crate::storage::MetaStoreError;

pub type MetadataModelResult<T> = std::result::Result<T, MetadataModelError>;

#[derive(Error, Debug)]
pub enum MetadataModelError {
    #[error("Meta store error: {0}")]
    MetaStoreError(#[from] MetaStoreError),

    #[error("Prost decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error(transparent)]
    InternalError(anyhow::Error),
}

impl From<ProstFieldNotFound> for MetadataModelError {
    fn from(p: ProstFieldNotFound) -> Self {
        MetadataModelError::InternalError(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            p.0
        ))
    }
}

impl From<MetadataModelError> for tonic::Status {
    fn from(e: MetadataModelError) -> Self {
        tonic::Status::new(tonic::Code::Internal, format!("{}", e))
    }
}
