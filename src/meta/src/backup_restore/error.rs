// Copyright 2023 RisingWave Labs
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

use risingwave_backup::error::BackupError;

use crate::model::MetadataModelError;
use crate::storage::MetaStoreError;
use crate::MetaError;

impl From<BackupError> for MetaError {
    fn from(e: BackupError) -> Self {
        anyhow::anyhow!(e).into()
    }
}

impl From<MetaStoreError> for BackupError {
    fn from(e: MetaStoreError) -> Self {
        BackupError::MetaStorage(e.into())
    }
}

impl From<MetaError> for BackupError {
    fn from(e: MetaError) -> Self {
        BackupError::Other(anyhow::anyhow!(e))
    }
}

impl From<MetadataModelError> for BackupError {
    fn from(e: MetadataModelError) -> Self {
        BackupError::Decoding(e.into())
    }
}
