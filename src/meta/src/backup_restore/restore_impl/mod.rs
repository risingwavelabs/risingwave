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

use risingwave_backup::error::BackupResult;
use risingwave_backup::meta_snapshot::{MetaSnapshot, Metadata};
use risingwave_backup::MetaSnapshotId;

pub mod v1;
pub mod v2;

/// `Loader` gets, validates and amends `MetaSnapshot`.
#[async_trait::async_trait]
pub trait Loader<S: Metadata> {
    async fn load(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>>;
}

/// `Writer` writes `MetaSnapshot` to meta store.
#[async_trait::async_trait]
pub trait Writer<S: Metadata> {
    async fn write(&self, s: MetaSnapshot<S>) -> BackupResult<()>;
}
