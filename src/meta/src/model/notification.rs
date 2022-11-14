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

use crate::storage::{MetaStore, MetaStoreError, MetaStoreResult, DEFAULT_COLUMN_FAMILY};

/// `NotificationVersion` records the last sent notification version, this will be stored
/// persistently to meta store.
pub struct NotificationVersion(u64);

const NOTIFICATION_VERSION_KEY: &[u8] = b"notification_version";

impl NotificationVersion {
    pub async fn new<S>(store: &S) -> Self
    where
        S: MetaStore,
    {
        let version = match store
            .get_cf(DEFAULT_COLUMN_FAMILY, NOTIFICATION_VERSION_KEY)
            .await
        {
            Ok(byte_vec) => u64::from_be_bytes(byte_vec.as_slice().try_into().unwrap()),
            Err(MetaStoreError::ItemNotFound(_)) => 0,
            Err(e) => panic!("{:?}", e),
        };
        Self(version)
    }

    pub async fn increase_version<S>(&mut self, store: &S) -> MetaStoreResult<()>
    where
        S: MetaStore,
    {
        let version = self.0 + 1;
        store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                NOTIFICATION_VERSION_KEY.to_vec(),
                version.to_be_bytes().to_vec(),
            )
            .await?;
        self.0 = version;
        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.0
    }
}
