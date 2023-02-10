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

use std::collections::HashSet;

use async_trait::async_trait;
use risingwave_pb::meta::SystemParams;

use crate::model::{MetadataModelError, MetadataModelResult};
use crate::storage::{MetaStore, Transaction};

const SYSTEM_PARAM_CF_NAME: &str = "cf/system_params";

const BARRIER_INTERVAL_MS_KEY: &str = "barrier_interval_ms";
const CHECKPOINT_FREQUENCY_KEY: &str = "checkpoint_interval";
const SSTABLE_SIZE_MB_KEY: &str = "sstable_size_mb";
const BLOCK_SIZE_KB_KEY: &str = "block_size_kb";
const BLOOM_FALSE_POSITIVE_KEY: &str = "bloom_false_positive";
const STATE_STORE_KEY: &str = "state_store";
const DATA_DIRECTORY_KEY: &str = "data_directory";
const BACKUP_STORAGE_URL_KEY: &str = "backup_storage_url";
const BACKUP_STORAGE_DIRECTORY_KEY: &str = "backup_storage_directory";

// A dummy trait to implement custom methods on `SystemParams`.
#[async_trait]
pub trait KvSingletonModel: Sized {
    fn cf_name() -> String;
    async fn get<S: MetaStore>(store: &S) -> MetadataModelResult<Option<Self>>;
    async fn insert<S: MetaStore>(&self, store: &S) -> MetadataModelResult<()>;
}

#[async_trait]
impl KvSingletonModel for SystemParams {
    fn cf_name() -> String {
        SYSTEM_PARAM_CF_NAME.to_string()
    }

    /// The undeprecated fields are guaranteed to be `Some`.
    async fn get<S>(store: &S) -> MetadataModelResult<Option<Self>>
    where
        S: MetaStore,
    {
        let kvs = store.list_cf(&Self::cf_name()).await?;
        if kvs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(system_param_from_kv(kvs)?))
        }
    }

    async fn insert<S>(&self, store: &S) -> MetadataModelResult<()>
    where
        S: MetaStore,
    {
        let mut txn = Transaction::default();
        self.barrier_interval_ms.inspect(|v| {
            txn.put(
                Self::cf_name(),
                BARRIER_INTERVAL_MS_KEY.as_bytes().to_vec(),
                v.to_string().into_bytes(),
            );
        });
        self.checkpoint_frequency.inspect(|v| {
            txn.put(
                Self::cf_name(),
                CHECKPOINT_FREQUENCY_KEY.as_bytes().to_vec(),
                v.to_string().into_bytes(),
            );
        });
        self.sstable_size_mb.inspect(|v| {
            txn.put(
                Self::cf_name(),
                SSTABLE_SIZE_MB_KEY.as_bytes().to_vec(),
                v.to_string().into_bytes(),
            );
        });
        self.block_size_kb.inspect(|v| {
            txn.put(
                Self::cf_name(),
                BLOCK_SIZE_KB_KEY.as_bytes().to_vec(),
                v.to_string().into_bytes(),
            );
        });
        self.bloom_false_positive.inspect(|v| {
            txn.put(
                Self::cf_name(),
                BLOOM_FALSE_POSITIVE_KEY.as_bytes().to_vec(),
                v.to_string().into_bytes(),
            );
        });
        self.state_store.as_ref().inspect(|v| {
            txn.put(
                Self::cf_name(),
                STATE_STORE_KEY.as_bytes().to_vec(),
                v.as_bytes().to_vec(),
            );
        });
        self.data_directory.as_ref().inspect(|v| {
            txn.put(
                Self::cf_name(),
                DATA_DIRECTORY_KEY.as_bytes().to_vec(),
                v.as_bytes().to_vec(),
            );
        });
        self.backup_storage_url.as_ref().inspect(|v| {
            txn.put(
                Self::cf_name(),
                BACKUP_STORAGE_URL_KEY.as_bytes().to_vec(),
                v.as_bytes().to_vec(),
            );
        });
        self.backup_storage_directory.as_ref().inspect(|v| {
            txn.put(
                Self::cf_name(),
                BACKUP_STORAGE_DIRECTORY_KEY.as_bytes().to_vec(),
                v.as_bytes().to_vec(),
            );
        });
        Ok(store.txn(txn).await?)
    }
}

/// For each field in `SystemParams`, one of these rules apply:
/// - Up-to-date: Required. If it is not present, may try to derive it from previous versions of
///   this field.
/// - Deprecated: Optional.
/// - Unrecognized: Not allowed.
fn system_param_from_kv(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> MetadataModelResult<SystemParams> {
    let mut ret = SystemParams::default();
    let mut expected_keys: HashSet<_> = [
        BARRIER_INTERVAL_MS_KEY,
        CHECKPOINT_FREQUENCY_KEY,
        SSTABLE_SIZE_MB_KEY,
        BLOCK_SIZE_KB_KEY,
        BLOOM_FALSE_POSITIVE_KEY,
        STATE_STORE_KEY,
        DATA_DIRECTORY_KEY,
        BACKUP_STORAGE_URL_KEY,
        BACKUP_STORAGE_DIRECTORY_KEY,
    ]
    .iter()
    .cloned()
    .collect();
    for (k, v) in kvs {
        let k = String::from_utf8(k).unwrap();
        let v = String::from_utf8(v).unwrap();
        match k.as_str() {
            BARRIER_INTERVAL_MS_KEY => ret.barrier_interval_ms = Some(v.parse().unwrap()),
            CHECKPOINT_FREQUENCY_KEY => ret.checkpoint_frequency = Some(v.parse().unwrap()),
            SSTABLE_SIZE_MB_KEY => ret.sstable_size_mb = Some(v.parse().unwrap()),
            BLOCK_SIZE_KB_KEY => ret.block_size_kb = Some(v.parse().unwrap()),
            BLOOM_FALSE_POSITIVE_KEY => ret.bloom_false_positive = Some(v.parse().unwrap()),
            STATE_STORE_KEY => ret.state_store = Some(v),
            DATA_DIRECTORY_KEY => ret.data_directory = Some(v),
            BACKUP_STORAGE_URL_KEY => ret.backup_storage_url = Some(v),
            BACKUP_STORAGE_DIRECTORY_KEY => ret.backup_storage_directory = Some(v),
            _ => {
                return Err(MetadataModelError::internal(format!(
                    "unrecognized system param {:?}",
                    k
                )));
            }
        }
        expected_keys.remove(k.as_str());
    }
    if !expected_keys.is_empty() {
        return Err(MetadataModelError::internal(format!(
            "missing system param {:?}",
            expected_keys
        )));
    }
    Ok(ret)
}
