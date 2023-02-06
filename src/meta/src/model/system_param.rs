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

use super::{MetadataModel, MetadataModelError, MetadataModelResult};
use crate::storage::{MetaStore, Snapshot, Transaction};

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

#[async_trait]
impl MetadataModel for SystemParams {
    type KeyType = u32;
    type ProstType = SystemParams;

    fn cf_name() -> String {
        SYSTEM_PARAM_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        // System params are stored as raw kv pairs.
        unimplemented!()
    }

    fn from_protobuf(_prost: Self::ProstType) -> Self {
        // System params are stored as raw kv pairs.
        unimplemented!()
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        // System params are stored as raw kv pairs.
        unimplemented!()
    }

    async fn list<S>(store: &S) -> MetadataModelResult<Vec<Self>>
    where
        S: MetaStore,
    {
        let kvs = store.list_cf(&Self::cf_name()).await?;
        if kvs.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![system_param_from_kv(kvs)?])
        }
    }

    async fn list_at_snapshot<S>(snapshot: &S::Snapshot) -> MetadataModelResult<Vec<Self>>
    where
        S: MetaStore,
    {
        let kvs = snapshot.list_cf(&Self::cf_name()).await?;
        if kvs.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![system_param_from_kv(kvs)?])
        }
    }

    async fn insert<S>(&self, store: &S) -> MetadataModelResult<()>
    where
        S: MetaStore,
    {
        let mut txn = Transaction::default();
        txn.put(
            Self::cf_name(),
            BARRIER_INTERVAL_MS_KEY.as_bytes().to_vec(),
            self.barrier_interval_ms.to_string().into_bytes(),
        );
        txn.put(
            Self::cf_name(),
            CHECKPOINT_FREQUENCY_KEY.as_bytes().to_vec(),
            self.checkpoint_frequency.to_string().into_bytes(),
        );
        txn.put(
            Self::cf_name(),
            SSTABLE_SIZE_MB_KEY.as_bytes().to_vec(),
            self.sstable_size_mb.to_string().into_bytes(),
        );
        txn.put(
            Self::cf_name(),
            BLOCK_SIZE_KB_KEY.as_bytes().to_vec(),
            self.block_size_kb.to_string().into_bytes(),
        );
        txn.put(
            Self::cf_name(),
            BLOOM_FALSE_POSITIVE_KEY.as_bytes().to_vec(),
            self.bloom_false_positive.to_string().into_bytes(),
        );
        txn.put(
            Self::cf_name(),
            STATE_STORE_KEY.as_bytes().to_vec(),
            self.state_store.as_bytes().to_vec(),
        );
        txn.put(
            Self::cf_name(),
            DATA_DIRECTORY_KEY.as_bytes().to_vec(),
            self.data_directory.as_bytes().to_vec(),
        );
        txn.put(
            Self::cf_name(),
            BACKUP_STORAGE_URL_KEY.as_bytes().to_vec(),
            self.backup_storage_url.as_bytes().to_vec(),
        );
        txn.put(
            Self::cf_name(),
            BACKUP_STORAGE_DIRECTORY_KEY.as_bytes().to_vec(),
            self.backup_storage_directory.as_bytes().to_vec(),
        );
        Ok(store.txn(txn).await?)
    }

    async fn delete<S>(_store: &S, _key: &Self::KeyType) -> MetadataModelResult<()>
    where
        S: MetaStore,
    {
        unimplemented!()
    }
}

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
            BARRIER_INTERVAL_MS_KEY => ret.barrier_interval_ms = v.parse().unwrap(),
            CHECKPOINT_FREQUENCY_KEY => ret.checkpoint_frequency = v.parse().unwrap(),
            SSTABLE_SIZE_MB_KEY => ret.sstable_size_mb = v.parse().unwrap(),
            BLOCK_SIZE_KB_KEY => ret.block_size_kb = v.parse().unwrap(),
            BLOOM_FALSE_POSITIVE_KEY => ret.bloom_false_positive = v.parse().unwrap(),
            STATE_STORE_KEY => ret.state_store = v,
            DATA_DIRECTORY_KEY => ret.data_directory = v,
            BACKUP_STORAGE_URL_KEY => ret.backup_storage_url = v,
            BACKUP_STORAGE_DIRECTORY_KEY => ret.backup_storage_directory = v,
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
