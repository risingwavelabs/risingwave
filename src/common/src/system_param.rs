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

use risingwave_pb::meta::SystemParams;

use crate::error::{ErrorCode, RwError};

pub const BARRIER_INTERVAL_MS_KEY: &str = "barrier_interval_ms";
pub const CHECKPOINT_FREQUENCY_KEY: &str = "checkpoint_interval";
pub const SSTABLE_SIZE_MB_KEY: &str = "sstable_size_mb";
pub const BLOCK_SIZE_KB_KEY: &str = "block_size_kb";
pub const BLOOM_FALSE_POSITIVE_KEY: &str = "bloom_false_positive";
pub const STATE_STORE_KEY: &str = "state_store";
pub const DATA_DIRECTORY_KEY: &str = "data_directory";
pub const BACKUP_STORAGE_URL_KEY: &str = "backup_storage_url";
pub const BACKUP_STORAGE_DIRECTORY_KEY: &str = "backup_storage_directory";

type Result<T> = core::result::Result<T, RwError>;

macro_rules! for_all_undeprecated_params {
    ($macro:ident) => {
        $macro! {
            { barrier_interval_ms, BARRIER_INTERVAL_MS_KEY },
            { checkpoint_frequency, CHECKPOINT_FREQUENCY_KEY },
            { sstable_size_mb, SSTABLE_SIZE_MB_KEY },
            { block_size_kb, BLOCK_SIZE_KB_KEY },
            { bloom_false_positive, BLOOM_FALSE_POSITIVE_KEY },
            { state_store, STATE_STORE_KEY },
            { data_directory, DATA_DIRECTORY_KEY },
            { backup_storage_url, BACKUP_STORAGE_URL_KEY },
            { backup_storage_directory, BACKUP_STORAGE_DIRECTORY_KEY },
        }
    };
}

macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $key:expr },)*) => {
        /// All undeprecated fields are guaranteed to be contained in the returned map.
        /// Return error if there are missing fields.
        pub fn system_params_to_kv(params: &SystemParams) -> Result<Vec<(String, String)>> {
            let mut ret = Vec::with_capacity(9);
            $(ret.push((
                $key.to_string(),
                params
                    .$field.as_ref()
                    .ok_or::<RwError>(ErrorCode::SystemParamsError(format!(
                        "missing system param {:?}",
                        $key
                    )).into())?
                    .to_string(),
            ));)*
            Ok(ret)
        }
    };
}

/// For each field in `SystemParams`, one of these rules apply:
/// - Up-to-date: Required. If it is not present, may try to derive it from previous versions of
///   this field.
/// - Deprecated: Optional.
/// - Unrecognized: Not allowed.
pub fn system_params_from_kv(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<SystemParams> {
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
                return Err(ErrorCode::SystemParamsError(format!(
                    "unrecognized system param {:?}",
                    k
                ))
                .into());
            }
        }
        expected_keys.remove(k.as_str());
    }
    if !expected_keys.is_empty() {
        return Err(ErrorCode::SystemParamsError(format!(
            "missing system param {:?}",
            expected_keys
        ))
        .into());
    }
    Ok(ret)
}

for_all_undeprecated_params!(impl_system_params_to_kv);
