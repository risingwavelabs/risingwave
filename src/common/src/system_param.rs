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
    ($({ $field:ident, $key:path },)*) => {
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

macro_rules! impl_system_params_from_kv {
    ($({ $field:ident, $key:path },)*) => {
        /// For each field in `SystemParams`, one of these rules apply:
        /// - Up-to-date: Guaranteed to be `Some`. If it is not present, may try to derive it from previous
        ///   versions of this field.
        /// - Deprecated: Guaranteed to be `None`.
        /// - Unrecognized: Not allowed.
        pub fn system_params_from_kv(kvs: Vec<(impl AsRef<[u8]>, impl AsRef<[u8]>)>) -> Result<SystemParams> {
            let mut ret = SystemParams::default();
            let mut expected_keys: HashSet<_> = [
                $($key,)*
            ]
            .iter()
            .cloned()
            .collect();
            for (k, v) in kvs {
                let k = std::str::from_utf8(k.as_ref()).unwrap();
                let v = std::str::from_utf8(v.as_ref()).unwrap();
                match k {
                    $(
                        $key => ret.$field = Some(v.parse().unwrap()),
                    )*
                    _ => {
                        return Err(ErrorCode::SystemParamsError(format!(
                            "unrecognized system param {:?}",
                            k
                        ))
                        .into());
                    }
                }
                expected_keys.remove(k);
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
    };
}

for_all_undeprecated_params!(impl_system_params_from_kv);

for_all_undeprecated_params!(impl_system_params_to_kv);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_from_kv() {
        // Include all fields (deprecated also).
        let kvs = vec![
            (BARRIER_INTERVAL_MS_KEY, "1"),
            (CHECKPOINT_FREQUENCY_KEY, "1"),
            (SSTABLE_SIZE_MB_KEY, "1"),
            (BLOCK_SIZE_KB_KEY, "1"),
            (BLOOM_FALSE_POSITIVE_KEY, "1"),
            (STATE_STORE_KEY, "a"),
            (DATA_DIRECTORY_KEY, "a"),
            (BACKUP_STORAGE_URL_KEY, "a"),
            (BACKUP_STORAGE_DIRECTORY_KEY, "a"),
        ];

        // To kv - missing field.
        let p = SystemParams::default();
        assert!(system_params_to_kv(&p).is_err());

        // From kv - missing field.
        assert!(system_params_from_kv(vec![(BARRIER_INTERVAL_MS_KEY, "1")]).is_err());

        // From kv - unrecognized field.
        assert!(system_params_from_kv(vec![("?", "?")]).is_err());

        // Deser & ser.
        let p = system_params_from_kv(kvs).unwrap();
        assert_eq!(
            p,
            system_params_from_kv(system_params_to_kv(&p).unwrap()).unwrap()
        );
    }
}
