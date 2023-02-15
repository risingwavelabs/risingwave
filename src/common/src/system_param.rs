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

use paste::paste;
use risingwave_pb::meta::SystemParams;

pub type SystemParamsError = String;

type Result<T> = core::result::Result<T, SystemParamsError>;

#[allow(dead_code)]
const MUTABLE: bool = true;
#[allow(dead_code)]
const IMMUTABLE: bool = false;

// Only includes undeprecated params.
// Macro input is { field identifier, mutability }
macro_rules! for_all_undeprecated_params {
    ($macro:ident) => {
        $macro! {
            { barrier_interval_ms, MUTABLE },
            { checkpoint_frequency, MUTABLE },
            { sstable_size_mb, IMMUTABLE },
            { block_size_kb, IMMUTABLE },
            { bloom_false_positive, IMMUTABLE },
            { state_store, IMMUTABLE },
            { data_directory, IMMUTABLE },
            { backup_storage_url, IMMUTABLE },
            { backup_storage_directory, IMMUTABLE },
        }
    };
}

// Only includes deprecated params. Used to define key constants.
// Macro input is { field identifier, mutability }
macro_rules! for_all_deprecated_params {
    ($macro:ident) => {
        $macro! {}
    };
}

/// Convert field name to string.
macro_rules! key_of {
    ($field:ident) => {
        stringify!($field)
    };
}

/// Define key constants for fields in `SystemParams` for use of other modules.
macro_rules! def_key {
    ($({ $field:ident, $_:expr },)*) => {
        paste! {
            $(
                pub const [<$field:upper _KEY>]: &str = key_of!($field);
            )*
        }

    };
}

for_all_undeprecated_params!(def_key);
for_all_deprecated_params!(def_key);

macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $_:expr },)*) => {
        /// All undeprecated fields are guaranteed to be contained in the returned map.
        /// Return error if there are missing fields.
        pub fn system_params_to_kv(params: &SystemParams) -> Result<Vec<(String, String)>> {
            let mut ret = Vec::with_capacity(9);
            $(ret.push((
                key_of!($field).to_string(),
                params
                    .$field.as_ref()
                    .ok_or_else(||format!(
                        "missing system param {:?}",
                        key_of!($field)
                    ))?
                    .to_string(),
            ));)*
            Ok(ret)
        }
    };
}

macro_rules! impl_system_params_from_kv {
    ($({ $field:ident, $_:expr },)*) => {
        /// For each field in `SystemParams`, one of these rules apply:
        /// - Up-to-date: Guaranteed to be `Some`. If it is not present, may try to derive it from previous
        ///   versions of this field.
        /// - Deprecated: Guaranteed to be `None`.
        /// - Unrecognized: Not allowed.
        pub fn system_params_from_kv(kvs: Vec<(impl AsRef<[u8]>, impl AsRef<[u8]>)>) -> Result<SystemParams> {
            let mut ret = SystemParams::default();
            let mut expected_keys: HashSet<_> = [
                $(key_of!($field),)*
            ]
            .iter()
            .cloned()
            .collect();
            for (k, v) in kvs {
                let k = std::str::from_utf8(k.as_ref()).unwrap();
                let v = std::str::from_utf8(v.as_ref()).unwrap();
                match k {
                    $(
                        key_of!($field) => ret.$field = Some(v.parse().unwrap()),
                    )*
                    _ => {
                        return Err(format!(
                            "unrecognized system param {:?}",
                            k
                        ));
                    }
                }
                expected_keys.remove(k);
            }
            if !expected_keys.is_empty() {
                return Err(format!(
                    "missing system param {:?}",
                    expected_keys
                ));
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
