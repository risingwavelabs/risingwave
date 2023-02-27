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

pub mod local_manager;
pub mod reader;

use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::RangeBounds;

use paste::paste;
use risingwave_pb::meta::SystemParams;

pub type SystemParamsError = String;

type Result<T> = core::result::Result<T, SystemParamsError>;

// Only includes undeprecated params.
// Macro input is { field identifier, default value }
macro_rules! for_all_undeprecated_params {
    ($macro:ident) => {
        $macro! {
            { barrier_interval_ms, u32, 1000_u32 },
            { checkpoint_frequency, u64, 10_u64 },
            { sstable_size_mb, u32, 256_u32 },
            { block_size_kb, u32, 64_u32 },
            { bloom_false_positive, f64, 0.001_f64 },
            { state_store, String, "hummock+memory".to_string() },
            { data_directory, String, "hummock_001".to_string() },
            { backup_storage_url, String, "memory".to_string() },
            { backup_storage_directory, String, "backup".to_string() },
        }
    };
}

// Only includes deprecated params. Used to define key constants.
// Macro input is { field identifier, default value }
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

// Define key constants for fields in `SystemParams` for use of other modules.
macro_rules! def_key {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        paste! {
            $(
                pub const [<$field:upper _KEY>]: &str = key_of!($field);
            )*
        }
    };
}

for_all_undeprecated_params!(def_key);
for_all_deprecated_params!(def_key);

// Derive serialization to kv pairs.
macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
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

// Derive deserialization from kv pairs.
macro_rules! impl_system_params_from_kv {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
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

// Define check rules when a field is changed. By default all fields are immutable.
// If you want custom rules, please override the default implementation in
// `OverrideValidateOnSet` below.
macro_rules! impl_default_validation_on_set {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        #[allow(clippy::ptr_arg)]
        trait ValidateOnSet {
            $(
                fn $field(_v: &$type) -> Result<()> {
                    Self::expect_immutable(key_of!($field))
                }
            )*

            fn expect_immutable(field: &str) -> Result<()> {
                Err(format!("{:?} is immutable", field))
            }

            fn expect_range<T, R>(v: T, range: R) -> Result<()>
            where
                T: Debug + PartialOrd,
                R: RangeBounds<T> + Debug,
            {
                if !range.contains::<T>(&v) {
                    Err(format!("value {:?} out of range, expect {:?}", v, range))
                } else {
                    Ok(())
                }
            }
        }
    }
}

macro_rules! impl_set_system_param {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        pub fn set_system_param(params: &mut SystemParams, key: &str, value: Option<String>) -> Result<()> {
             match key {
                $(
                    key_of!($field) => {
                        let v = if let Some(v) = value {
                            v.parse().map_err(|_| format!("cannot parse parameter value"))?
                        } else {
                            $default
                        };
                        OverrideValidateOnSet::$field(&v)?;
                        params.$field = Some(v);
                    },
                )*
                _ => {
                    return Err(format!(
                        "unrecognized system param {:?}",
                        key
                    ));
                }
            };
            Ok(())
        }
    };
}

for_all_undeprecated_params!(impl_system_params_from_kv);

for_all_undeprecated_params!(impl_system_params_to_kv);

for_all_undeprecated_params!(impl_set_system_param);

for_all_undeprecated_params!(impl_default_validation_on_set);

struct OverrideValidateOnSet;
impl ValidateOnSet for OverrideValidateOnSet {
    fn barrier_interval_ms(v: &u32) -> Result<()> {
        Self::expect_range(*v, 1..)
    }

    fn checkpoint_frequency(v: &u64) -> Result<()> {
        Self::expect_range(*v, 1..)
    }
}

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

    #[test]
    fn test_set() {
        let mut p = SystemParams::default();
        // Unrecognized param.
        assert!(set_system_param(&mut p, "?", Some("?".to_string())).is_err());
        // Value out of range.
        assert!(set_system_param(&mut p, BARRIER_INTERVAL_MS_KEY, Some("-1".to_string())).is_err());
        // Set immutable.
        assert!(set_system_param(&mut p, STATE_STORE_KEY, Some("?".to_string())).is_err());
        // Parse error.
        assert!(set_system_param(&mut p, BARRIER_INTERVAL_MS_KEY, Some("?".to_string())).is_err());
        // Normal set.
        assert!(set_system_param(&mut p, BARRIER_INTERVAL_MS_KEY, Some("500".to_string())).is_ok());
        assert_eq!(p.barrier_interval_ms, Some(500));
    }
}
