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

// Only includes undeprecated params.
// Macro input is { field identifier, default value }
macro_rules! for_all_undeprecated_params {
    ($macro:ident) => {
        $macro! {
            { barrier_interval_ms, 1000_u32 },
            { checkpoint_frequency, 10_u64 },
            { sstable_size_mb, 256_u32 },
            { block_size_kb, 64_u32 },
            { bloom_false_positive, 0.001_f64 },
            { state_store, "hummock+memory".to_string() },
            { data_directory, "hummock_001".to_string() },
            { backup_storage_url, "memory".to_string() },
            { backup_storage_directory, "backup".to_string() },
        }
    };
}

// Only includes deprecated params. Used to define key constants.
// Macro input is { field identifier, mutability, default value }
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
    ($({ $field:ident, $default:expr },)*) => {
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
    ($({ $field:ident, $default:expr },)*) => {
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
    ($({ $field:ident, $default:expr },)*) => {
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

macro_rules! impl_set_system_param {
    ($({ $field:ident, $default:expr },)*) => {
        pub fn set_system_param(params: &mut SystemParams, key: &str, value: Option<String>) -> Result<()> {
             match key {
                $(
                    key_of!($field) => {
                        let v = if let Some(v) = value {
                            v.parse().map_err(|_| RwError::from(ErrorCode::SystemParamsError(format!("cannot parse parameter value"))))?
                        } else {
                            $default
                        };
                        sanity_check::$field(&v)?;
                        params.$field = Some(v);
                    },
                )*
                _ => {
                    return Err(ErrorCode::SystemParamsError(format!(
                        "unrecognized system param {:?}",
                        key
                    ))
                    .into());
                }
            };
            Ok(())
        }
    };
}

for_all_undeprecated_params!(impl_system_params_from_kv);

for_all_undeprecated_params!(impl_system_params_to_kv);

for_all_undeprecated_params!(impl_set_system_param);

/// Check the validity of set values.
mod sanity_check {
    use std::fmt::Debug;
    use std::ops::{Bound, RangeBounds};

    use super::*;

    pub fn barrier_interval_ms(v: &u32) -> Result<()> {
        expect_range(*v, (Bound::Excluded(0), Bound::Unbounded))
    }

    pub fn checkpoint_frequency(v: &u64) -> Result<()> {
        expect_range(*v, (Bound::Excluded(0), Bound::Unbounded))
    }

    pub fn sstable_size_mb(_: &u32) -> Result<()> {
        expect_immutable(SSTABLE_SIZE_MB_KEY)
    }

    pub fn block_size_kb(_: &u32) -> Result<()> {
        expect_immutable(BLOCK_SIZE_KB_KEY)
    }

    pub fn bloom_false_positive(_: &f64) -> Result<()> {
        expect_immutable(BLOOM_FALSE_POSITIVE_KEY)
    }

    pub fn state_store(_: &str) -> Result<()> {
        expect_immutable(STATE_STORE_KEY)
    }

    pub fn data_directory(_: &str) -> Result<()> {
        expect_immutable(DATA_DIRECTORY_KEY)
    }

    pub fn backup_storage_url(_: &str) -> Result<()> {
        expect_immutable(BACKUP_STORAGE_URL_KEY)
    }

    pub fn backup_storage_directory(_: &str) -> Result<()> {
        expect_immutable(BACKUP_STORAGE_DIRECTORY_KEY)
    }

    fn expect_immutable(field: &str) -> Result<()> {
        Err(ErrorCode::SystemParamsError(format!("{:?} is immutable", field)).into())
    }

    fn expect_range<T, R>(v: T, range: R) -> Result<()>
    where
        T: Debug + PartialOrd,
        R: RangeBounds<T> + Debug,
    {
        if !range.contains::<T>(&v) {
            Err(ErrorCode::SystemParamsError(format!(
                "value {:?} out of range, expect {:?}",
                v, range
            ))
            .into())
        } else {
            Ok(())
        }
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
}
