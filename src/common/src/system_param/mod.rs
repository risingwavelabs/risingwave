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

use std::fmt::Debug;
use std::ops::RangeBounds;

use paste::paste;
use risingwave_pb::meta::SystemParams;

pub type SystemParamsError = String;

type Result<T> = core::result::Result<T, SystemParamsError>;

/// Only includes undeprecated params.
/// Macro input is { field identifier, type, default value }
///
/// Note:
/// - Having `None` as default value means the parameter must be initialized.
#[macro_export]
macro_rules! for_all_undeprecated_params {
    ($macro:ident
        // Hack: match trailing fields to implement `for_all_params`
        $(, { $field:ident, $type:ty, $default:expr })*) => {
        $macro! {
            { barrier_interval_ms, u32, Some(1000_u32) },
            { checkpoint_frequency, u64, Some(10_u64) },
            { sstable_size_mb, u32, Some(256_u32) },
            { block_size_kb, u32, Some(64_u32) },
            { bloom_false_positive, f64, Some(0.001_f64) },
            { state_store, String, None },
            { data_directory, String, None },
            { backup_storage_url, String, Some("memory".to_string()) },
            { backup_storage_directory, String, Some("backup".to_string()) },
            { telemetry_enabled, bool, Some(true) },
            $({ $field, $type, $default },)*
        }
    };
}

/// Includes all params.
/// Macro input is same as `for_all_undeprecated_params`.
macro_rules! for_all_params {
    ($macro:ident) => {
        for_all_undeprecated_params!(
            $macro /* Define future deprecated params here, such as
                    * ,{ backup_storage_directory, String, "backup".to_string() } */
        );
    };
}

/// Convert field name to string.
#[macro_export]
macro_rules! key_of {
    ($field:ident) => {
        stringify!($field)
    };
}

/// Define key constants for fields in `SystemParams` for use of other modules.
macro_rules! def_key {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        paste! {
            $(
                pub const [<$field:upper _KEY>]: &str = key_of!($field);
            )*
        }
    };
}

for_all_params!(def_key);

/// Define default value functions.
macro_rules! def_default {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        pub mod default {
            $(
                pub fn $field() -> Option<$type> {
                    $default
                }
            )*
        }
    };
}

for_all_undeprecated_params!(def_default);

/// Derive serialization to kv pairs.
macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        /// The returned map only contains undeprecated fields.
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

macro_rules! impl_derive_missing_fields {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        fn derive_missing_fields(params: &mut SystemParams) {
            $(
                if params.$field.is_none() && let Some(v) = OverrideFromParams::$field(params) {
                    params.$field = Some(v);
                }
            )*
        }
    };
}

/// Derive deserialization from kv pairs.
macro_rules! impl_system_params_from_kv {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        /// Try to deserialize deprecated fields as well.
        /// Return error if there are unrecognized fields.
        pub fn system_params_from_kv<K, V>(mut kvs: Vec<(K, V)>) -> Result<SystemParams>
        where
            K: AsRef<[u8]> + Debug,
            V: AsRef<[u8]> + Debug,
        {
            let mut ret = SystemParams::default();
            kvs.retain(|(k,v)| {
                let k = std::str::from_utf8(k.as_ref()).unwrap();
                let v = std::str::from_utf8(v.as_ref()).unwrap();
                match k {
                    $(
                        key_of!($field) => {
                            ret.$field = Some(v.parse().unwrap());
                            false
                        }
                    )*
                    _ => {
                        true
                    }
                }
            });
            derive_missing_fields(&mut ret);
            if !kvs.is_empty() {
                Err(format!("unrecognized system params {:?}", kvs))
            } else {
                Ok(ret)
            }
        }
    };
}

/// Define check rules when a field is changed. By default all fields are immutable.
/// If you want custom rules, please override the default implementation in
/// `OverrideValidateOnSet` below.
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

/// Define rules to derive a parameter from others. This is useful for parameter type change or
/// semantic change, where a new parameter has to be introduced. When the cluster upgrades to a
/// newer version, we need to ensure the effect of the new parameter is equal to its older versions.
/// For example, if you had `interval_sec` and now you want finer granularity, you can introduce a
/// new param `interval_ms` and try to derive it from `interval_sec` by overriding `FromParams`
/// trait in `OverrideFromParams`:
///
/// ```ignore
/// impl FromParams for OverrideFromParams {
///     fn interval_ms(params: &SystemParams) -> Option<u64> {
///         if let Some(sec) = params.interval_sec {
///             Some(sec * 1000)
///         } else {
///             None
///         }
///     }
/// }
/// ```
///
/// Note that newer versions must be prioritized during derivation.
macro_rules! impl_default_from_other_params {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        trait FromParams {
            $(
                fn $field(_params: &SystemParams) -> Option<$type> {
                    None
                }
            )*
        }
    };
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
                            $default.ok_or(format!("{} does not have a default value", key))?
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

macro_rules! impl_system_params_for_test {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        #[allow(clippy::needless_update)]
        pub fn system_params_for_test() -> SystemParams {
            let mut ret = SystemParams {
                $(
                    $field: $default,
                )*
                ..Default::default() // `None` for deprecated params
            };
            ret.data_directory = Some("hummock_001".to_string());
            ret.state_store = Some("hummock+memory".to_string());
            ret
        }
    };
}

for_all_undeprecated_params!(impl_derive_missing_fields);
for_all_params!(impl_system_params_from_kv);
for_all_undeprecated_params!(impl_system_params_to_kv);
for_all_undeprecated_params!(impl_set_system_param);
for_all_undeprecated_params!(impl_default_validation_on_set);
for_all_undeprecated_params!(impl_system_params_for_test);

struct OverrideValidateOnSet;
impl ValidateOnSet for OverrideValidateOnSet {
    fn checkpoint_frequency(v: &u64) -> Result<()> {
        Self::expect_range(*v, 1..)
    }

    fn backup_storage_directory(_v: &String) -> Result<()> {
        // TODO
        Ok(())
    }

    fn backup_storage_url(_v: &String) -> Result<()> {
        // TODO
        Ok(())
    }

    fn telemetry_enabled(_: &bool) -> Result<()> {
        Ok(())
    }
}

for_all_undeprecated_params!(impl_default_from_other_params);

struct OverrideFromParams;
impl FromParams for OverrideFromParams {}

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
            (TELEMETRY_ENABLED_KEY, "false"),
        ];

        // To kv - missing field.
        let p = SystemParams::default();
        assert!(system_params_to_kv(&p).is_err());

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
        assert!(
            set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("-1".to_string())).is_err()
        );
        // Set immutable.
        assert!(set_system_param(&mut p, STATE_STORE_KEY, Some("?".to_string())).is_err());
        // Parse error.
        assert!(set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("?".to_string())).is_err());
        // Normal set.
        assert!(
            set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("500".to_string())).is_ok()
        );
        assert_eq!(p.checkpoint_frequency, Some(500));
    }
}
