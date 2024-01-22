// Copyright 2024 RisingWave Labs
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

//! This module defines utilities to work with system parameters ([`PbSystemParams`] in
//! `meta.proto`).
//!
//! To add a new system parameter:
//! - Add a new field to [`PbSystemParams`] in `meta.proto`.
//! - Add a new entry to `for_all_undeprecated_params` in this file.
//! - Add a new method to [`reader::SystemParamsReader`].

pub mod common;
pub mod diff;
pub mod local_manager;
pub mod reader;

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::str::FromStr;

use paste::paste;
use risingwave_pb::meta::PbSystemParams;

use self::diff::SystemParamsDiff;

pub type SystemParamsError = String;

type Result<T> = core::result::Result<T, SystemParamsError>;

/// The trait for the value type of a system parameter.
pub trait ParamValue: ToString + FromStr {
    type Borrowed<'a>;
}

macro_rules! impl_param_value {
    ($type:ty) => {
        impl_param_value!($type => $type);
    };
    ($type:ty => $borrowed:ty) => {
        impl ParamValue for $type {
            type Borrowed<'a> = $borrowed;
        }
    };
}

impl_param_value!(bool);
impl_param_value!(u32);
impl_param_value!(u64);
impl_param_value!(f64);
impl_param_value!(String => &'a str);

/// Define all system parameters here.
///
/// To match all these information, write the match arm as follows:
/// ```text
/// ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
/// ```
///
/// Note:
/// - Having `None` as default value means the parameter must be initialized.
#[macro_export]
macro_rules! for_all_params {
    ($macro:ident) => {
        $macro! {
            // name                                     type    default value                               mut?    doc
            { barrier_interval_ms,                      u32,    Some(1000_u32),                             true,   "The interval of periodic barrier.", },
            { checkpoint_frequency,                     u64,    Some(1_u64),                                true,   "There will be a checkpoint for every n barriers.", },
            { sstable_size_mb,                          u32,    Some(256_u32),                              false,  "Target size of the Sstable.", },
            { parallel_compact_size_mb,                 u32,    Some(512_u32),                              false,  "", },
            { block_size_kb,                            u32,    Some(64_u32),                               false,  "Size of each block in bytes in SST.", },
            { bloom_false_positive,                     f64,    Some(0.001_f64),                            false,  "False positive probability of bloom filter.", },
            { state_store,                              String, None,                                       false,  "", },
            { data_directory,                           String, None,                                       false,  "Remote directory for storing data and metadata objects.", },
            { backup_storage_url,                       String, Some("memory".to_string()),                 true,   "Remote storage url for storing snapshots.", },
            { backup_storage_directory,                 String, Some("backup".to_string()),                 true,   "Remote directory for storing snapshots.", },
            { max_concurrent_creating_streaming_jobs,   u32,    Some(1_u32),                                true,   "Max number of concurrent creating streaming jobs.", },
            { pause_on_next_bootstrap,                  bool,   Some(false),                                true,   "Whether to pause all data sources on next bootstrap.", },
            { wasm_storage_url,                         String, Some("fs://.risingwave/data".to_string()),  false,  "", },
            { enable_tracing,                           bool,   Some(false),                                true,   "Whether to enable distributed tracing.", },
        }
    };
}

/// Convert field name to string.
#[macro_export]
macro_rules! key_of {
    ($field:ident) => {
        stringify!($field)
    };
}

/// Define key constants for fields in `PbSystemParams` for use of other modules.
macro_rules! def_key {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        paste! {
            $(
                pub const [<$field:upper _KEY>]: &str = key_of!($field);
            )*
        }
    };
}

for_all_params!(def_key);

/// Define default value functions returning `Option`.
macro_rules! def_default_opt {
    ($({ $field:ident, $type:ty, $default: expr, $($rest:tt)* },)*) => {
        $(
            paste::paste!(
                pub fn [<$field _opt>]() -> Option<$type> {
                    $default
                }
            );
        )*
    };
}

/// Define default value functions for those with `Some` default values.
macro_rules! def_default {
    ($({ $field:ident, $type:ty, $default: expr, $($rest:tt)* },)*) => {
        $(
            def_default!(@ $field, $type, $default);
        )*
    };
    (@ $field:ident, $type:ty, None) => {};
    (@ $field:ident, $type:ty, $default: expr) => {
        pub fn $field() -> $type {
            $default.unwrap()
        }
        paste::paste!(
            pub static [<$field:upper>]: LazyLock<$type> = LazyLock::new($field);
        );
    };
}

/// Default values for all parameters.
pub mod default {
    use std::sync::LazyLock;

    for_all_params!(def_default_opt);
    for_all_params!(def_default);
}

macro_rules! impl_check_missing_fields {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// Check if any undeprecated fields are missing.
        pub fn check_missing_params(params: &PbSystemParams) -> Result<()> {
            $(
                if params.$field.is_none() {
                    return Err(format!("missing system param {:?}", key_of!($field)));
                }
            )*
            Ok(())
        }
    };
}

/// Derive serialization to kv pairs.
macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// The returned map only contains undeprecated fields.
        /// Return error if there are missing fields.
        #[allow(clippy::vec_init_then_push)]
        pub fn system_params_to_kv(params: &PbSystemParams) -> Result<Vec<(String, String)>> {
            check_missing_params(params)?;
            let mut ret = Vec::new();
            $(ret.push((
                key_of!($field).to_string(),
                params.$field.as_ref().unwrap().to_string(),
            ));)*
            Ok(ret)
        }
    };
}

macro_rules! impl_derive_missing_fields {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        pub fn derive_missing_fields(params: &mut PbSystemParams) {
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
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// Try to deserialize deprecated fields as well.
        /// Return error if there are unrecognized fields.
        pub fn system_params_from_kv<K, V>(mut kvs: Vec<(K, V)>) -> Result<PbSystemParams>
        where
            K: AsRef<[u8]> + Debug,
            V: AsRef<[u8]> + Debug,
        {
            let mut ret = PbSystemParams::default();
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
                let unrecognized_params = kvs.into_iter().map(|(k, v)| {
                    (
                        std::str::from_utf8(k.as_ref()).unwrap().to_string(),
                        std::str::from_utf8(v.as_ref()).unwrap().to_string()
                    )
                }).collect::<Vec<_>>();
                tracing::warn!("unrecognized system params {:?}", unrecognized_params);
            }
            Ok(ret)
        }
    };
}

/// Define check rules when a field is changed.
/// If you want custom rules, please override the default implementation in
/// `OverrideValidateOnSet` below.
macro_rules! impl_default_validation_on_set {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $($rest:tt)* },)*) => {
        #[allow(clippy::ptr_arg)]
        trait ValidateOnSet {
            $(
                fn $field(_v: &$type) -> Result<()> {
                    if !$is_mutable {
                        Err(format!("{:?} is immutable", key_of!($field)))
                    } else {
                        Ok(())
                    }
                }
            )*

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
///     fn interval_ms(params: &PbSystemParams) -> Option<u64> {
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
    ($({ $field:ident, $type:ty, $($rest:tt)* },)*) => {
        trait FromParams {
            $(
                fn $field(_params: &PbSystemParams) -> Option<$type> {
                    None
                }
            )*
        }
    };
}

macro_rules! impl_set_system_param {
    ($({ $field:ident, $type:ty, $default:expr, $($rest:tt)* },)*) => {
        /// Set a system parameter with the given value or default one.
        ///
        /// Returns the new value if changed, or an error if the parameter is unrecognized
        /// or the value is invalid.
        pub fn set_system_param(
            params: &mut PbSystemParams,
            key: &str,
            value: Option<impl AsRef<str>>,
        ) -> Result<Option<(String, SystemParamsDiff)>> {
            use crate::system_param::reader::{SystemParamsReader, SystemParamsRead};

            match key {
                $(
                    key_of!($field) => {
                        let v = if let Some(v) = value {
                            v.as_ref().parse().map_err(|_| format!("cannot parse parameter value"))?
                        } else {
                            $default.ok_or_else(|| format!("{} does not have a default value", key))?
                        };
                        OverrideValidateOnSet::$field(&v)?;

                        let changed = SystemParamsReader::new(&*params).$field() != v;
                        if changed {
                            let new_value = v.to_string();
                            let diff = SystemParamsDiff {
                                $field: Some(v.to_owned()),
                                ..Default::default()
                            };
                            params.$field = Some(v);
                            Ok(Some((new_value, diff)))
                        } else {
                            Ok(None)
                        }
                    },
                )*
                _ => {
                    Err(format!(
                        "unrecognized system param {:?}",
                        key
                    ))
                }
            }
        }
    };
}

macro_rules! impl_is_mutable {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $($rest:tt)* },)*) => {
        pub fn is_mutable(field: &str) -> Result<bool> {
            match field {
                $(
                    key_of!($field) => Ok($is_mutable),
                )*
                _ => Err(format!("{:?} is not a system parameter", field))
            }
        }
    }
}

macro_rules! impl_system_params_for_test {
    ($({ $field:ident, $type:ty, $default:expr, $($rest:tt)* },)*) => {
        #[allow(clippy::needless_update)]
        pub fn system_params_for_test() -> PbSystemParams {
            let mut ret = PbSystemParams {
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

for_all_params!(impl_system_params_from_kv);
for_all_params!(impl_is_mutable);
for_all_params!(impl_derive_missing_fields);
for_all_params!(impl_check_missing_fields);
for_all_params!(impl_system_params_to_kv);
for_all_params!(impl_set_system_param);
for_all_params!(impl_default_validation_on_set);
for_all_params!(impl_system_params_for_test);

struct OverrideValidateOnSet;
impl ValidateOnSet for OverrideValidateOnSet {
    fn barrier_interval_ms(v: &u32) -> Result<()> {
        Self::expect_range(*v, 100..)
    }

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
}

for_all_params!(impl_default_from_other_params);

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
            (PARALLEL_COMPACT_SIZE_MB_KEY, "2"),
            (BLOCK_SIZE_KB_KEY, "1"),
            (BLOOM_FALSE_POSITIVE_KEY, "1"),
            (STATE_STORE_KEY, "a"),
            (DATA_DIRECTORY_KEY, "a"),
            (BACKUP_STORAGE_URL_KEY, "a"),
            (BACKUP_STORAGE_DIRECTORY_KEY, "a"),
            (MAX_CONCURRENT_CREATING_STREAMING_JOBS_KEY, "1"),
            (PAUSE_ON_NEXT_BOOTSTRAP_KEY, "false"),
            (WASM_STORAGE_URL_KEY, "a"),
            (ENABLE_TRACING_KEY, "true"),
            ("a_deprecated_param", "foo"),
        ];

        // To kv - missing field.
        let p = PbSystemParams::default();
        assert!(system_params_to_kv(&p).is_err());

        // From kv - unrecognized field should be ignored
        assert!(system_params_from_kv(vec![("?", "?")]).is_ok());

        // Deser & ser.
        let p = system_params_from_kv(kvs).unwrap();
        assert_eq!(
            p,
            system_params_from_kv(system_params_to_kv(&p).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_set() {
        let mut p = system_params_for_test();
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
