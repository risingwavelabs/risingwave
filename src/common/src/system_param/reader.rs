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

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use risingwave_license::LicenseKeyRef;
use risingwave_pb::meta::PbSystemParams;

use super::{AdaptiveParallelismStrategy, ParamValue, default};

/// Information about a system parameter.
pub struct ParameterInfo {
    pub name: &'static str,
    pub mutable: bool,
    pub value: String,
    pub description: &'static str,
}

macro_rules! define_system_params_read_trait {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        /// The trait delegating reads on [`risingwave_pb::meta::SystemParams`].
        pub trait SystemParamsRead {
            $(
                #[doc = $doc]
                fn $field(&self) -> <$type as ParamValue>::Borrowed<'_>;
            )*

            fn get_all(&self) -> Vec<ParameterInfo> {
                vec![
                    $(
                        ParameterInfo {
                            name: stringify!($field),
                            mutable: $is_mutable,
                            value: self.$field().to_string(),
                            description: $doc,
                        },
                    )*
                ]
            }
        }
    };
}

crate::for_all_params!(define_system_params_read_trait);

/// A wrapper for [`PbSystemParams`] to provide easy access to system parameters.
#[derive(Clone, PartialEq)]
pub struct SystemParamsReader<I = Arc<PbSystemParams>> {
    inner: I,
}

impl From<PbSystemParams> for SystemParamsReader {
    fn from(params: PbSystemParams) -> Self {
        Self {
            inner: Arc::new(params),
        }
    }
}

impl<I> SystemParamsReader<I>
where
    I: Deref<Target = PbSystemParams>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &PbSystemParams {
        &self.inner
    }

    /// Return a new reader with the reference to the inner system params.
    pub fn as_ref(&self) -> SystemParamsReader<&PbSystemParams> {
        SystemParamsReader {
            inner: self.inner(),
        }
    }
}

macro_rules! impl_system_params_reader_debug {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        impl<I> Debug for SystemParamsReader<I>
        where
            I: Deref<Target = PbSystemParams>,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SystemParamsReader")
                    $(
                        .field(stringify!($field), &self.$field())
                    )*
                    .finish()
            }
        }
    };
}

crate::for_all_params!(impl_system_params_reader_debug);

impl<I> SystemParamsRead for SystemParamsReader<I>
where
    I: Deref<Target = PbSystemParams>,
{
    fn barrier_interval_ms(&self) -> u32 {
        self.inner()
            .barrier_interval_ms
            .unwrap_or(*default::BARRIER_INTERVAL_MS)
    }

    fn checkpoint_frequency(&self) -> u64 {
        self.inner()
            .checkpoint_frequency
            .unwrap_or(*default::CHECKPOINT_FREQUENCY)
    }

    fn sstable_size_mb(&self) -> u32 {
        self.inner()
            .sstable_size_mb
            .unwrap_or(*default::SSTABLE_SIZE_MB)
    }

    fn parallel_compact_size_mb(&self) -> u32 {
        self.inner()
            .parallel_compact_size_mb
            .unwrap_or(*default::PARALLEL_COMPACT_SIZE_MB)
    }

    fn block_size_kb(&self) -> u32 {
        self.inner()
            .block_size_kb
            .unwrap_or(*default::BLOCK_SIZE_KB)
    }

    fn bloom_false_positive(&self) -> f64 {
        self.inner()
            .bloom_false_positive
            .unwrap_or(*default::BLOOM_FALSE_POSITIVE)
    }

    fn state_store(&self) -> &str {
        self.inner()
            .state_store
            .as_deref()
            .unwrap_or(default::STATE_STORE.as_str())
    }

    fn data_directory(&self) -> &str {
        self.inner()
            .data_directory
            .as_deref()
            .unwrap_or(default::DATA_DIRECTORY.as_str())
    }

    fn backup_storage_url(&self) -> &str {
        self.inner()
            .backup_storage_url
            .as_deref()
            .unwrap_or(default::BACKUP_STORAGE_URL.as_str())
    }

    fn backup_storage_directory(&self) -> &str {
        self.inner()
            .backup_storage_directory
            .as_deref()
            .unwrap_or(default::BACKUP_STORAGE_DIRECTORY.as_str())
    }

    fn max_concurrent_creating_streaming_jobs(&self) -> u32 {
        self.inner()
            .max_concurrent_creating_streaming_jobs
            .unwrap_or(*default::MAX_CONCURRENT_CREATING_STREAMING_JOBS)
    }

    fn pause_on_next_bootstrap(&self) -> bool {
        self.inner()
            .pause_on_next_bootstrap
            .unwrap_or(*default::PAUSE_ON_NEXT_BOOTSTRAP)
    }

    fn enable_tracing(&self) -> bool {
        self.inner()
            .enable_tracing
            .unwrap_or(*default::ENABLE_TRACING)
    }

    fn use_new_object_prefix_strategy(&self) -> bool {
        self.inner()
            .use_new_object_prefix_strategy
            .unwrap_or(*default::USE_NEW_OBJECT_PREFIX_STRATEGY)
    }

    fn license_key(&self) -> LicenseKeyRef<'_> {
        self.inner()
            .license_key
            .as_deref()
            .unwrap_or_default()
            .into()
    }

    fn time_travel_retention_ms(&self) -> u64 {
        self.inner()
            .time_travel_retention_ms
            .unwrap_or(*default::TIME_TRAVEL_RETENTION_MS)
    }

    fn adaptive_parallelism_strategy(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY)
    }

    fn per_database_isolation(&self) -> bool {
        self.inner()
            .per_database_isolation
            .unwrap_or(*default::PER_DATABASE_ISOLATION)
    }

    fn enforce_secret(&self) -> bool {
        self.inner()
            .enforce_secret
            .unwrap_or(*default::ENFORCE_SECRET)
    }

    fn streaming_parallelism_for_table(&self) -> u32 {
        self.inner()
            .streaming_parallelism_for_table
            .unwrap_or(*default::STREAMING_PARALLELISM_FOR_TABLE)
    }

    fn streaming_parallelism_for_materialized_view(&self) -> u32 {
        self.inner()
            .streaming_parallelism_for_materialized_view
            .unwrap_or(*default::STREAMING_PARALLELISM_FOR_MATERIALIZED_VIEW)
    }

    fn streaming_parallelism_for_sink(&self) -> u32 {
        self.inner()
            .streaming_parallelism_for_sink
            .unwrap_or(*default::STREAMING_PARALLELISM_FOR_SINK)
    }

    fn streaming_parallelism_for_source(&self) -> u32 {
        self.inner()
            .streaming_parallelism_for_source
            .unwrap_or(*default::STREAMING_PARALLELISM_FOR_SOURCE)
    }

    fn streaming_parallelism_for_index(&self) -> u32 {
        self.inner()
            .streaming_parallelism_for_index
            .unwrap_or(*default::STREAMING_PARALLELISM_FOR_INDEX)
    }

    fn adaptive_parallelism_strategy_for_table(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy_for_table
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY_FOR_TABLE)
    }

    fn adaptive_parallelism_strategy_for_materialized_view(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy_for_materialized_view
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY_FOR_MATERIALIZED_VIEW)
    }

    fn adaptive_parallelism_strategy_for_sink(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy_for_sink
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY_FOR_SINK)
    }

    fn adaptive_parallelism_strategy_for_source(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy_for_source
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY_FOR_SOURCE)
    }

    fn adaptive_parallelism_strategy_for_index(&self) -> AdaptiveParallelismStrategy {
        self.inner()
            .adaptive_parallelism_strategy_for_index
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(*default::ADAPTIVE_PARALLELISM_STRATEGY_FOR_INDEX)
    }
}
