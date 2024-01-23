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

use std::borrow::Borrow;

use risingwave_pb::meta::PbSystemParams;

use super::{default, system_params_to_kv, ParamValue};
use crate::for_all_params;

macro_rules! define_system_params_read_trait {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        /// The trait delegating reads on [`risingwave_pb::meta::SystemParams`].
        ///
        /// For 2 purposes:
        /// - Avoid misuse of deprecated fields by hiding their getters.
        /// - Abstract fallback logic for fields that might not be provided by meta service due to backward
        ///   compatibility.
        pub trait SystemParamsRead {
            $(
                #[doc = $doc]
                fn $field(&self) -> <$type as ParamValue>::Borrowed<'_>;
            )*
        }
    };
}

for_all_params!(define_system_params_read_trait);

/// The wrapper delegating reads on [`risingwave_pb::meta::SystemParams`].
///
/// See [`SystemParamsRead`] for more details.
#[derive(Clone, Debug, PartialEq)]
pub struct SystemParamsReader<I = PbSystemParams> {
    inner: I,
}

impl<I> From<I> for SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    fn from(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    /// Return a new reader with the reference to the inner system params.
    pub fn as_ref(&self) -> SystemParamsReader<&PbSystemParams> {
        SystemParamsReader {
            inner: self.inner(),
        }
    }

    pub fn to_kv(&self) -> Vec<(String, String)> {
        system_params_to_kv(self.inner()).unwrap()
    }

    fn inner(&self) -> &PbSystemParams {
        self.inner.borrow()
    }
}

/// - Unwrap the field if it always exists.
///   For example, if a parameter is introduced before the initial public release.
///
/// - Otherwise, specify the fallback logic when the field is missing.
impl<I> SystemParamsRead for SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    fn barrier_interval_ms(&self) -> u32 {
        self.inner().barrier_interval_ms.unwrap()
    }

    fn checkpoint_frequency(&self) -> u64 {
        self.inner().checkpoint_frequency.unwrap()
    }

    fn parallel_compact_size_mb(&self) -> u32 {
        self.inner().parallel_compact_size_mb.unwrap()
    }

    fn sstable_size_mb(&self) -> u32 {
        self.inner().sstable_size_mb.unwrap()
    }

    fn block_size_kb(&self) -> u32 {
        self.inner().block_size_kb.unwrap()
    }

    fn bloom_false_positive(&self) -> f64 {
        self.inner().bloom_false_positive.unwrap()
    }

    fn state_store(&self) -> &str {
        self.inner().state_store.as_ref().unwrap()
    }

    fn data_directory(&self) -> &str {
        self.inner().data_directory.as_ref().unwrap()
    }

    fn backup_storage_url(&self) -> &str {
        self.inner().backup_storage_url.as_ref().unwrap()
    }

    fn backup_storage_directory(&self) -> &str {
        self.inner().backup_storage_directory.as_ref().unwrap()
    }

    fn max_concurrent_creating_streaming_jobs(&self) -> u32 {
        self.inner().max_concurrent_creating_streaming_jobs.unwrap()
    }

    fn pause_on_next_bootstrap(&self) -> bool {
        self.inner()
            .pause_on_next_bootstrap
            .unwrap_or_else(default::pause_on_next_bootstrap)
    }

    fn enable_tracing(&self) -> bool {
        self.inner()
            .enable_tracing
            .unwrap_or_else(default::enable_tracing)
    }

    fn wasm_storage_url(&self) -> &str {
        self.inner()
            .wasm_storage_url
            .as_ref()
            .unwrap_or(&default::WASM_STORAGE_URL)
    }
}
