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
                fn $field<'a>(&'a self) -> <$type as ParamValue>::Borrowed<'a>;
            )*
        }
    };
}

for_all_params!(define_system_params_read_trait);

/// The wrapper delegating reads on [`risingwave_pb::meta::SystemParams`].
///
/// See [`SystemParamsRead`] for more details.
#[derive(Clone, Debug, PartialEq)]
pub struct SystemParamsReader {
    prost: PbSystemParams,
}

impl From<PbSystemParams> for SystemParamsReader {
    fn from(prost: PbSystemParams) -> Self {
        Self { prost }
    }
}

// Unwrap the field if it exists from the initial release.
// Otherwise, fill it with the backward compatibility logic.
impl SystemParamsRead for SystemParamsReader {
    fn barrier_interval_ms(&self) -> u32 {
        self.prost.barrier_interval_ms.unwrap()
    }

    fn checkpoint_frequency(&self) -> u64 {
        self.prost.checkpoint_frequency.unwrap()
    }

    fn parallel_compact_size_mb(&self) -> u32 {
        self.prost.parallel_compact_size_mb.unwrap()
    }

    fn sstable_size_mb(&self) -> u32 {
        self.prost.sstable_size_mb.unwrap()
    }

    fn block_size_kb(&self) -> u32 {
        self.prost.block_size_kb.unwrap()
    }

    fn bloom_false_positive(&self) -> f64 {
        self.prost.bloom_false_positive.unwrap()
    }

    fn state_store(&self) -> &str {
        self.prost.state_store.as_ref().unwrap()
    }

    fn data_directory(&self) -> &str {
        self.prost.data_directory.as_ref().unwrap()
    }

    fn backup_storage_url(&self) -> &str {
        self.prost.backup_storage_url.as_ref().unwrap()
    }

    fn backup_storage_directory(&self) -> &str {
        self.prost.backup_storage_directory.as_ref().unwrap()
    }

    fn max_concurrent_creating_streaming_jobs(&self) -> u32 {
        self.prost.max_concurrent_creating_streaming_jobs.unwrap()
    }

    fn pause_on_next_bootstrap(&self) -> bool {
        self.prost
            .pause_on_next_bootstrap
            .unwrap_or_else(|| default::pause_on_next_bootstrap().unwrap())
    }

    fn enable_tracing(&self) -> bool {
        self.prost
            .enable_tracing
            .unwrap_or_else(|| default::enable_tracing().unwrap())
    }

    fn wasm_storage_url(&self) -> &str {
        self.prost.wasm_storage_url.as_ref().unwrap()
    }
}

impl SystemParamsReader {
    pub fn to_kv(&self) -> Vec<(String, String)> {
        system_params_to_kv(&self.prost).unwrap()
    }
}
