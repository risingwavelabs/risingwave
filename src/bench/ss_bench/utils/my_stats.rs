// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::core::Metric;
use risingwave_storage::monitor::{MyHistogram, StateStoreMetrics};

#[derive(Clone, Default)]
pub(crate) struct MyStateStoreStats {
    pub(crate) write_batch_duration: MyHistogram,
    pub(crate) write_batch_size: MyHistogram,
}

impl MyStateStoreStats {
    pub(crate) fn from_prom_stats(stats: &StateStoreMetrics) -> Self {
        Self {
            write_batch_duration: MyHistogram::from_prom_hist(
                stats.write_batch_duration.metric().get_histogram(),
            ),
            write_batch_size: MyHistogram::from_prom_hist(
                stats.write_batch_size.metric().get_histogram(),
            ),
        }
    }
}
