// Copyright 2025 RisingWave Labs
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
use std::sync::Arc;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_pb::id::TableId;

use crate::monitor::HummockStateStoreMetrics;

pub mod shared_buffer_batch;

pub(crate) struct TableMemoryMetrics {
    imm_total_size: LabelGuardedIntGauge,
    imm_count: LabelGuardedIntGauge,
}

impl TableMemoryMetrics {
    pub(super) fn new(metrics: &HummockStateStoreMetrics, table_id: TableId) -> Self {
        let table_id_string = table_id.to_string();
        Self {
            imm_total_size: metrics
                .per_table_imm_size
                .with_guarded_label_values(&[&table_id_string]),
            imm_count: metrics
                .per_table_imm_count
                .with_guarded_label_values(&[&table_id_string]),
        }
    }

    pub(super) fn for_test() -> Arc<Self> {
        Self::new(&HummockStateStoreMetrics::unused(), TableId::new(233)).into()
    }

    pub(super) fn inc(&self, imm_size: usize) {
        self.imm_total_size.add(imm_size as _);
        self.imm_count.inc();
    }

    pub(super) fn dec(&self, imm_size: usize) {
        self.imm_total_size.sub(imm_size as _);
        self.imm_count.dec();
    }
}

impl Debug for TableMemoryMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMemoryMetrics").finish()
    }
}
