// Copyright 2022 RisingWave Labs
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

pub(crate) const TEST_TABLE_ID: TableId = TableId::new(233);

use std::fmt::Debug;
use std::sync::Arc;

use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedIntCounter, LabelGuardedIntGauge,
    LazyLabelGuardedIntCounter, LazyLabelGuardedIntGauge,
};
use risingwave_pb::id::{FragmentId, TableId};

use crate::monitor::HummockStateStoreMetrics;

pub mod shared_buffer_batch;

pub(crate) struct TableMemoryMetrics {
    imm_total_size: LabelGuardedIntGauge,
    fragment_imm_total_size: LabelGuardedIntGauge,
    imm_count: LabelGuardedIntGauge,
    fragment_imm_count: LabelGuardedIntGauge,
    pub write_batch_tuple_counts: LabelGuardedIntCounter,
    pub write_batch_duration: LabelGuardedHistogram,
    pub write_batch_size: LabelGuardedHistogram,
    pub mem_table_spill_counts: LazyLabelGuardedIntCounter,
    pub old_value_size: LazyLabelGuardedIntGauge,
}

impl TableMemoryMetrics {
    pub(super) fn new(
        metrics: &HummockStateStoreMetrics,
        table_id: TableId,
        fragment_id: FragmentId,
        is_replicated: bool,
    ) -> Self {
        let table_id_string = if is_replicated {
            format!("{} replicated", table_id)
        } else {
            table_id.to_string()
        };
        let labels_vec = vec![table_id_string];
        let labels = labels_vec.as_slice();
        let fragment_labels_vec = vec![fragment_id.to_string()];
        let fragment_labels = fragment_labels_vec.as_slice();
        Self {
            imm_total_size: metrics.per_table_imm_size.with_guarded_label_values(labels),
            fragment_imm_total_size: metrics
                .per_fragment_imm_size
                .with_guarded_label_values(fragment_labels),
            imm_count: metrics
                .per_table_imm_count
                .with_guarded_label_values(labels),
            fragment_imm_count: metrics
                .per_fragment_imm_count
                .with_guarded_label_values(fragment_labels),
            write_batch_tuple_counts: metrics
                .write_batch_tuple_counts
                .with_guarded_label_values(labels),
            write_batch_duration: metrics
                .write_batch_duration
                .with_guarded_label_values(labels),
            write_batch_size: metrics.write_batch_size.with_guarded_label_values(labels),
            mem_table_spill_counts: metrics
                .mem_table_spill_counts
                .lazy_guarded_metrics(labels_vec.clone()),
            old_value_size: metrics.old_value_size.lazy_guarded_metrics(labels_vec),
        }
    }

    pub(super) fn for_test() -> Arc<Self> {
        Self::new(
            &HummockStateStoreMetrics::unused(),
            TEST_TABLE_ID,
            FragmentId::default(),
            false,
        )
        .into()
    }

    pub(super) fn inc_imm(&self, imm_size: usize) {
        self.imm_total_size.add(imm_size as _);
        self.fragment_imm_total_size.add(imm_size as _);
        self.imm_count.inc();
        self.fragment_imm_count.inc();
    }

    pub(super) fn dec_imm(&self, imm_size: usize) {
        self.imm_total_size.sub(imm_size as _);
        self.fragment_imm_total_size.sub(imm_size as _);
        self.imm_count.dec();
        self.fragment_imm_count.dec();
    }
}

impl Debug for TableMemoryMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMemoryMetrics").finish()
    }
}
