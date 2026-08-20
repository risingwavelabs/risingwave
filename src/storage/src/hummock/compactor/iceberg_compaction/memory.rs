// Copyright 2026 RisingWave Labs
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

use std::mem::size_of;

use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, FormatVersion, PrimitiveType, Schema, Type};
use iceberg_compaction_core::compaction::CompactionPlan;

/// `DataFusion`'s `datafusion.execution.sort_spill_reservation_bytes` default.
///
/// Setting `max_memory_bytes` makes `iceberg-compaction-core` build a `DiskManager` alongside the
/// bounded pool. Each `ExternalSorter` partition then resizes a merge reservation to this value
/// before it sorts a single row, on a consumer registered *without* `can_spill` -- so the pool can
/// neither spill nor shrink it.
const DATAFUSION_SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;

/// Fixed task-context, operator, and allocator overhead observed for small streaming plans.
const DATAFUSION_RUNTIME_FIXED_BYTES: usize = 640 * 1024;

/// Upper bound for decoded Arrow data retained beyond the explicitly accounted record batches.
const DATAFUSION_STREAMING_DECODED_WINDOW_BYTES: usize = 16 * 1024 * 1024;

/// Output writer memory retained across input batches.
const DATAFUSION_WRITER_WINDOW_BYTES: usize = 10 * 1024 * 1024;

const LARGE_SORT_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
const SORT_TEMPORARY_HEADROOM_BYTES: usize = 32 * 1024 * 1024;
const HEAP_FIXED_HEADROOM_BYTES: usize = 256 * 1024;

/// Compressed-to-decoded fallback used when the schema contains variable-width fields.
const COMPRESSED_TO_DECODED_INFLATION: usize = 4;

/// At this scan concurrency, prefetched files and decoded buffers measurably overlap at peak.
const STREAMING_PREFETCH_DECODE_OVERLAP_MIN_ACTIVE_FILES: usize = 4;

/// Compressed-size estimate for decoded equality-delete value buffers.
const EQUALITY_DELETE_INFLATION: usize = 8;

/// Compressed-size estimate for decoded position-delete value buffers.
const POSITION_DELETE_INFLATION: usize = 5;

/// Per-row hash table and join bookkeeping floor for `DataFusion`'s non-spillable `HashJoinInput`.
const HASH_JOIN_ROW_OVERHEAD_BYTES: usize = 40;

/// Per-row Arrow and hashing overhead for the two position-delete join keys.
const POSITION_DELETE_KEY_OVERHEAD_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanMemoryEstimate {
    /// Minimum logical `DataFusion` pool capacity that cannot be recovered by spilling.
    pub non_spillable_bytes: usize,
    /// Minimum pool capacity required for the scan and sort pipeline to make progress.
    pub minimum_pool_bytes: usize,
    /// Logical `DataFusion` pool capacity preferred when no worker budget is applied.
    pub preferred_pool_bytes: usize,
    fixed_heap_peak_bytes: usize,
    sorted: bool,
    large_sorted: bool,
}

impl PlanMemoryEstimate {
    pub fn expected_heap_peak_bytes(&self, pool_limit_bytes: usize) -> usize {
        let heap_peak_bytes = if self.large_sorted {
            self.fixed_heap_peak_bytes.max(
                self.non_spillable_bytes
                    .saturating_add(pool_limit_bytes / 2)
                    .saturating_add(SORT_TEMPORARY_HEADROOM_BYTES),
            )
        } else if self.sorted {
            self.fixed_heap_peak_bytes
                .max(pool_limit_bytes.saturating_mul(3) / 4)
        } else {
            self.fixed_heap_peak_bytes
        }
        .saturating_add(HEAP_FIXED_HEADROOM_BYTES);

        heap_peak_bytes.saturating_add(heap_peak_bytes / 50)
    }

    pub fn minimum_heap_bytes(&self) -> usize {
        self.expected_heap_peak_bytes(self.minimum_pool_bytes)
    }

    pub fn pool_limit_for_heap_budget(
        &self,
        heap_budget_bytes: usize,
        maximum_pool_bytes: usize,
    ) -> Option<usize> {
        let maximum_pool_bytes = maximum_pool_bytes.min(self.preferred_pool_bytes);
        if maximum_pool_bytes < self.minimum_pool_bytes
            || self.minimum_heap_bytes() > heap_budget_bytes
        {
            return None;
        }
        if self.expected_heap_peak_bytes(maximum_pool_bytes) <= heap_budget_bytes {
            return Some(maximum_pool_bytes);
        }

        debug_assert!(self.large_sorted);
        let heap_before_margin = heap_budget_bytes
            .saturating_mul(50)
            .checked_div(51)
            .unwrap_or_default()
            .saturating_sub(HEAP_FIXED_HEADROOM_BYTES);
        let pool_limit_bytes = heap_before_margin
            .saturating_sub(self.non_spillable_bytes)
            .saturating_sub(SORT_TEMPORARY_HEADROOM_BYTES)
            .saturating_mul(2)
            .min(maximum_pool_bytes);

        (pool_limit_bytes >= self.minimum_pool_bytes).then_some(pool_limit_bytes)
    }
}

pub fn estimate_plan_memory(
    plan: &CompactionPlan,
    schema: &Schema,
    format_version: FormatVersion,
    max_record_batch_rows: usize,
    enable_prefetch: bool,
    requires_sort: bool,
    output_parallelism: usize,
) -> PlanMemoryEstimate {
    let data_files = &plan.file_group.data_files;
    let compressed_input_bytes = sum_file_sizes(data_files.iter());
    let prefetch = if enable_prefetch {
        // `iceberg-compaction-core` stages one complete file per active scan partition in a
        // memory-backed `FileIO`. The decoded batches are accounted separately below, so the
        // staged compressed bytes must not be multiplied by another decoded-copy factor.
        estimate_prefetch_bytes(plan, format_version)
    } else {
        0
    };

    let mut record_count = 0usize;
    let mut has_complete_record_counts = true;
    for task in data_files {
        match task.record_count {
            Some(count) => {
                record_count = record_count.saturating_add(count as usize);
            }
            None => has_complete_record_counts = false,
        }
    }

    let schema_row_width = estimated_schema_row_width(schema);
    let hidden_row_width = hidden_row_width(plan, format_version);
    let (batch_row_width, decoded_input_bytes, decoded_output_bytes) =
        if let (Some(schema_row_width), true) = (schema_row_width, has_complete_record_counts) {
            let row_width = schema_row_width.saturating_add(hidden_row_width);
            (
                row_width,
                row_width.saturating_mul(record_count),
                schema_row_width.saturating_mul(record_count),
            )
        } else {
            let fallback_row_count = record_count.max(1);
            let fallback_row_width = compressed_input_bytes
                .checked_div(fallback_row_count)
                .unwrap_or_default()
                .saturating_mul(COMPRESSED_TO_DECODED_INFLATION)
                .max(1);
            // Keep whole-input inflation separate from batch sizing. Missing counts must not
            // turn the entire compressed input into one synthetic row-sized batch.
            let batch_row_width = fallback_row_width.saturating_add(hidden_row_width);
            let decoded_output_bytes =
                compressed_input_bytes.saturating_mul(COMPRESSED_TO_DECODED_INFLATION);
            let decoded_input_bytes =
                decoded_output_bytes.saturating_add(hidden_row_width.saturating_mul(record_count));
            (batch_row_width, decoded_input_bytes, decoded_output_bytes)
        };

    // Equality deletes are broadcast and fully materialized by the merge-on-read plan.
    let equality_delete_bytes = estimate_equality_delete_join_bytes(plan);

    // Pre-V3 position deletes build a full anti-join hash table. V3 deletion vectors do not.
    let position_delete_raw_bytes = if format_version < FormatVersion::V3 {
        sum_file_sizes(plan.file_group.position_delete_files.iter())
    } else {
        0
    };

    let position_delete_join_bytes = if position_delete_raw_bytes > 0 {
        let compressed_estimate = position_delete_raw_bytes
            .saturating_mul(POSITION_DELETE_INFLATION)
            .saturating_add(estimate_position_delete_join_overhead_bytes(plan));
        // HashJoin retains both the collected build-side batches and its hash table for the whole
        // probe stream, so both fully overlap a SortExec consuming and buffering the join output.
        compressed_input_bytes.saturating_add(compressed_estimate)
    } else {
        0
    };

    let executor_parallelism = plan.recommended_executor_parallelism().max(1);
    let sort_workspace_bytes = if requires_sort {
        // SortExec preserves and concurrently executes every output partition. The aggregate
        // spillable reservation therefore covers the full decoded input, not one partition.
        decoded_output_bytes
    } else {
        0
    };
    // Every sorted output partition pins a non-spillable merge reservation before reading data.
    let sort_merge_headroom_bytes = if requires_sort {
        DATAFUSION_SORT_SPILL_RESERVATION_BYTES.saturating_mul(output_parallelism.max(1))
    } else {
        0
    };
    let batch_allocation_bytes = batch_row_width.saturating_mul(max_record_batch_rows);
    let concurrent_batch_allocation_bytes =
        batch_allocation_bytes.saturating_mul(executor_parallelism);
    // Every scan partition has its own buffer. Streaming plans keep two batches in flight per
    // partition; sorted plans keep one before handing it to the sorter.
    let batch_overhead_bytes = if requires_sort {
        concurrent_batch_allocation_bytes
    } else {
        concurrent_batch_allocation_bytes.saturating_mul(2)
    };
    let (non_spillable_bytes, preferred_pool_bytes) = if requires_sort {
        let non_spillable_bytes = sort_merge_headroom_bytes
            .saturating_add(equality_delete_bytes)
            .saturating_add(position_delete_join_bytes)
            .saturating_add(prefetch)
            .saturating_add(batch_overhead_bytes);
        let sort_peak = sort_workspace_bytes
            .saturating_add(sort_merge_headroom_bytes)
            .saturating_add(equality_delete_bytes)
            .saturating_add(position_delete_join_bytes)
            .saturating_add(prefetch);
        (non_spillable_bytes, sort_peak.max(non_spillable_bytes))
    } else {
        let data_file_count = data_files.len().max(1);
        let active_files = executor_parallelism.min(data_files.len());
        // Bound the extra decoder window from two directions: one partition's proportional share
        // of the decoded input, and the active files' fair share with 25% pipeline headroom.
        let per_partition_share = decoded_input_bytes
            .checked_div(active_files.max(1))
            .unwrap_or(decoded_input_bytes);
        let active_file_share = decoded_input_bytes
            .checked_div(data_file_count)
            .unwrap_or(decoded_input_bytes)
            .saturating_mul(active_files);
        let active_file_share_with_headroom =
            active_file_share.saturating_add(active_file_share / 4);
        let decoded_in_flight = per_partition_share
            .min(active_file_share_with_headroom)
            .min(DATAFUSION_STREAMING_DECODED_WINDOW_BYTES);
        let overlap_input_threshold =
            DATAFUSION_STREAMING_DECODED_WINDOW_BYTES / COMPRESSED_TO_DECODED_INFLATION;
        let scan_buffer_bytes = if active_files
            >= STREAMING_PREFETCH_DECODE_OVERLAP_MIN_ACTIVE_FILES
            && compressed_input_bytes >= overlap_input_threshold
        {
            prefetch.saturating_add(decoded_in_flight)
        } else {
            prefetch.max(decoded_in_flight)
        };
        let common_bytes = scan_buffer_bytes
            .saturating_add(batch_overhead_bytes)
            .saturating_add(equality_delete_bytes);
        let non_spillable_bytes = common_bytes
            .saturating_add(position_delete_join_bytes)
            .max(batch_allocation_bytes)
            .max(DATAFUSION_RUNTIME_FIXED_BYTES);
        (non_spillable_bytes, non_spillable_bytes)
    };

    let data_file_count = data_files.len().max(1);
    let active_data_files = executor_parallelism.min(data_files.len());
    let active_decoded_bytes = decoded_input_bytes
        .checked_div(data_file_count)
        .unwrap_or(decoded_input_bytes)
        .saturating_mul(active_data_files);
    let active_input_bytes = estimate_prefetch_bytes(plan, format_version);
    let total_batches = record_count
        .saturating_add(max_record_batch_rows.saturating_sub(1))
        .checked_div(max_record_batch_rows)
        .unwrap_or_default();
    let batch_overlap =
        (total_batches as f64 / executor_parallelism.saturating_mul(8) as f64).min(1.0);
    let batch_heap_bytes = (batch_overhead_bytes as f64 * batch_overlap) as usize;
    // OpenDAL S3 buffers the active compressed input while Parquet decoding and scan batches
    // overlap. This phase is independent from DataFusion's logical pool reservations.
    let scan_heap_bytes = active_input_bytes
        .saturating_add(active_input_bytes.min(DATAFUSION_STREAMING_DECODED_WINDOW_BYTES))
        .saturating_add(active_decoded_bytes.min(DATAFUSION_STREAMING_DECODED_WINDOW_BYTES))
        .saturating_add(batch_heap_bytes);
    let writer_heap_bytes = if schema_row_width.is_some() {
        decoded_output_bytes.saturating_mul(3) / 2
    } else {
        let output_scale = output_parallelism.min(4) as f64 / 4.0;
        let large_single_scan_scale = if executor_parallelism == 1 {
            (active_input_bytes as f64 / DATAFUSION_STREAMING_DECODED_WINDOW_BYTES as f64).min(1.0)
        } else {
            0.0
        };
        ((decoded_output_bytes / 2).min(DATAFUSION_WRITER_WINDOW_BYTES) as f64
            * output_scale.max(large_single_scan_scale)) as usize
    }
    .min(DATAFUSION_WRITER_WINDOW_BYTES);
    let streaming_heap_bytes = scan_heap_bytes.saturating_add(writer_heap_bytes);

    // Delete joins retain hidden probe columns while the build side remains materialized.
    let hidden_total_bytes = hidden_row_width.saturating_mul(record_count);
    let position_delete_records = plan
        .file_group
        .position_delete_files
        .iter()
        .filter_map(|task| task.record_count)
        .fold(0u64, u64::saturating_add) as usize;
    let join_heap_bytes = if position_delete_records > 0 {
        let delete_ratio = position_delete_records as f64 / record_count.max(1) as f64;
        let overlap_factor = 0.6 + 0.3 * delete_ratio.min(1.0);
        (hidden_total_bytes as f64 * overlap_factor) as usize
            + position_delete_raw_bytes.saturating_mul(8)
    } else if !plan.file_group.equality_delete_files.is_empty() {
        hidden_total_bytes.saturating_add(equality_delete_bytes)
    } else {
        0
    };
    // Large sorts retain their full decoded input. Smaller sorts release part of each partition as
    // merge runs, based on the S3 heap profiles used to calibrate this estimate.
    let sorted_decoded_bytes = if decoded_output_bytes > 64 * 1024 * 1024 {
        decoded_output_bytes
    } else {
        decoded_output_bytes.saturating_mul(2) / 3
    };
    let sorted_heap_bytes = active_input_bytes
        .saturating_add(active_input_bytes.min(DATAFUSION_STREAMING_DECODED_WINDOW_BYTES))
        .saturating_add(sorted_decoded_bytes);
    let execution_heap_bytes = if requires_sort {
        sorted_heap_bytes
    } else {
        streaming_heap_bytes
    };
    let join_phase_bytes = scan_heap_bytes.saturating_add(join_heap_bytes);
    let join_phase_bytes = if requires_sort {
        join_phase_bytes.saturating_mul(3) / 4
    } else {
        join_phase_bytes
    };
    let large_sorted = requires_sort && decoded_output_bytes > LARGE_SORT_THRESHOLD_BYTES;
    let fixed_heap_peak_bytes = if large_sorted {
        scan_heap_bytes.max(join_phase_bytes)
    } else {
        execution_heap_bytes.max(join_phase_bytes)
    };
    let preferred_pool_with_headroom = preferred_pool_bytes
        .saturating_add(batch_allocation_bytes)
        .saturating_add(if requires_sort {
            DATAFUSION_STREAMING_DECODED_WINDOW_BYTES
        } else {
            0
        });
    let minimum_pool_bytes = if large_sorted {
        non_spillable_bytes
            .saturating_add(active_decoded_bytes.saturating_mul(2))
            .saturating_add(DATAFUSION_STREAMING_DECODED_WINDOW_BYTES)
            .saturating_add(batch_allocation_bytes)
    } else if requires_sort {
        preferred_pool_with_headroom
    } else {
        non_spillable_bytes.saturating_add(batch_allocation_bytes)
    };
    let preferred_pool_bytes = preferred_pool_with_headroom.max(minimum_pool_bytes);

    PlanMemoryEstimate {
        non_spillable_bytes,
        minimum_pool_bytes,
        preferred_pool_bytes,
        fixed_heap_peak_bytes,
        sorted: requires_sort,
        large_sorted,
    }
}

fn estimate_prefetch_bytes(plan: &CompactionPlan, format_version: FormatVersion) -> usize {
    let concurrency = plan.recommended_executor_parallelism().max(1);
    let data = estimate_provider_prefetch(
        plan.file_group.data_files.iter().filter(|task| {
            format_version < FormatVersion::V3
                || !task
                    .deletes
                    .iter()
                    .any(|delete| delete.file_type == DataContentType::PositionDeletes)
        }),
        concurrency,
    );
    // Delete files that get their own table provider are scanned -- and therefore prefetched --
    // concurrently with the data scan, because `DatafusionTableRegister` passes the same
    // `enable_prefetch` flag to every provider it builds. From V3 on, position deletes are
    // deletion vectors attached to the data task instead of a registered table, so only the
    // pre-V3 position-delete provider counts here.
    let position_deletes = if format_version < FormatVersion::V3 {
        estimate_provider_prefetch(plan.file_group.position_delete_files.iter(), concurrency)
    } else {
        0
    };
    let equality_deletes =
        estimate_provider_prefetch(plan.file_group.equality_delete_files.iter(), concurrency);

    data.saturating_add(position_deletes)
        .saturating_add(equality_deletes)
}

fn estimate_provider_prefetch<'a>(
    tasks: impl Iterator<Item = &'a FileScanTask>,
    concurrency: usize,
) -> usize {
    let mut file_sizes = tasks
        .map(|task| task.file_size_in_bytes as usize)
        .collect::<Vec<_>>();
    file_sizes.sort_unstable_by(|left, right| right.cmp(left));
    file_sizes
        .into_iter()
        .take(concurrency)
        .fold(0usize, usize::saturating_add)
}

fn sum_file_sizes<'a>(tasks: impl Iterator<Item = &'a FileScanTask>) -> usize {
    tasks
        .map(|task| task.file_size_in_bytes as usize)
        .fold(0usize, usize::saturating_add)
}

fn estimate_position_delete_join_overhead_bytes(plan: &CompactionPlan) -> usize {
    let Some(record_count) = complete_record_count(&plan.file_group.position_delete_files) else {
        return 0;
    };

    HASH_JOIN_ROW_OVERHEAD_BYTES
        .saturating_add(POSITION_DELETE_KEY_OVERHEAD_BYTES)
        .saturating_mul(record_count)
}

fn estimate_equality_delete_join_bytes(plan: &CompactionPlan) -> usize {
    let compressed_estimate = sum_file_sizes(plan.file_group.equality_delete_files.iter())
        .saturating_mul(EQUALITY_DELETE_INFLATION);
    let row_overhead = complete_record_count(&plan.file_group.equality_delete_files)
        .unwrap_or_default()
        .saturating_mul(HASH_JOIN_ROW_OVERHEAD_BYTES);

    compressed_estimate.saturating_add(row_overhead)
}

fn complete_record_count(tasks: &[FileScanTask]) -> Option<usize> {
    tasks.iter().try_fold(0usize, |total, task| {
        let count = usize::try_from(task.record_count?).unwrap_or(usize::MAX);
        Some(total.saturating_add(count))
    })
}

fn position_delete_row_width(plan: &CompactionPlan) -> usize {
    let max_data_path_width = plan
        .file_group
        .data_files
        .iter()
        .map(|task| task.data_file_path.len())
        .max()
        .unwrap_or_default();
    max_data_path_width
        .saturating_add(size_of::<i32>())
        .saturating_add(size_of::<i64>())
}

fn hidden_row_width(plan: &CompactionPlan, format_version: FormatVersion) -> usize {
    // Match DataFusionTaskContextBuilder: equality deletes add an i64 sequence number, while
    // pre-V3 position deletes add the data path Utf8 array and an i64 row position.
    let sequence_number_width = if !plan.file_group.equality_delete_files.is_empty() {
        size_of::<i64>()
    } else {
        0
    };
    let position_delete_width = if format_version < FormatVersion::V3
        && !plan.file_group.position_delete_files.is_empty()
    {
        position_delete_row_width(plan)
    } else {
        0
    };

    sequence_number_width.saturating_add(position_delete_width)
}

fn estimated_schema_row_width(schema: &Schema) -> Option<usize> {
    let mut width = 0usize;
    for field in schema.as_struct().fields() {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            return None;
        };
        let field_width = primitive_width(primitive)?;
        width = width.saturating_add(field_width);
    }
    (width > 0).then_some(width)
}

fn primitive_width(primitive: &PrimitiveType) -> Option<usize> {
    let width = match primitive {
        PrimitiveType::Boolean => 1,
        PrimitiveType::Int | PrimitiveType::Float | PrimitiveType::Date => 4,
        PrimitiveType::Long
        | PrimitiveType::Double
        | PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => 8,
        PrimitiveType::Decimal { .. } | PrimitiveType::Uuid => 16,
        PrimitiveType::Fixed(size) => *size as usize,
        PrimitiveType::String | PrimitiveType::Binary => return None,
    };
    Some(width)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
    use iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Type,
    };
    use iceberg_compaction_core::compaction::CompactionPlan;
    use iceberg_compaction_core::file_selection::FileGroup;

    use super::{
        COMPRESSED_TO_DECODED_INFLATION, DATAFUSION_SORT_SPILL_RESERVATION_BYTES,
        DATAFUSION_STREAMING_DECODED_WINDOW_BYTES, estimate_plan_memory, estimate_prefetch_bytes,
    };

    fn schema(fields: Vec<Type>) -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(
                    fields
                        .into_iter()
                        .enumerate()
                        .map(|(index, field_type)| {
                            NestedField::required(
                                i32::try_from(index + 1).unwrap(),
                                format!("field_{index}"),
                                field_type,
                            )
                            .into()
                        })
                        .collect::<Vec<_>>(),
                )
                .build()
                .unwrap(),
        )
    }

    fn scan_task(
        schema: Arc<Schema>,
        path: &str,
        content: DataContentType,
        file_size_in_bytes: u64,
        record_count: Option<u64>,
    ) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(file_size_in_bytes)
            .with_start(0)
            .with_length(file_size_in_bytes)
            .with_record_count(record_count)
            .with_data_file_path(path.to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(
                matches!(content, DataContentType::EqualityDeletes)
                    .then(|| vec![1])
                    .unwrap_or_default(),
            )
            .with_case_sensitive(true)
            .build()
    }

    fn delete_task(
        path: &str,
        content: DataContentType,
        file_size_in_bytes: u64,
        record_count: Option<u64>,
    ) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path(path.to_owned())
            .with_file_size_in_bytes(file_size_in_bytes)
            .with_file_type(content)
            .with_partition_spec_id(0)
            .with_equality_ids(matches!(content, DataContentType::EqualityDeletes).then(|| vec![1]))
            .with_file_format(DataFileFormat::Parquet)
            .with_record_count(record_count)
            .with_sequence_number(0)
            .build()
    }

    fn plan_with_files(
        schema: Arc<Schema>,
        data_size: u64,
        data_records: Option<u64>,
        deletes: Vec<(DataContentType, u64, Option<u64>)>,
        input_parallelism: usize,
    ) -> CompactionPlan {
        let mut data = scan_task(
            schema.clone(),
            "data.parquet",
            DataContentType::Data,
            data_size,
            data_records,
        );
        data.deletes = deletes
            .into_iter()
            .enumerate()
            .map(|(index, (content, size, records))| {
                delete_task(&format!("delete_{index}.parquet"), content, size, records)
            })
            .collect();
        let mut file_group = FileGroup::new(vec![data]);
        file_group.executor_parallelism = input_parallelism;
        CompactionPlan::new(file_group, "main", 1)
    }

    #[test]
    fn prefetch_matches_core_normalization_and_reserves_complete_files() {
        let schema = schema(vec![Type::Primitive(PrimitiveType::Long)]);
        let mut position_delete_bearing = scan_task(
            schema.clone(),
            "position-delete-bearing.parquet",
            DataContentType::Data,
            1_000,
            Some(10),
        );
        position_delete_bearing.deletes.push(delete_task(
            "position-delete.parquet",
            DataContentType::PositionDeletes,
            30,
            Some(1),
        ));
        let mut equality_delete_bearing = scan_task(
            schema.clone(),
            "equality-delete-bearing.parquet",
            DataContentType::Data,
            200,
            Some(10),
        );
        equality_delete_bearing.deletes.push(delete_task(
            "equality-delete.parquet",
            DataContentType::EqualityDeletes,
            70,
            Some(1),
        ));
        let large_file_size = 256 * 1024 * 1024;
        let mut file_group = FileGroup::new(vec![
            position_delete_bearing,
            equality_delete_bearing,
            scan_task(
                schema.clone(),
                "large.parquet",
                DataContentType::Data,
                large_file_size,
                Some(1_000_000),
            ),
        ]);
        file_group.position_delete_files = vec![scan_task(
            schema.clone(),
            "position-delete.parquet",
            DataContentType::PositionDeletes,
            30,
            Some(1),
        )];
        file_group.equality_delete_files = vec![scan_task(
            schema,
            "equality-delete.parquet",
            DataContentType::EqualityDeletes,
            70,
            Some(1),
        )];
        file_group.executor_parallelism = 2;
        let plan = CompactionPlan::new(file_group, "main", 1);

        let v2 = estimate_prefetch_bytes(&plan, FormatVersion::V2);
        assert_eq!(v2, large_file_size as usize + 1_000 + 30 + 70);

        let v3 = estimate_prefetch_bytes(&plan, FormatVersion::V3);
        assert_eq!(v3, large_file_size as usize + 200 + 70);
    }

    #[test]
    fn streaming_decoded_window_is_bounded_for_large_inputs() {
        let schema = schema(vec![
            Type::Primitive(PrimitiveType::Int),
            Type::Primitive(PrimitiveType::String),
        ]);
        let file_size = 256 * 1024 * 1024;
        let plan = plan_with_files(schema.clone(), file_size, Some(file_size), vec![], 1);

        let estimate = estimate_plan_memory(&plan, &schema, FormatVersion::V2, 1, false, false, 1);

        // The fallback row width is 4 bytes, and the streaming branch accounts two explicit
        // one-row batches in addition to the bounded decoded window.
        assert_eq!(
            estimate.preferred_pool_bytes,
            DATAFUSION_STREAMING_DECODED_WINDOW_BYTES + 12
        );

        let file_size = 2 * 1024 * 1024;
        let data_files = (0..4)
            .map(|index| {
                scan_task(
                    schema.clone(),
                    &format!("data-{index}.parquet"),
                    DataContentType::Data,
                    file_size,
                    Some(file_size),
                )
            })
            .collect();
        let mut file_group = FileGroup::new(data_files);
        file_group.executor_parallelism = 4;
        let plan = CompactionPlan::new(file_group, "main", 1);
        let enabled = estimate_plan_memory(&plan, &schema, FormatVersion::V2, 1, true, false, 4);
        let disabled = estimate_plan_memory(&plan, &schema, FormatVersion::V2, 1, false, false, 4);

        assert_eq!(
            enabled.preferred_pool_bytes - disabled.preferred_pool_bytes,
            4 * file_size as usize
        );
        assert_eq!(
            disabled.preferred_pool_bytes,
            file_size as usize * COMPRESSED_TO_DECODED_INFLATION + 36
        );
    }

    #[test]
    fn sorted_plan_reserves_every_partition_merge_head() {
        let schema = schema(vec![Type::Primitive(PrimitiveType::Int)]);
        let mut plan = plan_with_files(schema.clone(), 8, Some(1), vec![], 1);
        plan.file_group.output_parallelism = 4;

        let estimate = estimate_plan_memory(&plan, &schema, FormatVersion::V2, 1, false, true, 4);

        assert_eq!(
            estimate.preferred_pool_bytes,
            4 * DATAFUSION_SORT_SPILL_RESERVATION_BYTES
                + DATAFUSION_STREAMING_DECODED_WINDOW_BYTES
                + 8
        );
    }

    #[test]
    fn delete_files_are_accounted_from_file_group() {
        let schema = schema(vec![Type::Primitive(PrimitiveType::Int)]);
        let baseline_plan =
            plan_with_files(schema.clone(), 10 * 1024 * 1024, Some(100_000), vec![], 1);
        let baseline = estimate_plan_memory(
            &baseline_plan,
            &schema,
            FormatVersion::V2,
            1024,
            false,
            false,
            1,
        );

        let mut file_group = baseline_plan.file_group.clone();
        file_group.position_delete_files = vec![scan_task(
            schema.clone(),
            "position-delete.parquet",
            DataContentType::PositionDeletes,
            1024 * 1024,
            Some(10_000),
        )];
        file_group.equality_delete_files = vec![scan_task(
            schema.clone(),
            "equality-delete.parquet",
            DataContentType::EqualityDeletes,
            1024 * 1024,
            Some(10_000),
        )];
        let plan = CompactionPlan::new(file_group, "main", 1);
        let v2 = estimate_plan_memory(&plan, &schema, FormatVersion::V2, 1024, false, false, 1);
        let v3 = estimate_plan_memory(&plan, &schema, FormatVersion::V3, 1024, false, false, 1);

        assert!(v2.preferred_pool_bytes >= baseline.preferred_pool_bytes + 18 * 1024 * 1024);
        assert!(v2.preferred_pool_bytes >= v3.preferred_pool_bytes + 10 * 1024 * 1024);
    }
}
