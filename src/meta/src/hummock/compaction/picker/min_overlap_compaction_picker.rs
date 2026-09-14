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

use std::sync::Arc;

use risingwave_hummock_sdk::level::{InputLevel, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::hummock::LevelType;

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    level: usize,
    target_level: usize,
    max_select_bytes: u64,
    vnode_partition_count: u32,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl MinOverlappingPicker {
    pub fn new(
        level: usize,
        target_level: usize,
        max_select_bytes: u64,
        vnode_partition_count: u32,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            level,
            target_level,
            max_select_bytes,
            vnode_partition_count,
            overlap_strategy,
        }
    }

    pub fn pick_tables(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        level_handlers: &[LevelHandler],
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let mut select_file_ranges = vec![];
        for (idx, sst) in select_tables.iter().enumerate() {
            if level_handlers[self.level].is_pending_compact(&sst.sst_id) {
                continue;
            }
            let mut overlap_info = self.overlap_strategy.create_overlap_info();
            overlap_info.update(&sst.key_range);
            let overlap_files_range = overlap_info.check_multiple_overlap(target_tables);

            if overlap_files_range.is_empty() {
                return (vec![sst.clone()], vec![]);
            }
            select_file_ranges.push((idx, overlap_files_range));
        }
        select_file_ranges.retain(|(_, range)| {
            let mut pending_compact = false;
            for other in &target_tables[range.clone()] {
                if level_handlers[self.target_level].is_pending_compact(&other.sst_id) {
                    pending_compact = true;
                    break;
                }
            }
            !pending_compact
        });

        let mut select_prefix_sizes = None;
        let mut same_overlap_ends = vec![];
        let mut tried_skipping = false;

        let mut min_score = u64::MAX;
        let mut min_score_select_range = 0..0;
        let mut min_score_target_range = 0..0;
        let mut min_score_select_file_size = 0;
        for left in 0..select_file_ranges.len() {
            let mut select_file_size = 0;
            let mut target_level_overlap_range = select_file_ranges[left].1.clone();
            let mut total_file_size = 0;
            for other in &target_tables[target_level_overlap_range.clone()] {
                total_file_size += other.sst_size;
            }
            let start_idx = select_file_ranges[left].0;
            let mut end_idx = start_idx + 1;
            let mut right = left;
            while right < select_file_ranges.len() {
                let (idx, range) = &select_file_ranges[right];
                if select_file_size > self.max_select_bytes
                    || *idx > end_idx
                    || range.start >= target_level_overlap_range.end
                {
                    break;
                }
                select_file_size += select_tables[*idx].sst_size;
                if range.end > target_level_overlap_range.end {
                    for other in &target_tables[target_level_overlap_range.end..range.end] {
                        total_file_size += other.sst_size;
                    }
                    target_level_overlap_range.end = range.end;
                }
                let numerator = total_file_size * 100;
                end_idx = idx + 1;
                // Build search metadata lazily, after finding eight equal-overlap
                // windows. Short windows keep the original allocation behavior.
                if !tried_skipping
                    && right >= left + 7
                    && select_file_size <= self.max_select_bytes
                    && select_file_ranges[right - 7..right]
                        .iter()
                        .all(|(_, previous_range)| previous_range == range)
                {
                    tried_skipping = true;
                    let mut sums = Vec::with_capacity(select_file_ranges.len() + 1);
                    sums.push(0_u64);
                    // Zero sizes and overflowing sums retain exhaustive enumeration.
                    select_prefix_sizes =
                        select_file_ranges
                            .iter()
                            .try_fold(sums, |mut sums, (idx, _)| {
                                let size = select_tables[*idx].sst_size;
                                if size == 0 {
                                    return None;
                                }
                                sums.push(sums.last()?.checked_add(size)?);
                                Some(sums)
                            });
                    if select_prefix_sizes.is_some() {
                        same_overlap_ends = (1..=select_file_ranges.len()).collect::<Vec<_>>();
                        for i in (0..select_file_ranges.len() - 1).rev() {
                            if select_file_ranges[i].0 + 1 == select_file_ranges[i + 1].0
                                && select_file_ranges[i].1 == select_file_ranges[i + 1].1
                            {
                                same_overlap_ends[i] = same_overlap_ends[i + 1];
                            }
                        }
                    }
                }
                if let Some(sums) = &select_prefix_sizes
                    && same_overlap_ends[right] >= right + 8
                    && sums[right + 7] - sums[left] <= self.max_select_bytes
                {
                    let base = sums[left];
                    // The limit is checked BEFORE adding the next SST, so include
                    // the first SST that takes the window over the limit.
                    let end = right
                        + 1
                        + sums[right + 1..same_overlap_ends[right]]
                            .partition_point(|size| size - base <= self.max_select_bytes);
                    select_file_size = sums[end] - base;
                    // With a constant numerator, the furthest legal endpoint has
                    // the lowest score and the largest source size on a score tie.
                    end_idx = select_file_ranges[end - 1].0 + 1;
                    right = end;
                } else {
                    right += 1;
                }
                let score = numerator
                    .checked_div(select_file_size)
                    .unwrap_or(total_file_size);
                if score < min_score
                    || (score == min_score && select_file_size > min_score_select_file_size)
                {
                    min_score = score;
                    min_score_select_range = start_idx..end_idx;
                    min_score_target_range = target_level_overlap_range.clone();
                    min_score_select_file_size = select_file_size;
                }
            }
        }
        if min_score == u64::MAX {
            return (vec![], vec![]);
        }
        let select_input_ssts = select_tables[min_score_select_range].to_vec();
        let target_input_ssts = target_tables[min_score_target_range].to_vec();
        (select_input_ssts, target_input_ssts)
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        assert!(self.level > 0);
        let (select_input_ssts, target_input_ssts) = self.pick_tables(
            &levels.get_level(self.level).table_infos,
            &levels.get_level(self.target_level).table_infos,
            level_handlers,
        );
        if select_input_ssts.is_empty() {
            stats.skip_by_pending_files += 1;
            return None;
        }
        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            target_input_size: target_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: (select_input_ssts.len() + target_input_ssts.len()) as u64,
            input_levels: vec![
                InputLevel {
                    level_idx: self.level as u32,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: target_input_ssts,
                },
            ],
            target_level: self.target_level,
            vnode_partition_count: self.vnode_partition_count,
            ..Default::default()
        })
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_hummock_sdk::key_range::KeyRangeCommon;
    use risingwave_hummock_sdk::level::Level;

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_table,
    };

    fn sized_table(id: u64, left: usize, right: usize, size: u64) -> SstableInfo {
        let mut table =
            crate::hummock::compaction::selector::tests::generate_table_impl(id, 1, left, right, 1);
        table.sst_size = size;
        // Fixed-width keys preserve byte ordering beyond the shared helper's five-digit range.
        let key = |idx: usize| {
            risingwave_hummock_sdk::key::FullKey::for_test(1.into(), (idx as u64).to_be_bytes(), 1)
                .encode()
        };
        table.key_range.left = key(left).into();
        table.key_range.right = key(right).into();
        table.into()
    }

    #[test]
    fn test_equal_overlap_window_ties_and_limit() {
        let source: Vec<_> = (0..16)
            .map(|i| sized_table(i, i as usize * 2, i as usize * 2 + 1, 20))
            .collect();
        let handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        for (target_size, limit, expected_len) in [
            (1, u64::MAX, 16), // Prefer the largest window on the score-zero plateau.
            (1000, u64::MAX, 16),
            (1000, 160, 9),
            (1000, 0, 1),
            (1000, 20, 2),
            (1000, 99, 5),
            (1000, 100, 6), // Preserve the last SST that takes the window over the limit.
        ] {
            let picker = MinOverlappingPicker::new(
                1,
                2,
                limit,
                0,
                Arc::new(RangeOverlapStrategy::default()),
            );
            let target = vec![sized_table(100, 0, 100, target_size)];
            let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
            assert_eq!(selected, source[..expected_len]);
            assert_eq!(overlapped, target);
        }
    }

    #[test]
    fn test_score_then_source_size_then_encounter_order() {
        let handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        for (second_source_size, second_target_size, expected_idx) in [
            (1000, 1009, 1), // Equal integer scores: prefer more source bytes.
            (1000, 1010, 0), // A lower score still wins over a larger source.
            (100, 100, 0),   // Equal score and size: preserve encounter order.
        ] {
            let source = vec![
                sized_table(0, 0, 9, 100),
                sized_table(1, 10, 19, second_source_size),
            ];
            let target = vec![
                sized_table(100, 0, 9, 100),
                sized_table(101, 10, 19, second_target_size),
            ];
            let picker = MinOverlappingPicker::new(
                1,
                2,
                u64::MAX,
                0,
                Arc::new(RangeOverlapStrategy::default()),
            );
            let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
            // Adjacent disjoint target ranges remain separate candidates.
            assert_eq!(selected, source[expected_idx..expected_idx + 1]);
            assert_eq!(overlapped, target[expected_idx..expected_idx + 1]);
        }
    }

    #[test]
    fn test_equal_overlap_pending_gap() {
        let source: Vec<_> = (0..16)
            .map(|i| sized_table(i, i as usize * 2, i as usize * 2 + 1, 20))
            .collect();
        let target = vec![sized_table(100, 0, 100, 1)];
        let mut handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        handlers[1].test_add_pending_sst(source[4].sst_id, 1);
        let picker =
            MinOverlappingPicker::new(1, 2, u64::MAX, 0, Arc::new(RangeOverlapStrategy::default()));
        let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
        assert_eq!(selected, source[5..16]);
        assert_eq!(overlapped, target);
        handlers[2].test_add_pending_sst(target[0].sst_id, 2);
        assert_eq!(
            picker.pick_tables(&source, &target, &handlers),
            (vec![], vec![])
        );
    }

    #[test]
    fn test_equal_overlap_zero_and_overflowing_prefix() {
        let handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        for size in [0, u64::MAX] {
            let source: Vec<_> = (0..8)
                .map(|i| sized_table(i, i as usize * 2, i as usize * 2 + 1, size))
                .collect();
            let target = vec![sized_table(100, 0, 100, 0)];
            let picker =
                MinOverlappingPicker::new(1, 2, 0, 0, Arc::new(RangeOverlapStrategy::default()));
            let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
            assert_eq!(selected, source[..1]);
            assert_eq!(overlapped, target);
        }
    }

    #[test]
    fn test_equal_overlap_overflow_outside_window() {
        let source: Vec<_> = (0..18)
            .map(|i| {
                sized_table(
                    i,
                    i as usize * 2,
                    i as usize * 2 + 1,
                    if i >= 16 { u64::MAX } else { 1 },
                )
            })
            .collect();
        let target = vec![sized_table(100, 0, 100, 0)];
        let mut handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        handlers[1].test_add_pending_sst(source[15].sst_id, 1);
        let picker =
            MinOverlappingPicker::new(1, 2, 8, 0, Arc::new(RangeOverlapStrategy::default()));
        // No legal window overflows, even though the prefix over all candidates would.
        let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
        assert_eq!(selected, source[16..17]);
        assert_eq!(overlapped, target);
    }

    #[test]
    fn test_equal_overlap_expansion_after_skipping() {
        let source: Vec<_> = (0..16)
            .map(|i| {
                sized_table(
                    i,
                    i as usize * 2,
                    i as usize * 2 + 1,
                    if i == 8 { 10000 } else { 100 },
                )
            })
            .collect();
        let target = vec![
            sized_table(100, 0, 16, 1000),
            sized_table(101, 17, 100, 1000),
        ];
        let handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
        let picker =
            MinOverlappingPicker::new(1, 2, u64::MAX, 0, Arc::new(RangeOverlapStrategy::default()));
        let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
        // The bridging SST expands the target range. Prefer all 11600 bytes at score 17.
        assert_eq!(selected, source);
        assert_eq!(overlapped, target);
    }

    // Run with cargo test --release -p risingwave_meta bench_min_overlap -- --ignored --nocapture --test-threads=1.
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_min_overlap() {
        use std::hint::black_box;
        use std::time::Duration;
        let mut c = criterion::Criterion::default()
            .sample_size(30)
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(1))
            .without_plots();
        for (name, n, group, limit, pending) in [
            ("small", 8, 1, u64::MAX, false),
            ("one_to_one", 10000, 1, u64::MAX, false),
            ("short_runs", 10000, 7, u64::MAX, false),
            ("tiny_limit", 10000, 10000, 4, false),
            ("fan_in_1000", 1000, 1000, u64::MAX, false),
            ("fan_in_10000", 10000, 10000, u64::MAX, false),
            ("fan_in_100000", 100000, 100000, u64::MAX, false),
            ("limited", 10000, 10000, 128, false),
            ("pending_gaps", 10000, 10000, u64::MAX, true),
            ("mixed", 10000, 20, 2048, false),
        ] {
            let source: Vec<_> = (0..n)
                .map(|i| sized_table(i as u64, i * 4, i * 4 + 3, 1 + (i % 7) as u64))
                .collect();
            let target: Vec<_> = (0..n.div_ceil(group))
                .map(|i| {
                    sized_table(
                        100000 + i as u64,
                        if name == "mixed" && i > 0 {
                            i * group * 4 + 2
                        } else {
                            i * group * 4
                        },
                        if name == "mixed" {
                            (i + 1) * group * 4 + 1
                        } else {
                            (i + 1) * group * 4 - 1
                        },
                        1000,
                    )
                })
                .collect();
            for tables in [&source, &target] {
                assert!(tables.windows(2).all(|pair| {
                    pair[0]
                        .key_range
                        .compare_right_with(&pair[1].key_range.left)
                        == std::cmp::Ordering::Less
                }));
            }
            let mut handlers: Vec<_> = (0..3).map(LevelHandler::new).collect();
            if pending {
                for i in (99..n).step_by(100) {
                    handlers[1].test_add_pending_sst((i as u64).into(), 1);
                }
            }
            let picker = MinOverlappingPicker::new(
                1,
                2,
                limit,
                0,
                Arc::new(RangeOverlapStrategy::default()),
            );
            let (selected, overlapped) = picker.pick_tables(&source, &target, &handlers);
            assert!(
                !overlapped.is_empty(),
                "fixture must not take the trivial-move path"
            );
            if name.starts_with("fan_in_") {
                assert_eq!(selected, source);
            }
            c.bench_function(&format!("min_overlap/{name}"), |b| {
                b.iter(|| {
                    black_box(picker.pick_tables(
                        black_box(&source),
                        black_box(&target),
                        black_box(&handlers),
                    ))
                })
            });
        }
        c.final_summary();
    }

    #[test]
    fn test_compact_l1() {
        let mut picker =
            MinOverlappingPicker::new(1, 2, 10000, 0, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                ..Default::default()
            },
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let mut level_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 0);
        ret.add_pending_task(0, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 0);
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 5);
    }

    #[test]
    fn test_expand_l1_files() {
        let mut picker =
            MinOverlappingPicker::new(1, 2, 10000, 0, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                ..Default::default()
            },
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let ret = picker
            .pick_compaction(
                &levels,
                &levels_handler,
                &mut LocalPickerStatistic::default(),
            )
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.input_levels[1].level_idx, 2);

        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 0);
        assert_eq!(ret.input_levels[0].table_infos[1].sst_id, 1);

        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 4);
    }

    #[test]
    fn test_trivial_move_bug() {
        let levels = [
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![generate_table(0, 1, 400, 500, 2)],
                total_file_size: 100,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 600, 700, 1),
                ],
                total_file_size: 200,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(3, 1, 100, 300, 2),
                    generate_table(4, 1, 600, 800, 1),
                ],
                total_file_size: 400,
                ..Default::default()
            },
        ];

        let levels_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
        ];
        // no limit
        let picker =
            MinOverlappingPicker::new(2, 3, 1000, 0, Arc::new(RangeOverlapStrategy::default()));
        let (select_files, target_files) = picker.pick_tables(
            &levels[1].table_infos,
            &levels[2].table_infos,
            &levels_handlers,
        );
        let overlap_strategy = Arc::new(RangeOverlapStrategy::default());
        let mut overlap_info = overlap_strategy.create_overlap_info();
        for sst in &select_files {
            overlap_info.update(&sst.key_range);
        }
        let range = overlap_info.check_multiple_overlap(&levels[0].table_infos);
        assert!(range.is_empty());
        assert_eq!(select_files.len(), 1);
        assert_eq!(target_files.len(), 1);
    }
}
