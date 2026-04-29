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

use std::collections::BTreeMap;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::level::{InputLevel, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::ReadTableWatermark;

use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct VnodeWatermarkCompactionPicker {}

impl VnodeWatermarkCompactionPicker {
    pub fn new() -> Self {
        Self {}
    }

    /// The current implementation only picks trivial reclaim tasks.
    /// Must modify `is_trivial_reclaim`, if non-trivial reclaim is supported in the future.
    pub fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
    ) -> Option<CompactionInput> {
        let mut input_levels = vec![];
        let mut select_input_size = 0;
        let mut total_file_count = 0;
        let mut target_level = None;

        for level in &levels.levels {
            let table_infos: Vec<_> = level
                .table_infos
                .iter()
                .filter(|sst_info| {
                    !level_handlers[level.level_idx as usize].is_pending_compact(&sst_info.sst_id)
                        && should_trivial_reclaim_sst_by_watermark(sst_info, table_watermarks)
                })
                .cloned()
                .collect();

            if table_infos.is_empty() {
                continue;
            }

            select_input_size += table_infos.iter().map(|sst| sst.sst_size).sum::<u64>();
            total_file_count += table_infos.len() as u64;
            target_level = Some((level.level_idx, level.sub_level_id));
            input_levels.push(InputLevel {
                level_idx: level.level_idx,
                level_type: level.level_type,
                table_infos,
            });
        }

        let (target_level_idx, target_sub_level_id) = target_level?;
        if total_file_count == 0 {
            return None;
        }

        Some(CompactionInput {
            select_input_size,
            total_file_count,
            input_levels,
            target_level: target_level_idx as usize,
            target_sub_level_id,
            ..Default::default()
        })
    }
}

fn should_trivial_reclaim_sst_by_watermark(
    sst_info: &SstableInfo,
    table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
) -> bool {
    let Some(max_seen_watermark) = sst_info.max_seen_watermark.as_ref() else {
        return false;
    };
    let [table_id] = sst_info.table_ids.as_slice() else {
        return false;
    };
    let Some(watermarks) = table_watermarks.get(table_id) else {
        return false;
    };
    let Some(table_watermark) = table_watermark_for_reclaim(watermarks) else {
        return false;
    };
    watermarks
        .direction
        .key_filter_by_watermark(max_seen_watermark, table_watermark)
}

fn table_watermark_for_reclaim(watermarks: &ReadTableWatermark) -> Option<&bytes::Bytes> {
    match watermarks.direction {
        risingwave_hummock_sdk::table_watermark::WatermarkDirection::Ascending => {
            watermarks.vnode_watermarks.values().max()
        }
        risingwave_hummock_sdk::table_watermark::WatermarkDirection::Descending => {
            watermarks.vnode_watermarks.values().min()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::level::Levels;
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, WatermarkDirection};

    use crate::hummock::compaction::picker::vnode_watermark_picker::{
        VnodeWatermarkCompactionPicker, should_trivial_reclaim_sst_by_watermark,
        table_watermark_for_reclaim,
    };
    use crate::hummock::compaction::selector::tests::{generate_level, generate_table_impl};
    use crate::hummock::level_handler::LevelHandler;

    #[test]
    fn test_should_trivial_reclaim_sst_by_watermark() {
        let table_watermarks = maplit::btreemap! {
            1.into() => ReadTableWatermark {
                direction: WatermarkDirection::Ascending,
                vnode_watermarks: maplit::btreemap! {
                    VirtualNode::from_index(16) => Bytes::from_static(b"some_watermark_key_8"),
                    VirtualNode::from_index(17) => Bytes::from_static(b"some_watermark_key_9"),
                },
            },
        };

        let sst_info = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![2.into()],
            max_seen_watermark: Some(Bytes::from_static(b"some_watermark_key_1")),
            ..Default::default()
        }
        .into();
        assert!(
            !should_trivial_reclaim_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because no matching watermark found"
        );

        let sst_info = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![1.into(), 2.into()],
            max_seen_watermark: Some(Bytes::from_static(b"some_watermark_key_1")),
            ..Default::default()
        }
        .into();
        assert!(
            !should_trivial_reclaim_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because max_seen_watermark is only tracked for single-table SSTs"
        );

        let sst_info = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![1.into()],
            max_seen_watermark: None,
            ..Default::default()
        }
        .into();
        assert!(
            !should_trivial_reclaim_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because max_seen_watermark is absent"
        );

        let sst_info = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![1.into()],
            max_seen_watermark: Some(Bytes::from_static(b"some_watermark_key_9")),
            ..Default::default()
        }
        .into();
        assert!(
            !should_trivial_reclaim_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because max_seen_watermark is not filtered by the table watermark"
        );

        let sst_info = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![1.into()],
            max_seen_watermark: Some(Bytes::from_static(b"some_watermark_key_8")),
            ..Default::default()
        }
        .into();
        assert!(
            should_trivial_reclaim_sst_by_watermark(&sst_info, &table_watermarks),
            "should use the max watermark across all vnodes of the table"
        );
    }

    #[test]
    fn test_table_watermark_for_reclaim_respects_direction() {
        let ascending = ReadTableWatermark {
            direction: WatermarkDirection::Ascending,
            vnode_watermarks: maplit::btreemap! {
                VirtualNode::from_index(1) => Bytes::from_static(b"key_8"),
                VirtualNode::from_index(2) => Bytes::from_static(b"key_9"),
            },
        };
        assert_eq!(
            table_watermark_for_reclaim(&ascending),
            Some(&Bytes::from_static(b"key_9"))
        );

        let descending = ReadTableWatermark {
            direction: WatermarkDirection::Descending,
            vnode_watermarks: maplit::btreemap! {
                VirtualNode::from_index(1) => Bytes::from_static(b"key_8"),
                VirtualNode::from_index(2) => Bytes::from_static(b"key_9"),
            },
        };
        assert_eq!(
            table_watermark_for_reclaim(&descending),
            Some(&Bytes::from_static(b"key_8"))
        );
    }

    #[test]
    fn test_pick_compaction_scans_all_levels() {
        let level_2_sst = SstableInfoInner {
            max_seen_watermark: Some(Bytes::from_static(b"key_8")),
            ..generate_table_impl(1, 1, 0, 100, 1)
        }
        .into();
        let level_3_sst = SstableInfoInner {
            max_seen_watermark: Some(Bytes::from_static(b"key_8")),
            ..generate_table_impl(2, 1, 101, 200, 1)
        }
        .into();
        let level_4_sst = SstableInfoInner {
            max_seen_watermark: Some(Bytes::from_static(b"key_9")),
            ..generate_table_impl(3, 1, 201, 300, 1)
        }
        .into();

        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![level_2_sst]),
                generate_level(3, vec![level_3_sst]),
                generate_level(4, vec![level_4_sst]),
            ],
            ..Default::default()
        };
        let level_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
        ];
        let table_watermarks = BTreeMap::from([(
            1.into(),
            ReadTableWatermark {
                direction: WatermarkDirection::Ascending,
                vnode_watermarks: maplit::btreemap! {
                    VirtualNode::from_index(1) => Bytes::from_static(b"key_9"),
                },
            },
        )]);

        let mut picker = VnodeWatermarkCompactionPicker::new();
        let ret = picker
            .pick_compaction(&levels, &level_handlers, &table_watermarks)
            .unwrap();

        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(ret.input_levels[0].level_idx, 2);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 1);
        assert_eq!(ret.input_levels[1].level_idx, 3);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 2);
        assert_eq!(ret.target_level, 3);
        assert_eq!(ret.total_file_count, 2);
    }
}
