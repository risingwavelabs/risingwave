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

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_pb::hummock::table_watermarks::PbEpochNewWatermarks;
use risingwave_pb::hummock::{PbTableWatermarks, PbVnodeWatermark};
use tracing::debug;

use crate::HummockEpoch;

#[derive(Clone)]
pub struct ReadTableWatermark {
    pub direction: WatermarkDirection,
    pub vnode_watermarks: BTreeMap<VirtualNode, Bytes>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
}

impl WatermarkDirection {
    pub fn filter_by_watermark(&self, key: impl AsRef<[u8]>, watermark: impl AsRef<[u8]>) -> bool {
        let key = key.as_ref();
        let watermark = watermark.as_ref();
        match self {
            WatermarkDirection::Ascending => key < watermark,
            WatermarkDirection::Descending => key > watermark,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VnodeWatermark {
    vnode_bitmap: Arc<Bitmap>,
    watermark: Bytes,
}

impl VnodeWatermark {
    pub fn new(vnode_bitmap: Arc<Bitmap>, watermark: Bytes) -> Self {
        Self {
            vnode_bitmap,
            watermark,
        }
    }

    pub fn to_protobuf(&self) -> PbVnodeWatermark {
        PbVnodeWatermark {
            watermark: self.watermark.to_vec(),
            vnode_bitmap: Some(self.vnode_bitmap.to_protobuf()),
        }
    }

    pub fn from_protobuf(pb: &PbVnodeWatermark) -> Self {
        Self {
            vnode_bitmap: Arc::new(Bitmap::from(pb.vnode_bitmap.as_ref().unwrap())),
            watermark: Bytes::from(pb.watermark.clone()),
        }
    }

    pub fn vnode_bitmap(&self) -> &Bitmap {
        &self.vnode_bitmap
    }
}

#[derive(Clone, Debug)]
pub struct TableWatermarks {
    // later epoch at the back
    watermarks: Vec<(HummockEpoch, Vec<VnodeWatermark>)>,
    direction: WatermarkDirection,
}

impl TableWatermarks {
    pub fn single_epoch(
        epoch: HummockEpoch,
        watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) -> Self {
        Self {
            direction,
            watermarks: vec![(epoch, watermarks)],
        }
    }

    pub fn to_protobuf(&self) -> PbTableWatermarks {
        PbTableWatermarks {
            epoch_watermarks: self
                .watermarks
                .iter()
                .map(|(epoch, watermarks)| PbEpochNewWatermarks {
                    watermarks: watermarks.iter().map(VnodeWatermark::to_protobuf).collect(),
                    epoch: *epoch,
                })
                .collect(),
            is_ascending: match self.direction {
                WatermarkDirection::Ascending => true,
                WatermarkDirection::Descending => false,
            },
        }
    }

    pub fn add_new_epoch_watermarks(
        &mut self,
        epoch: HummockEpoch,
        watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) {
        assert_eq!(self.direction, direction);
        if let Some((prev_epoch, _)) = self.watermarks.last() {
            assert!(*prev_epoch < epoch);
        }
        self.watermarks.push((epoch, watermarks));
    }

    pub fn from_protobuf(pb: &PbTableWatermarks) -> Self {
        Self {
            watermarks: pb
                .epoch_watermarks
                .iter()
                .map(|epoch_watermark| {
                    let epoch = epoch_watermark.epoch;
                    let watermarks = epoch_watermark
                        .watermarks
                        .iter()
                        .map(VnodeWatermark::from_protobuf)
                        .collect();
                    (epoch, watermarks)
                })
                .collect(),
            direction: if pb.is_ascending {
                WatermarkDirection::Ascending
            } else {
                WatermarkDirection::Descending
            },
        }
    }
}

pub fn merge_multiple_new_table_watermarks(
    table_watermarks_list: impl IntoIterator<Item = HashMap<u64, PbTableWatermarks>>,
) -> HashMap<u64, PbTableWatermarks> {
    let mut ret: HashMap<u64, (bool, BTreeMap<u64, PbEpochNewWatermarks>)> = HashMap::new();
    for table_watermarks in table_watermarks_list {
        for (table_id, new_table_watermarks) in table_watermarks {
            let epoch_watermarks = match ret.entry(table_id) {
                Entry::Occupied(entry) => {
                    let (is_ascending, epoch_watermarks) = entry.into_mut();
                    assert_eq!(new_table_watermarks.is_ascending, *is_ascending);
                    epoch_watermarks
                }
                Entry::Vacant(entry) => {
                    let (_, epoch_watermarks) =
                        entry.insert((new_table_watermarks.is_ascending, BTreeMap::new()));
                    epoch_watermarks
                }
            };
            for new_epoch_watermarks in new_table_watermarks.epoch_watermarks {
                epoch_watermarks
                    .entry(new_epoch_watermarks.epoch)
                    .or_insert_with(|| PbEpochNewWatermarks {
                        watermarks: vec![],
                        epoch: new_epoch_watermarks.epoch,
                    })
                    .watermarks
                    .extend(new_epoch_watermarks.watermarks);
            }
        }
    }
    ret.into_iter()
        .map(|(table_id, (is_ascending, epoch_watermarks))| {
            (
                table_id,
                PbTableWatermarks {
                    is_ascending,
                    // ordered from earlier epoch to later epoch
                    epoch_watermarks: epoch_watermarks.into_values().collect(),
                },
            )
        })
        .collect()
}

#[easy_ext::ext(PbTableWatermarksExt)]
impl PbTableWatermarks {
    pub fn apply_new_table_watermarks(&mut self, newly_added_watermarks: &PbTableWatermarks) {
        assert_eq!(self.is_ascending, newly_added_watermarks.is_ascending);
        assert!(self.epoch_watermarks.iter().map(|w| w.epoch).is_sorted());
        assert!(newly_added_watermarks
            .epoch_watermarks
            .iter()
            .map(|w| w.epoch)
            .is_sorted());
        // ensure that the newly added watermarks have a later epoch than the previous latest epoch.
        if let Some(prev_last_epoch_watermarks) = self.epoch_watermarks.last() && let Some(new_first_epoch_watermarks) = newly_added_watermarks.epoch_watermarks.first() {
            assert!(prev_last_epoch_watermarks.epoch < new_first_epoch_watermarks.epoch);
        }
        self.epoch_watermarks
            .extend(newly_added_watermarks.epoch_watermarks.clone());
    }

    pub fn clear_stale_epoch_watermark(&mut self, safe_epoch: u64) {
        match self.epoch_watermarks.first() {
            None => {
                // return on empty watermark
                return;
            }
            Some(earliest_epoch_watermark) => {
                if earliest_epoch_watermark.epoch >= safe_epoch {
                    // No stale epoch watermark needs to be cleared.
                    return;
                }
            }
        }
        debug!("clear stale table watermark below epoch {}", safe_epoch);
        let mut result_epoch_watermark = Vec::with_capacity(self.epoch_watermarks.len());
        let mut unset_vnode: HashSet<VirtualNode> = (0..VirtualNode::COUNT)
            .map(VirtualNode::from_index)
            .collect();
        while let Some(epoch_watermark) = self.epoch_watermarks.last() {
            if epoch_watermark.epoch >= safe_epoch {
                let epoch_watermark = self.epoch_watermarks.pop().expect("have check Some");
                for watermark in &epoch_watermark.watermarks {
                    for vnode in
                        Bitmap::from(watermark.vnode_bitmap.as_ref().expect("should not be None"))
                            .iter_vnodes()
                    {
                        unset_vnode.remove(&vnode);
                    }
                }
                result_epoch_watermark.push(epoch_watermark);
            } else {
                break;
            }
        }
        while !unset_vnode.is_empty() && let Some(epoch_watermark) = self.epoch_watermarks.pop() {
            let mut new_vnode_watermarks = Vec::new();
            for vnode_watermark in &epoch_watermark.watermarks {
                let mut set_vnode = Vec::new();
                for vnode in Bitmap::from(vnode_watermark.vnode_bitmap.as_ref().expect("should not be None")).iter_vnodes() {
                    if unset_vnode.remove(&vnode) {
                        set_vnode.push(vnode);
                    }
                }
                if !set_vnode.is_empty() {
                    let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
                    for vnode in set_vnode {
                        builder.set(vnode.to_index(), true);
                    }
                    let bitmap = builder.finish();
                    new_vnode_watermarks.push(PbVnodeWatermark {
                        vnode_bitmap: Some(bitmap.to_protobuf()),
                        watermark: vnode_watermark.watermark.clone(),
                    })
                }
            }
            if !new_vnode_watermarks.is_empty() {
                if let Some(last_epoch_watermark) = result_epoch_watermark.last_mut() && last_epoch_watermark.epoch == safe_epoch {
                    last_epoch_watermark.watermarks.extend(new_vnode_watermarks);
                } else {
                    result_epoch_watermark.push(PbEpochNewWatermarks {
                        watermarks: new_vnode_watermarks,
                        // set epoch as safe epoch
                        epoch: safe_epoch,
                    })
                }
            }
        }
        // epoch watermark are added from later epoch to earlier epoch.
        // reverse to ensure that earlier epochs are at the front
        result_epoch_watermark.reverse();
        assert!(result_epoch_watermark.is_sorted_by(|first, second| {
            let ret = first.epoch.cmp(&second.epoch);
            assert_ne!(ret, Ordering::Equal);
            Some(ret)
        }));
        *self = PbTableWatermarks {
            epoch_watermarks: result_epoch_watermark,
            is_ascending: self.is_ascending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::vec;

    use bytes::Bytes;
    use risingwave_common::buffer::{Bitmap, BitmapBuilder};
    use risingwave_common::hash::VirtualNode;
    use risingwave_pb::hummock::table_watermarks::PbEpochNewWatermarks;
    use risingwave_pb::hummock::{PbTableWatermarks, PbVnodeWatermark};

    use crate::table_watermark::{
        merge_multiple_new_table_watermarks, PbTableWatermarksExt, TableWatermarks, VnodeWatermark,
        WatermarkDirection,
    };

    fn build_bitmap(vnodes: impl IntoIterator<Item = usize>) -> Arc<Bitmap> {
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
        for vnode in vnodes {
            builder.set(vnode, true);
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn test_apply_new_table_watermark() {
        let epoch1 = 233;
        let direction = WatermarkDirection::Ascending;
        let watermark1 = Bytes::from("watermark1");
        let watermark2 = Bytes::from("watermark2");
        let watermark3 = Bytes::from("watermark3");
        let watermark4 = Bytes::from("watermark4");
        let mut table_watermarks = TableWatermarks::single_epoch(
            epoch1,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2]),
                watermark1.clone(),
            )],
            direction,
        );
        let epoch2 = epoch1 + 1;
        table_watermarks.add_new_epoch_watermarks(
            epoch2,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2, 3]),
                watermark2.clone(),
            )],
            direction,
        );

        let mut pb_table_watermark = table_watermarks.to_protobuf();

        let epoch3 = epoch2 + 1;
        let mut second_table_watermark = TableWatermarks::single_epoch(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT),
                watermark3.clone(),
            )],
            direction,
        );
        table_watermarks.add_new_epoch_watermarks(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT),
                watermark3.clone(),
            )],
            direction,
        );
        let epoch4 = epoch3 + 1;
        let epoch5 = epoch4 + 1;
        table_watermarks.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )],
            direction,
        );
        second_table_watermark.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )],
            direction,
        );

        pb_table_watermark.apply_new_table_watermarks(&second_table_watermark.to_protobuf());
        assert_eq!(table_watermarks.to_protobuf(), pb_table_watermark);
    }

    #[test]
    fn test_clear_stale_epoch_watmermark() {
        let epoch1 = 233;
        let direction = WatermarkDirection::Ascending;
        let watermark1 = Bytes::from("watermark1");
        let watermark2 = Bytes::from("watermark2");
        let watermark3 = Bytes::from("watermark3");
        let watermark4 = Bytes::from("watermark4");
        let mut table_watermarks = TableWatermarks::single_epoch(
            epoch1,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2]),
                watermark1.clone(),
            )],
            direction,
        );
        let epoch2 = epoch1 + 1;
        table_watermarks.add_new_epoch_watermarks(
            epoch2,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2, 3]),
                watermark2.clone(),
            )],
            direction,
        );
        let epoch3 = epoch2 + 1;
        table_watermarks.add_new_epoch_watermarks(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT),
                watermark3.clone(),
            )],
            direction,
        );
        let epoch4 = epoch3 + 1;
        let epoch5 = epoch4 + 1;
        table_watermarks.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )],
            direction,
        );

        let mut pb_table_watermarks = table_watermarks.to_protobuf();
        pb_table_watermarks.clear_stale_epoch_watermark(epoch1);
        assert_eq!(pb_table_watermarks, table_watermarks.to_protobuf());

        pb_table_watermarks.clear_stale_epoch_watermark(epoch2);
        assert_eq!(
            pb_table_watermarks,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch2,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 1, 2, 3]),
                            watermark2.clone(),
                        )]
                    ),
                    (
                        epoch3,
                        vec![VnodeWatermark::new(
                            build_bitmap(0..VirtualNode::COUNT),
                            watermark3.clone(),
                        )]
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                    )
                ],
                direction,
            }
            .to_protobuf()
        );

        pb_table_watermarks.clear_stale_epoch_watermark(epoch3);
        assert_eq!(
            pb_table_watermarks,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch3,
                        vec![VnodeWatermark::new(
                            build_bitmap(0..VirtualNode::COUNT),
                            watermark3.clone(),
                        )]
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                    )
                ],
                direction,
            }
            .to_protobuf()
        );

        pb_table_watermarks.clear_stale_epoch_watermark(epoch4);
        assert_eq!(
            pb_table_watermarks,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch4,
                        vec![VnodeWatermark::new(
                            build_bitmap((1..3).chain(5..VirtualNode::COUNT)),
                            watermark3.clone()
                        )]
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                    )
                ],
                direction,
            }
            .to_protobuf()
        );

        pb_table_watermarks.clear_stale_epoch_watermark(epoch5);
        assert_eq!(
            pb_table_watermarks,
            TableWatermarks {
                watermarks: vec![(
                    epoch5,
                    vec![
                        VnodeWatermark::new(build_bitmap(vec![0, 3, 4]), watermark4.clone()),
                        VnodeWatermark::new(
                            build_bitmap((1..3).chain(5..VirtualNode::COUNT)),
                            watermark3.clone()
                        )
                    ]
                )],
                direction,
            }
            .to_protobuf()
        );
    }

    #[test]
    fn test_merge_multiple_new_table_watermarks() {
        fn epoch_new_watermark(epoch: u64, bitmaps: Vec<&Bitmap>) -> PbEpochNewWatermarks {
            PbEpochNewWatermarks {
                watermarks: bitmaps
                    .into_iter()
                    .map(|bitmap| PbVnodeWatermark {
                        watermark: vec![1, 2, epoch as _],
                        vnode_bitmap: Some(bitmap.to_protobuf()),
                    })
                    .collect(),
                epoch: epoch as _,
            }
        }
        fn build_table_watermark(
            vnodes: impl IntoIterator<Item = usize>,
            epochs: impl IntoIterator<Item = u64>,
        ) -> PbTableWatermarks {
            let bitmap = build_bitmap(vnodes);
            PbTableWatermarks {
                epoch_watermarks: epochs
                    .into_iter()
                    .map(|epoch: u64| epoch_new_watermark(epoch, vec![&bitmap]))
                    .collect(),
                is_ascending: true,
            }
        }
        let table1_watermark1 = build_table_watermark(0..3, vec![1, 2, 4]);
        let table1_watermark2 = build_table_watermark(4..6, vec![1, 2, 5]);
        let table2_watermark = build_table_watermark(0..4, 1..3);
        let table3_watermark = build_table_watermark(0..4, 3..5);
        let mut first = HashMap::new();
        first.insert(1, table1_watermark1);
        first.insert(2, table2_watermark.clone());
        let mut second = HashMap::new();
        second.insert(1, table1_watermark2);
        second.insert(3, table3_watermark.clone());
        let result = merge_multiple_new_table_watermarks(vec![first, second]);
        let mut expected = HashMap::new();
        expected.insert(
            1,
            PbTableWatermarks {
                epoch_watermarks: vec![
                    epoch_new_watermark(1, vec![&build_bitmap(0..3), &build_bitmap(4..6)]),
                    epoch_new_watermark(2, vec![&build_bitmap(0..3), &build_bitmap(4..6)]),
                    epoch_new_watermark(4, vec![&build_bitmap(0..3)]),
                    epoch_new_watermark(5, vec![&build_bitmap(4..6)]),
                ],
                is_ascending: true,
            },
        );
        expected.insert(2, table2_watermark);
        expected.insert(3, table3_watermark);
        assert_eq!(result, expected);
    }
}
