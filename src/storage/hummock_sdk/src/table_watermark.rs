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

use crate::HummockEpoch;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
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
            let ret = second.epoch.cmp(&first.epoch);
            assert_ne!(ret, Ordering::Equal);
            Some(ret)
        }));
        *self = PbTableWatermarks {
            epoch_watermarks: result_epoch_watermark,
            is_ascending: self.is_ascending,
        }
    }
}
