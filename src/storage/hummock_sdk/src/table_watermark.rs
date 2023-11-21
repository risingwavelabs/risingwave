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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;
use std::ops::Bound::Included;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::VirtualNode;
use risingwave_pb::hummock::table_watermarks::PbEpochNewWatermarks;
use risingwave_pb::hummock::vnodes::{AllVnodes, VnodeRange};
use risingwave_pb::hummock::{vnodes, PbTableWatermarks, PbVnodeWatermark, PbVnodes};
use tracing::warn;

use crate::HummockEpoch;

#[derive(Clone)]
pub struct TableWatermarksIndex {
    watermark_direction: WatermarkDirection,
    index: HashMap<VirtualNode, BTreeMap<HummockEpoch, Bytes>>,
    latest_epoch: HummockEpoch,
    committed_epoch: HummockEpoch,
}

impl TableWatermarksIndex {
    pub fn new(watermark_direction: WatermarkDirection, committed_epoch: HummockEpoch) -> Self {
        Self {
            watermark_direction,
            index: Default::default(),
            latest_epoch: committed_epoch,
            committed_epoch,
        }
    }

    pub fn table_watermark(&self, vnode: VirtualNode, epoch: HummockEpoch) -> Option<Bytes> {
        self.index.get(&vnode).and_then(|epoch_watermarks| {
            epoch_watermarks
                .upper_bound(Included(&epoch))
                .value()
                .cloned()
        })
    }

    pub fn latest_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.table_watermark(vnode, HummockEpoch::MAX)
    }

    pub fn filter_regress_watermarks(
        &self,
        watermarks: Vec<VnodeWatermark>,
    ) -> Vec<VnodeWatermark> {
        let mut ret = Vec::with_capacity(watermarks.len());
        for watermark in watermarks {
            let mut regress_vnodes = HashSet::new();
            for vnode in watermark.vnodes.vnodes() {
                if let Some(prev_watermark) = self.latest_watermark(vnode) {
                    let is_regress = match self.direction() {
                        WatermarkDirection::Ascending => prev_watermark > watermark.watermark,
                        WatermarkDirection::Descending => prev_watermark < watermark.watermark,
                    };
                    if is_regress {
                        warn!(
                            "table watermark regress: {:?} {} {:?} {:?}",
                            self.direction(),
                            vnode,
                            watermark.watermark,
                            prev_watermark
                        );
                        regress_vnodes.insert(vnode);
                    }
                }
            }
            if regress_vnodes.is_empty() {
                // no vnode has regress watermark
                ret.push(watermark);
            } else {
                let mut bitmap_builder = BitmapBuilder::with_capacity(VirtualNode::COUNT);
                for vnode in watermark.vnodes.vnodes() {
                    if !regress_vnodes.contains(&vnode) {
                        bitmap_builder.set(vnode.to_index(), true);
                    }
                }
                ret.push(VnodeWatermark::new(
                    Vnodes::Bitmap(Arc::new(bitmap_builder.finish())),
                    watermark.watermark,
                ));
            }
        }
        ret
    }

    pub fn direction(&self) -> WatermarkDirection {
        self.watermark_direction
    }

    pub fn add_epoch_watermark(
        &mut self,
        epoch: HummockEpoch,
        vnode_watermark_list: &Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) {
        assert!(epoch > self.latest_epoch);
        assert_eq!(self.watermark_direction, direction);
        self.latest_epoch = epoch;
        for vnode_watermark in vnode_watermark_list {
            for vnode in vnode_watermark.vnodes.vnodes() {
                let epoch_watermarks = self.index.entry(vnode).or_default();
                if let Some((prev_epoch, prev_watermark)) = epoch_watermarks.last_key_value() {
                    assert!(*prev_epoch < epoch);
                    match self.watermark_direction {
                        WatermarkDirection::Ascending => {
                            assert!(vnode_watermark.watermark >= prev_watermark);
                        }
                        WatermarkDirection::Descending => {
                            assert!(vnode_watermark.watermark <= prev_watermark);
                        }
                    };
                };
                assert!(self
                    .index
                    .entry(vnode)
                    .or_default()
                    .insert(epoch, vnode_watermark.watermark.clone())
                    .is_none());
            }
        }
    }

    pub fn apply_committed_watermarks(&mut self, committed_index: &TableWatermarksIndex) {
        self.committed_epoch = committed_index.committed_epoch;
        for (vnode, committed_epoch_watermark) in &committed_index.index {
            let epoch_watermark = self.index.entry(*vnode).or_default();
            // keep only watermark higher than committed epoch
            *epoch_watermark = epoch_watermark.split_off(&committed_index.committed_epoch);
            for (epoch, watermark) in committed_epoch_watermark {
                epoch_watermark.insert(*epoch, watermark.clone());
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
pub enum Vnodes {
    Bitmap(Arc<Bitmap>),
    Range {
        start_vnode: VirtualNode,
        end_vnode: VirtualNode,
    },
    Single(VirtualNode),
    All,
}

impl Vnodes {
    pub fn to_protobuf(&self) -> PbVnodes {
        let vnodes = match self {
            Vnodes::Bitmap(bitmap) => vnodes::PbVnodes::VnodeBitmap(bitmap.to_protobuf()),
            Vnodes::Range {
                start_vnode,
                end_vnode,
            } => vnodes::PbVnodes::Range(VnodeRange {
                vnode_start: start_vnode.to_index() as _,
                vnode_end: end_vnode.to_index() as _,
            }),
            Vnodes::Single(vnode) => vnodes::PbVnodes::Single(vnode.to_index() as _),
            Vnodes::All => vnodes::PbVnodes::All(AllVnodes {}),
        };
        PbVnodes {
            vnodes: Some(vnodes),
        }
    }

    pub fn from_protobuf(pb: &PbVnodes) -> Vnodes {
        match pb.vnodes.as_ref().expect("should not be None") {
            vnodes::Vnodes::VnodeBitmap(bitmap) => Vnodes::Bitmap(Arc::new(Bitmap::from(bitmap))),
            vnodes::Vnodes::Range(range) => Vnodes::Range {
                start_vnode: VirtualNode::from_index(range.vnode_start as _),
                end_vnode: VirtualNode::from_index(range.vnode_end as _),
            },
            vnodes::Vnodes::Single(vnode) => Vnodes::Single(VirtualNode::from_index(*vnode as _)),
            vnodes::Vnodes::All(_) => Vnodes::All,
        }
    }

    #[auto_enums::auto_enum(Iterator)]
    pub fn vnodes(&self) -> impl Iterator<Item = VirtualNode> + '_ {
        match self {
            Vnodes::Bitmap(bitmap) => bitmap.iter_ones().map(VirtualNode::from_index),
            Vnodes::Range {
                start_vnode,
                end_vnode,
            } => (start_vnode.to_index()..=end_vnode.to_index()).map(VirtualNode::from_index),
            Vnodes::Single(vnode) => once(*vnode),
            Vnodes::All => (0..VirtualNode::COUNT).map(VirtualNode::from_index),
        }
    }

    pub fn contains(&self, vnode: VirtualNode) -> bool {
        match self {
            Vnodes::Bitmap(bitmap) => bitmap.is_set(vnode.to_index()),
            Vnodes::Range {
                start_vnode,
                end_vnode,
            } => start_vnode <= &vnode && &vnode <= end_vnode,
            Vnodes::Single(single_vnode) => single_vnode == &vnode,
            Vnodes::All => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VnodeWatermark {
    vnodes: Vnodes,
    watermark: Bytes,
}

impl VnodeWatermark {
    pub fn new(vnodes: Vnodes, watermark: Bytes) -> Self {
        Self { vnodes, watermark }
    }

    pub fn to_protobuf(&self) -> PbVnodeWatermark {
        PbVnodeWatermark {
            watermark: self.watermark.to_vec(),
            vnodes: Some(self.vnodes.to_protobuf()),
        }
    }

    pub fn from_protobuf(pb: &PbVnodeWatermark) -> Self {
        Self {
            vnodes: match pb.vnodes.as_ref().unwrap().vnodes.as_ref().unwrap() {
                vnodes::PbVnodes::VnodeBitmap(bitmap) => {
                    Vnodes::Bitmap(Arc::new(Bitmap::from(bitmap)))
                }
                vnodes::PbVnodes::Range(range) => Vnodes::Range {
                    start_vnode: VirtualNode::from_index(range.vnode_start as _),
                    end_vnode: VirtualNode::from_index(range.vnode_end as _),
                },
                vnodes::PbVnodes::Single(vnode) => {
                    Vnodes::Single(VirtualNode::from_index(*vnode as _))
                }
                vnodes::PbVnodes::All(_) => Vnodes::All,
            },
            watermark: Bytes::from(pb.watermark.clone()),
        }
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

    pub fn build_index(&self, committed_epoch: HummockEpoch) -> TableWatermarksIndex {
        let mut ret = TableWatermarksIndex {
            index: HashMap::new(),
            watermark_direction: self.direction,
            latest_epoch: HummockEpoch::MIN,
            committed_epoch: HummockEpoch::MIN,
        };
        for (epoch, vnode_watermark_list) in &self.watermarks {
            ret.add_epoch_watermark(*epoch, vnode_watermark_list, self.direction);
        }
        ret.committed_epoch = committed_epoch;
        ret
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
                    for vnode in Vnodes::from_protobuf(
                        watermark.vnodes.as_ref().expect("should not be None"),
                    )
                    .vnodes()
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
                for vnode in Vnodes::from_protobuf(vnode_watermark.vnodes.as_ref().expect("should not be None")).vnodes() {
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
                        vnodes: Some(PbVnodes {
                            vnodes: Some(vnodes::Vnodes::VnodeBitmap(bitmap.to_protobuf()))
                        }),
                        watermark: vnode_watermark.watermark.clone(),
                    })
                }
            }
            if !new_vnode_watermarks.is_empty() {
                result_epoch_watermark.push(PbEpochNewWatermarks {
                    watermarks: new_vnode_watermarks,
                    // set epoch as safe epoch
                    epoch: safe_epoch,
                })
            }
        }
        // epoch watermark are added from later epoch to earlier epoch.
        // reverse to ensure that earlier epochs are at the front
        result_epoch_watermark.reverse();
        *self = PbTableWatermarks {
            epoch_watermarks: result_epoch_watermark,
            is_ascending: self.is_ascending,
        }
    }
}
