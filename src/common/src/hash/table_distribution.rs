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

use std::mem::replace;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_pb::plan_common::StorageTableDesc;

use crate::array::{Array, DataChunk, PrimitiveArray};
use crate::bitmap::Bitmap;
use crate::hash::{VirtualNode, VnodeCountCompat};
use crate::row::Row;
use crate::util::iter_util::ZipEqFast;

/// For tables without distribution (singleton), the `SINGLETON_VNODE` is encoded.
pub const SINGLETON_VNODE: VirtualNode = VirtualNode::ZERO;

use super::VnodeBitmapExt;

#[derive(Debug, Clone)]
enum ComputeVnode {
    Singleton,
    DistKeyIndices {
        /// Virtual nodes that the table is partitioned into.
        vnodes: Arc<Bitmap>,
        /// Indices of distribution key for computing vnode, based on the pk columns of the table.
        dist_key_in_pk_indices: Vec<usize>,
    },
    VnodeColumnIndex {
        /// Virtual nodes that the table is partitioned into.
        vnodes: Arc<Bitmap>,
        /// Index of vnode column.
        vnode_col_idx_in_pk: usize,
    },
}

#[derive(Debug, Clone)]
/// Represents the distribution for a specific table instance.
pub struct TableDistribution {
    /// The way to compute vnode provided primary key
    compute_vnode: ComputeVnode,
}

impl TableDistribution {
    pub fn new_from_storage_table_desc(
        vnodes: Option<Arc<Bitmap>>,
        table_desc: &StorageTableDesc,
    ) -> Self {
        let dist_key_in_pk_indices = table_desc
            .dist_key_in_pk_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let vnode_col_idx_in_pk = table_desc.vnode_col_idx_in_pk.map(|k| k as usize);

        let this = Self::new(vnodes, dist_key_in_pk_indices, vnode_col_idx_in_pk);
        assert_eq!(
            this.vnode_count(),
            table_desc.vnode_count(),
            "vnode count mismatch, scanning table {} under wrong distribution?",
            table_desc.table_id
        );
        this
    }

    pub fn new(
        vnodes: Option<Arc<Bitmap>>,
        dist_key_in_pk_indices: Vec<usize>,
        vnode_col_idx_in_pk: Option<usize>,
    ) -> Self {
        let compute_vnode = if let Some(vnode_col_idx_in_pk) = vnode_col_idx_in_pk {
            ComputeVnode::VnodeColumnIndex {
                vnodes: vnodes.unwrap_or_else(|| Bitmap::singleton_arc().clone()),
                vnode_col_idx_in_pk,
            }
        } else if !dist_key_in_pk_indices.is_empty() {
            ComputeVnode::DistKeyIndices {
                vnodes: vnodes.expect("vnodes must be `Some` as dist key indices are set"),
                dist_key_in_pk_indices,
            }
        } else {
            ComputeVnode::Singleton
        };

        Self { compute_vnode }
    }

    pub fn is_singleton(&self) -> bool {
        matches!(&self.compute_vnode, ComputeVnode::Singleton)
    }

    /// Distribution that accesses all vnodes, mainly used for tests.
    pub fn all(dist_key_in_pk_indices: Vec<usize>, vnode_count: usize) -> Self {
        Self {
            compute_vnode: ComputeVnode::DistKeyIndices {
                vnodes: Bitmap::ones(vnode_count).into(),
                dist_key_in_pk_indices,
            },
        }
    }

    /// Fallback distribution for singleton or tests.
    pub fn singleton() -> Self {
        Self {
            compute_vnode: ComputeVnode::Singleton,
        }
    }

    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        match &mut self.compute_vnode {
            ComputeVnode::Singleton => {
                if !new_vnodes.is_singleton() {
                    panic!(
                        "update vnode bitmap on singleton distribution to non-singleton: {:?}",
                        new_vnodes
                    );
                }
                self.vnodes().clone() // not updated
            }

            ComputeVnode::DistKeyIndices { vnodes, .. }
            | ComputeVnode::VnodeColumnIndex { vnodes, .. } => {
                assert_eq!(vnodes.len(), new_vnodes.len());
                replace(vnodes, new_vnodes)
            }
        }
    }

    /// Get vnode bitmap if distributed, or a dummy [`Bitmap::singleton()`] if singleton.
    pub fn vnodes(&self) -> &Arc<Bitmap> {
        match &self.compute_vnode {
            ComputeVnode::DistKeyIndices { vnodes, .. } => vnodes,
            ComputeVnode::VnodeColumnIndex { vnodes, .. } => vnodes,
            ComputeVnode::Singleton => Bitmap::singleton_arc(),
        }
    }

    /// Get vnode count (1 if singleton). Equivalent to `self.vnodes().len()`.
    pub fn vnode_count(&self) -> usize {
        self.vnodes().len()
    }

    /// Get vnode value with given primary key.
    pub fn compute_vnode_by_pk(&self, pk: impl Row) -> VirtualNode {
        match &self.compute_vnode {
            ComputeVnode::Singleton => SINGLETON_VNODE,
            ComputeVnode::DistKeyIndices {
                vnodes,
                dist_key_in_pk_indices,
            } => compute_vnode(pk, dist_key_in_pk_indices, vnodes),
            ComputeVnode::VnodeColumnIndex {
                vnodes,
                vnode_col_idx_in_pk,
            } => get_vnode_from_row(pk, *vnode_col_idx_in_pk, vnodes),
        }
    }

    pub fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: impl Row) -> Option<VirtualNode> {
        match &self.compute_vnode {
            ComputeVnode::Singleton => Some(SINGLETON_VNODE),
            ComputeVnode::DistKeyIndices {
                vnodes,
                dist_key_in_pk_indices,
            } => dist_key_in_pk_indices
                .iter()
                .all(|&d| d < pk_prefix.len())
                .then(|| compute_vnode(pk_prefix, dist_key_in_pk_indices, vnodes)),
            ComputeVnode::VnodeColumnIndex {
                vnodes,
                vnode_col_idx_in_pk,
            } => {
                if *vnode_col_idx_in_pk >= pk_prefix.len() {
                    None
                } else {
                    Some(get_vnode_from_row(pk_prefix, *vnode_col_idx_in_pk, vnodes))
                }
            }
        }
    }
}

/// Get vnode value with `indices` on the given `row`.
pub fn compute_vnode(row: impl Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    assert!(!indices.is_empty());
    let vnode = VirtualNode::compute_row(&row, indices, vnodes.len());
    check_vnode_is_set(vnode, vnodes);

    tracing::debug!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

    vnode
}

pub fn get_vnode_from_row(row: impl Row, index: usize, vnodes: &Bitmap) -> VirtualNode {
    let vnode = VirtualNode::from_datum_ref(row.datum_at(index));
    check_vnode_is_set(vnode, vnodes);

    tracing::debug!(target: "events::storage::storage_table", "get vnode from row: {:?} vnode column index {:?} => {}", row, index, vnode);

    vnode
}

impl TableDistribution {
    /// Get vnode values with `indices` on the given `chunk`.
    ///
    /// Vnode of invisible rows will be included. Only the vnode of visible row check if it's accessible
    pub fn compute_chunk_vnode(&self, chunk: &DataChunk, pk_indices: &[usize]) -> Vec<VirtualNode> {
        match &self.compute_vnode {
            ComputeVnode::Singleton => {
                vec![SINGLETON_VNODE; chunk.capacity()]
            }
            ComputeVnode::DistKeyIndices {
                vnodes,
                dist_key_in_pk_indices,
            } => {
                let dist_key_indices = dist_key_in_pk_indices
                    .iter()
                    .map(|idx| pk_indices[*idx])
                    .collect_vec();

                VirtualNode::compute_chunk(chunk, &dist_key_indices, vnodes.len())
                    .into_iter()
                    .zip_eq_fast(chunk.visibility().iter())
                    .map(|(vnode, vis)| {
                        // Ignore the invisible rows.
                        if vis {
                            check_vnode_is_set(vnode, vnodes);
                        }
                        vnode
                    })
                    .collect()
            }
            ComputeVnode::VnodeColumnIndex {
                vnodes,
                vnode_col_idx_in_pk,
            } => {
                let array: &PrimitiveArray<i16> =
                    chunk.columns()[pk_indices[*vnode_col_idx_in_pk]].as_int16();
                array
                    .raw_iter()
                    .zip_eq_fast(array.null_bitmap().iter())
                    .zip_eq_fast(chunk.visibility().iter())
                    .map(|((vnode, exist), vis)| {
                        let vnode = VirtualNode::from_scalar(vnode);
                        if vis {
                            assert!(exist);
                            check_vnode_is_set(vnode, vnodes);
                        }
                        vnode
                    })
                    .collect_vec()
            }
        }
    }
}

/// Check whether the given `vnode` is set in the `vnodes` of this table.
fn check_vnode_is_set(vnode: VirtualNode, vnodes: &Bitmap) {
    let is_set = vnodes.is_set(vnode.to_index());

    if !is_set {
        let high_ranges = vnodes.high_ranges().map(|r| format!("{r:?}")).join(", ");
        panic!(
            "vnode {} should not be accessed by this table\nvnode count: {}\nallowed vnodes: {}",
            vnode,
            vnodes.len(),
            high_ranges
        );
    }
}
