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

pub mod batch_table;
pub mod error;
pub mod streaming_table;

use std::sync::{Arc, LazyLock};

pub use batch_table::storage_table::TableIter;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::types::{VirtualNode, VIRTUAL_NODE_COUNT};
use risingwave_common::util::hash_util::Crc32FastBuilder;

/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = 0;

/// Represents the distribution for a specific table instance.
#[derive(Debug)]
pub struct Distribution {
    /// Indices of distribution key for computing vnode, based on the all columns of the table.
    pub dist_key_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    pub vnodes: Arc<Bitmap>,
}

impl Distribution {
    /// Fallback distribution for singleton or tests.
    pub fn fallback() -> Self {
        /// A bitmap that only the default vnode is set.
        static FALLBACK_VNODES: LazyLock<Arc<Bitmap>> = LazyLock::new(|| {
            let mut vnodes = BitmapBuilder::zeroed(VIRTUAL_NODE_COUNT);
            vnodes.set(DEFAULT_VNODE as _, true);
            vnodes.finish().into()
        });
        Self {
            dist_key_indices: vec![],
            vnodes: FALLBACK_VNODES.clone(),
        }
    }

    /// Distribution that accesses all vnodes, mainly used for tests.
    pub fn all_vnodes(dist_key_indices: Vec<usize>) -> Self {
        /// A bitmap that all vnodes are set.
        static ALL_VNODES: LazyLock<Arc<Bitmap>> =
            LazyLock::new(|| Bitmap::all_high_bits(VIRTUAL_NODE_COUNT).into());
        Self {
            dist_key_indices,
            vnodes: ALL_VNODES.clone(),
        }
    }
}

/// Get vnode value with `indices` on the given `row`.
fn compute_vnode(row: &Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    let vnode = if indices.is_empty() {
        DEFAULT_VNODE
    } else {
        let vnode = row
            .hash_by_indices(indices, &Crc32FastBuilder {})
            .to_vnode();
        check_vnode_is_set(vnode, vnodes);
        vnode
    };

    tracing::trace!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

    vnode
}

/// Get vnode values with `indices` on the given `chunk`.
fn compute_chunk_vnode(chunk: &DataChunk, indices: &[usize], vnodes: &Bitmap) -> Vec<VirtualNode> {
    if indices.is_empty() {
        vec![DEFAULT_VNODE; chunk.capacity()]
    } else {
        chunk
            .get_hash_values(indices, Crc32FastBuilder {})
            .into_iter()
            .zip_eq(chunk.vis().iter())
            .map(|(h, vis)| {
                let vnode = h.to_vnode();
                // Ignore the invisible rows.
                if vis {
                    check_vnode_is_set(vnode, vnodes);
                }
                vnode
            })
            .collect()
    }
}

/// Check whether the given `vnode` is set in the `vnodes` of this table.
fn check_vnode_is_set(vnode: VirtualNode, vnodes: &Bitmap) {
    let is_set = vnodes.is_set(vnode as usize);
    assert!(
        is_set,
        "vnode {} should not be accessed by this table",
        vnode
    );
}
