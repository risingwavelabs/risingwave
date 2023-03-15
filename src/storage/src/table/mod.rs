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

pub mod batch_table;

use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashCode, VirtualNode};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarRefImpl;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_id::extract_vnode_id_from_row_id;

use crate::error::StorageResult;

/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = VirtualNode::ZERO;

/// Represents the distribution for a specific table instance.
#[derive(Debug)]
pub struct Distribution {
    /// Indices of distribution key for computing vnode, based on the all columns of the table.
    pub dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    pub vnodes: Arc<Bitmap>,
}

impl Distribution {
    /// Fallback distribution for singleton or tests.
    pub fn fallback() -> Self {
        /// A bitmap that only the default vnode is set.
        static FALLBACK_VNODES: LazyLock<Arc<Bitmap>> = LazyLock::new(|| {
            let mut vnodes = BitmapBuilder::zeroed(VirtualNode::COUNT);
            vnodes.set(DEFAULT_VNODE.to_index(), true);
            vnodes.finish().into()
        });
        Self {
            dist_key_in_pk_indices: vec![],
            vnodes: FALLBACK_VNODES.clone(),
        }
    }

    pub fn fallback_vnodes() -> Arc<Bitmap> {
        /// A bitmap that only the default vnode is set.
        static FALLBACK_VNODES: LazyLock<Arc<Bitmap>> = LazyLock::new(|| {
            let mut vnodes = BitmapBuilder::zeroed(VirtualNode::COUNT);
            vnodes.set(DEFAULT_VNODE.to_index(), true);
            vnodes.finish().into()
        });

        FALLBACK_VNODES.clone()
    }

    /// Distribution that accesses all vnodes, mainly used for tests.
    pub fn all_vnodes(dist_key_in_pk_indices: Vec<usize>) -> Self {
        /// A bitmap that all vnodes are set.
        static ALL_VNODES: LazyLock<Arc<Bitmap>> =
            LazyLock::new(|| Bitmap::ones(VirtualNode::COUNT).into());
        Self {
            dist_key_in_pk_indices,
            vnodes: ALL_VNODES.clone(),
        }
    }
}

// TODO: GAT-ify this trait or remove this trait
#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>>;

    async fn collect_data_chunk(
        &mut self,
        schema: &Schema,
        chunk_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        let mut builders = schema.create_array_builders(chunk_size.unwrap_or(0));

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match self.next_row().await? {
                Some(row) => {
                    for (datum, builder) in row.iter().zip_eq_fast(builders.iter_mut()) {
                        builder.append_datum(datum);
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = {
            let columns: Vec<_> = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();
            DataChunk::new(columns, row_count)
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

/// Get vnode value with `indices` on the given `row`.
pub fn compute_vnode(row: impl Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    let vnode = if indices.is_empty() {
        DEFAULT_VNODE
    } else {
        let vnode = VirtualNode::compute_row(&(&row).project(indices));
        check_vnode_is_set(vnode, vnodes);
        vnode
    };

    tracing::trace!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

    vnode
}

/// Get vnode values with `indices` on the given `chunk`.
pub fn compute_chunk_vnode(
    chunk: &DataChunk,
    dist_key_in_pk_indices: &[usize],
    pk_indices: &[usize],
    vnodes: &Bitmap,
) -> Vec<VirtualNode> {
    if dist_key_in_pk_indices.is_empty() {
        vec![DEFAULT_VNODE; chunk.capacity()]
    } else {
        let dist_key_indices = dist_key_in_pk_indices
            .iter()
            .map(|idx| pk_indices[*idx])
            .collect_vec();

        VirtualNode::compute_chunk(chunk, &dist_key_indices, Crc32FastBuilder)
            .into_iter()
            .zip_eq_fast(chunk.vis().iter())
            .map(|(vnode, vis)| {
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
    let is_set = vnodes.is_set(vnode.to_index());
    assert!(
        is_set,
        "vnode {} should not be accessed by this table",
        vnode
    );
}
