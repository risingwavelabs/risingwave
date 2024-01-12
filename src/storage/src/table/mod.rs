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

pub mod batch_table;
pub mod merge_sort;

use std::mem::replace;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::key::TableKey;
use tracing::warn;

use crate::error::StorageResult;

/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = VirtualNode::ZERO;

#[derive(Debug, Clone)]
/// Represents the distribution for a specific table instance.
pub enum TableDistribution {
    Singleton,
    DistKeyIndices {
        /// Indices of distribution key for computing vnode, based on the all columns of the table.
        dist_key_in_pk_indices: Vec<usize>,

        /// Virtual nodes that the table is partitioned into.
        vnodes: Arc<Bitmap>,
    },
    VnodeColumnIndex {
        /// Indices of vnode columns.
        vnode_col_idx_in_pk: usize,

        /// Virtual nodes that the table is partitioned into.
        vnodes: Arc<Bitmap>,
    },
}

pub const SINGLETON_VNODE: VirtualNode = DEFAULT_VNODE;

impl TableDistribution {
    pub fn new(
        vnodes: Option<Arc<Bitmap>>,
        dist_key_in_pk_indices: Vec<usize>,
        vnode_col_idx_in_pk: Option<usize>,
    ) -> Self {
        match vnodes {
            None => {
                if !dist_key_in_pk_indices.is_empty() {
                    warn!(
                        ?dist_key_in_pk_indices,
                        "has dist key but no vnodes provided"
                    );
                }
                if vnode_col_idx_in_pk.is_some() {
                    warn!(
                        vnode_col_idx_in_pk = vnode_col_idx_in_pk.unwrap(),
                        "has vnode col idx in pk but no vnodes provided"
                    );
                }
                Self::Singleton
            }
            Some(vnodes) => {
                if let Some(vnode_col_idx_in_pk) = vnode_col_idx_in_pk {
                    Self::VnodeColumnIndex {
                        vnode_col_idx_in_pk,
                        vnodes,
                    }
                } else if !dist_key_in_pk_indices.is_empty() {
                    Self::DistKeyIndices {
                        dist_key_in_pk_indices,
                        vnodes,
                    }
                } else {
                    warn!(
                        ?vnodes,
                        "no dist key or vnode col idx provided but provided vnodes"
                    );
                    Self::Singleton
                }
            }
        }
    }

    pub fn is_singleton(&self) -> bool {
        matches!(self, Self::Singleton)
    }

    pub fn singleton_vnode_bitmap_ref() -> &'static Arc<Bitmap> {
        /// A bitmap that only the default vnode is set.
        static SINGLETON_VNODES: LazyLock<Arc<Bitmap>> = LazyLock::new(|| {
            let mut vnodes = BitmapBuilder::zeroed(VirtualNode::COUNT);
            vnodes.set(SINGLETON_VNODE.to_index(), true);
            vnodes.finish().into()
        });

        SINGLETON_VNODES.deref()
    }

    pub fn singleton_vnode_bitmap() -> Arc<Bitmap> {
        Self::singleton_vnode_bitmap_ref().clone()
    }

    pub fn all_vnodes() -> Arc<Bitmap> {
        /// A bitmap that all vnodes are set.
        static ALL_VNODES: LazyLock<Arc<Bitmap>> =
            LazyLock::new(|| Bitmap::ones(VirtualNode::COUNT).into());
        ALL_VNODES.clone()
    }

    /// Distribution that accesses all vnodes
    pub fn all(dist_key_in_pk_indices: Vec<usize>) -> Self {
        Self::DistKeyIndices {
            dist_key_in_pk_indices,
            vnodes: Self::all_vnodes(),
        }
    }

    /// Fallback distribution for singleton or tests.
    pub fn singleton() -> Self {
        Self::Singleton
    }

    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        match self {
            TableDistribution::Singleton => {
                let ret = Self::singleton_vnode_bitmap();
                if ret != new_vnodes {
                    warn!(?new_vnodes, "update vnode on singleton distribution");
                }
                ret
            }
            TableDistribution::DistKeyIndices { ref mut vnodes, .. }
            | TableDistribution::VnodeColumnIndex { ref mut vnodes, .. } => {
                assert_eq!(vnodes.len(), new_vnodes.len());
                replace(vnodes, new_vnodes)
            }
        }
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        match self {
            TableDistribution::Singleton => TableDistribution::singleton_vnode_bitmap_ref(),
            TableDistribution::DistKeyIndices { vnodes, .. }
            | TableDistribution::VnodeColumnIndex { vnodes, .. } => vnodes,
        }
    }

    /// Get vnode value with given primary key.
    pub fn compute_vnode_by_pk(&self, pk: impl Row) -> VirtualNode {
        match &self {
            TableDistribution::Singleton => SINGLETON_VNODE,
            TableDistribution::DistKeyIndices {
                dist_key_in_pk_indices,
                vnodes,
            } => compute_vnode(pk, dist_key_in_pk_indices, vnodes),
            TableDistribution::VnodeColumnIndex {
                vnode_col_idx_in_pk,
                vnodes,
            } => get_vnode_from_row(pk, *vnode_col_idx_in_pk, vnodes),
        }
    }

    pub fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: impl Row) -> Option<VirtualNode> {
        match self {
            TableDistribution::Singleton => Some(SINGLETON_VNODE),
            TableDistribution::DistKeyIndices {
                dist_key_in_pk_indices,
                vnodes,
            } => dist_key_in_pk_indices
                .iter()
                .all(|&d| d < pk_prefix.len())
                .then(|| compute_vnode(pk_prefix, dist_key_in_pk_indices, vnodes)),
            TableDistribution::VnodeColumnIndex {
                vnode_col_idx_in_pk,
                vnodes,
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

// TODO: GAT-ify this trait or remove this trait
#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>>;
}

pub async fn collect_data_chunk<E, S>(
    stream: &mut S,
    schema: &Schema,
    chunk_size: Option<usize>,
) -> Result<Option<DataChunk>, E>
where
    S: Stream<Item = Result<KeyedRow<Bytes>, E>> + Unpin,
{
    let mut builders = schema.create_array_builders(chunk_size.unwrap_or(0));
    let mut row_count = 0;
    for _ in 0..chunk_size.unwrap_or(usize::MAX) {
        match stream.next().await.transpose()? {
            Some(row) => {
                for (datum, builder) in row.iter().zip_eq_fast(builders.iter_mut()) {
                    builder.append(datum);
                }
            }
            None => break,
        }

        row_count += 1;
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

/// Collects data chunks from stream of rows.
pub async fn collect_data_chunk_with_builder<E, S, R>(
    stream: &mut S,
    builder: &mut DataChunkBuilder,
) -> Result<Option<DataChunk>, E>
where
    R: Row,
    S: Stream<Item = Result<R, E>> + Unpin,
{
    // TODO(kwannoel): If necessary, we can optimize it in the future.
    // This can be done by moving the check if builder is full from `append_one_row` to here,
    while let Some(row) = stream.next().await.transpose()? {
        let result = builder.append_one_row(row);
        if let Some(chunk) = result {
            return Ok(Some(chunk));
        }
    }

    let chunk = builder.consume_all();
    Ok(chunk)
}

pub fn get_second<T, U, E>(arg: Result<(T, U), E>) -> Result<U, E> {
    arg.map(|x| x.1)
}

/// Get vnode value with `indices` on the given `row`.
pub fn compute_vnode(row: impl Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    assert!(!indices.is_empty());
    let vnode = VirtualNode::compute_row(&row, indices);
    check_vnode_is_set(vnode, vnodes);

    tracing::debug!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

    vnode
}

pub fn get_vnode_from_row(row: impl Row, index: usize, vnodes: &Bitmap) -> VirtualNode {
    let vnode = VirtualNode::from_datum(row.datum_at(index));
    check_vnode_is_set(vnode, vnodes);

    tracing::debug!(target: "events::storage::storage_table", "get vnode from row: {:?} vnode column index {:?} => {}", row, index, vnode);

    vnode
}

impl TableDistribution {
    /// Get vnode values with `indices` on the given `chunk`.
    pub fn compute_chunk_vnode(&self, chunk: &DataChunk, pk_indices: &[usize]) -> Vec<VirtualNode> {
        match self {
            TableDistribution::Singleton => {
                vec![SINGLETON_VNODE; chunk.capacity()]
            }
            TableDistribution::DistKeyIndices {
                dist_key_in_pk_indices,
                vnodes,
            } => {
                let dist_key_indices = dist_key_in_pk_indices
                    .iter()
                    .map(|idx| pk_indices[*idx])
                    .collect_vec();

                VirtualNode::compute_chunk(chunk, &dist_key_indices)
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
            TableDistribution::VnodeColumnIndex {
                vnode_col_idx_in_pk,
                vnodes,
            } => chunk
                .rows()
                .map(|row| get_vnode_from_row(row, pk_indices[*vnode_col_idx_in_pk], vnodes))
                .collect(),
        }
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

#[derive(Debug)]
pub struct KeyedRow<T: AsRef<[u8]>> {
    vnode_prefixed_key: TableKey<T>,
    row: OwnedRow,
}

impl<T: AsRef<[u8]>> KeyedRow<T> {
    pub fn new(table_key: TableKey<T>, row: OwnedRow) -> Self {
        Self {
            vnode_prefixed_key: table_key,
            row,
        }
    }

    pub fn into_owned_row(self) -> OwnedRow {
        self.row
    }

    pub fn vnode(&self) -> VirtualNode {
        self.vnode_prefixed_key.vnode_part()
    }

    pub fn key(&self) -> &[u8] {
        self.vnode_prefixed_key.key_part()
    }

    pub fn into_parts(self) -> (TableKey<T>, OwnedRow) {
        (self.vnode_prefixed_key, self.row)
    }
}

impl<T: AsRef<[u8]>> Deref for KeyedRow<T> {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}
