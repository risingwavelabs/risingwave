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
pub mod streaming_table;

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_COUNT};
use risingwave_common::util::hash_util::CRC32FastBuilder;

use crate::error::StorageResult;
/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = 0;

type DataTypes = Arc<[DataType]>;

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
        lazy_static::lazy_static! {
            /// A bitmap that only the default vnode is set.
            static ref FALLBACK_VNODES: Arc<Bitmap> = {
                let mut vnodes = BitmapBuilder::zeroed(VIRTUAL_NODE_COUNT);
                vnodes.set(DEFAULT_VNODE as _, true);
                vnodes.finish().into()
            };
        }
        Self {
            dist_key_indices: vec![],
            vnodes: FALLBACK_VNODES.clone(),
        }
    }

    /// Distribution that accesses all vnodes, mainly used for tests.
    pub fn all_vnodes(dist_key_indices: Vec<usize>) -> Self {
        lazy_static::lazy_static! {
            /// A bitmap that all vnodes are set.
            static ref ALL_VNODES: Arc<Bitmap> = Bitmap::all_high_bits(VIRTUAL_NODE_COUNT).into();
        }
        Self {
            dist_key_indices,
            vnodes: ALL_VNODES.clone(),
        }
    }
}

// TODO: GAT-ify this trait or remove this trait
#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next_row(&mut self) -> StorageResult<Option<Row>>;

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
                    for (datum, builder) in row.0.into_iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&datum)?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(Into::into))
                .try_collect()?;
            DataChunk::new(columns, row_count)
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

/// Get vnode value with `indices` on the given `row`. Should not be used directly.
fn compute_vnode(row: &Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    let vnode = if indices.is_empty() {
        DEFAULT_VNODE
    } else {
        row.hash_by_indices(indices, &CRC32FastBuilder {})
            .to_vnode()
    };

    tracing::trace!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

    // FIXME: temporary workaround for local agg, may not needed after we have a vnode builder
    if !indices.is_empty() {
        check_vnode_is_set(vnode, vnodes);
    }
    vnode
}

/// Check whether the given `vnode` is set in the `vnodes` of this table.
fn check_vnode_is_set(vnode: VirtualNode, vnodes: &Bitmap) {
    let is_set = vnodes.is_set(vnode as usize).unwrap();
    assert!(
        is_set,
        "vnode {} should not be accessed by this table",
        vnode
    );
}
