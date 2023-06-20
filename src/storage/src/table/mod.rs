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

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::error::Error;
use std::sync::{Arc, LazyLock};

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::iter_util::ZipEqFast;

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
}

/// Collects data chunks from stream of rows.
pub async fn collect_data_chunk<E, S>(
    stream: &mut S,
    schema: &Schema,
    chunk_size: Option<usize>,
) -> Result<Option<DataChunk>, E>
where
    S: Stream<Item = Result<OwnedRow, E>> + Unpin,
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

pub fn get_second<T, U, E>(arg: Result<(T, U), E>) -> Result<U, E> {
    arg.map(|x| x.1)
}

/// Get vnode value with `indices` on the given `row`.
pub fn compute_vnode(row: impl Row, indices: &[usize], vnodes: &Bitmap) -> VirtualNode {
    let vnode = if indices.is_empty() {
        DEFAULT_VNODE
    } else {
        let vnode = VirtualNode::compute_row(&row, indices);
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

        VirtualNode::compute_chunk(chunk, &dist_key_indices)
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

pub trait MergeSortKey = Eq + PartialEq + Ord + PartialOrd;

struct Node<K: MergeSortKey, S> {
    stream: S,

    /// The next item polled from `stream` previously. Since the `eq` and `cmp` must be synchronous
    /// functions, we need to implement peeking manually.
    peeked: (K, OwnedRow),
}

impl<K: MergeSortKey, S> PartialEq for Node<K, S> {
    fn eq(&self, other: &Self) -> bool {
        match self.peeked.0 == other.peeked.0 {
            true => unreachable!("primary key from different iters should be unique"),
            false => false,
        }
    }
}
impl<K: MergeSortKey, S> Eq for Node<K, S> {}

impl<K: MergeSortKey, S> PartialOrd for Node<K, S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: MergeSortKey, S> Ord for Node<K, S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The heap is a max heap, so we need to reverse the order.
        self.peeked.0.cmp(&other.peeked.0).reverse()
    }
}

#[try_stream(ok=(K, OwnedRow), error=E)]
pub async fn merge_sort<'a, K, E, R>(streams: Vec<R>)
where
    K: MergeSortKey + 'a,
    E: Error + 'a,
    R: Stream<Item = Result<(K, OwnedRow), E>> + 'a + Unpin,
{
    let mut heap = BinaryHeap::new();
    for mut stream in streams {
        if let Some(peeked) = stream.next().await.transpose()? {
            heap.push(Node { stream, peeked });
        }
    }
    while let Some(mut node) = heap.peek_mut() {
        // Note: If the `next` returns `Err`, we'll fail to yield the previous item.
        yield match node.stream.next().await.transpose()? {
            // There still remains data in the stream, take and update the peeked value.
            Some(new_peeked) => std::mem::replace(&mut node.peeked, new_peeked),
            // This stream is exhausted, remove it from the heap.
            None => PeekMut::pop(node).peeked,
        };
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::types::ScalarImpl;

    use super::*;
    use crate::error::StorageResult;

    fn gen_pk_and_row(i: u8) -> StorageResult<(Vec<u8>, OwnedRow)> {
        Ok((
            vec![i],
            OwnedRow::new(vec![Some(ScalarImpl::Int64(i as _))]),
        ))
    }

    #[tokio::test]
    async fn test_merge_sort() {
        let streams = vec![
            futures::stream::iter(vec![
                gen_pk_and_row(0),
                gen_pk_and_row(3),
                gen_pk_and_row(6),
                gen_pk_and_row(9),
            ]),
            futures::stream::iter(vec![
                gen_pk_and_row(1),
                gen_pk_and_row(4),
                gen_pk_and_row(7),
                gen_pk_and_row(10),
            ]),
            futures::stream::iter(vec![
                gen_pk_and_row(2),
                gen_pk_and_row(5),
                gen_pk_and_row(8),
            ]),
            futures::stream::iter(vec![]), // empty stream
        ];

        let merge_sorted = merge_sort(streams);

        #[for_await]
        for (i, result) in merge_sorted.enumerate() {
            assert_eq!(result.unwrap(), gen_pk_and_row(i as u8).unwrap());
        }
    }
}
