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

//! FIXME: This is duplicated from batch iter utils,
//! and slightly modified to change the return error + the public key type.
//! Seems difficult to unify them, since batch uses `StorageError` for its iterators,
//! whereas stream uses `StreamExecutorError` for its iterators.
//! The underlying error type should be `StorageError` as well for stream.
//! Additionally since we use `futures_async_stream`'s `try_stream` macro,
//! the return type has to be concrete, so it can't be parameterized on the PK type.
//! Since batch and stream have different PK types (one is Bytes, other is Vec<u8>),
//! this also means we have to find some other mechanism to parameterize them, perhaps a macro.

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::ops::Bound::*;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use futures_async_stream::{for_await, try_stream};
use itertools::{izip, Itertools};
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{DataChunk, Op, StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{
    get_dist_key_in_pk_indices, ColumnDesc, Schema, TableId, TableOption,
};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{BasicSerde, ValueRowSerde};
use risingwave_hummock_sdk::key::{
    end_bound_of_prefix, next_key, prefixed_range, range_of_prefix, start_bound_of_excluded_prefix,
};
use risingwave_pb::catalog::Table;
use risingwave_storage::error::{StorageError, StorageResult};
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::mem_table::MemTableError;
use risingwave_storage::row_serde::row_serde_util::{
    deserialize_pk_with_vnode, serialize_pk, serialize_pk_with_vnode,
};
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, PrefetchOptions, ReadOptions, StateStoreIterItemStream,
};
use risingwave_storage::table::{compute_chunk_vnode, compute_vnode, Distribution, TableIter};
use risingwave_storage::StateStore;
use tracing::trace;

use super::watermark::{WatermarkBufferByEpoch, WatermarkBufferStrategy};
use crate::cache::cache_may_stale;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub struct Node<S> {
    stream: S,

    /// The next item polled from `stream` previously. Since the `eq` and `cmp` must be synchronous
    /// functions, we need to implement peeking manually.
    peeked: (Bytes, OwnedRow),
}

impl<S> PartialEq for Node<S> {
    fn eq(&self, other: &Self) -> bool {
        match self.peeked.0 == other.peeked.0 {
            true => unreachable!("primary key from different iters should be unique"),
            false => false,
        }
    }
}
impl<S> Eq for Node<S> {}

impl<S> PartialOrd for Node<S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S> Ord for Node<S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The heap is a max heap, so we need to reverse the order.
        self.peeked.0.cmp(&other.peeked.0).reverse()
    }
}

#[try_stream(ok=OwnedRow, error=StreamExecutorError)]
pub async fn merge_sort<'a, R>(streams: Vec<R>)
where
    R: Stream<Item = StreamExecutorResult<(Bytes, OwnedRow)>> + 'a + Unpin,
{
    let mut heap = BinaryHeap::new();
    for mut stream in streams {
        if let Some(peeked) = stream.next().await.transpose()? {
            heap.push(Node { stream, peeked });
        }
    }
    while let Some(mut node) = heap.peek_mut() {
        // Note: If the `next` returns `Err`, we'll fail to yield the previous item.
        // This is acceptable since we're not going to handle errors from cell-based table
        // iteration, so where to fail does not matter. Or we need an `Option` for this.
        yield match node.stream.next().await.transpose()? {
            // There still remains data in the stream, take and update the peeked value.
            Some(new_peeked) => std::mem::replace(&mut node.peeked, new_peeked).1,
            // This stream is exhausted, remove it from the heap.
            None => PeekMut::pop(node).peeked.1,
        };
    }
}
