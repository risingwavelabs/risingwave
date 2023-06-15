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





use bytes::{Bytes};
use futures::{Stream, StreamExt};
use futures_async_stream::{try_stream};







use risingwave_common::row::{OwnedRow};












use risingwave_storage::store::{
    StateStoreIterItemStream,
};






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
        yield match node.stream.next().await.transpose()? {
            // There still remains data in the stream, take and update the peeked value.
            Some(new_peeked) => std::mem::replace(&mut node.peeked, new_peeked).1,
            // This stream is exhausted, remove it from the heap.
            None => PeekMut::pop(node).peeked.1,
        };
    }
}
