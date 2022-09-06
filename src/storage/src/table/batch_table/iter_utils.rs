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

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::row::Row;

use super::storage_table::PkAndRowStream;
use crate::error::StorageError;

/// We use a binary heap to merge the results of the different streams in order.
/// This is the node type of the heap.
struct Node<S: PkAndRowStream> {
    stream: S,

    /// The next item polled from `stream` previously. Since the `eq` and `cmp` must be synchronous
    /// functions, we need to implement peeking manually.
    peeked: (Vec<u8>, Row),
}

impl<S: PkAndRowStream> PartialEq for Node<S> {
    fn eq(&self, other: &Self) -> bool {
        match self.peeked.0 == other.peeked.0 {
            true => unreachable!("primary key from different iters should be unique"),
            false => false,
        }
    }
}
impl<S: PkAndRowStream> Eq for Node<S> {}

impl<S: PkAndRowStream> PartialOrd for Node<S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<S: PkAndRowStream> Ord for Node<S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The heap is a max heap, so we need to reverse the order.
        self.peeked.0.cmp(&other.peeked.0).reverse()
    }
}

/// Merge multiple streams of primary key and rows into a single stream, sorted by primary key.
/// We should ensure that the primary key from different streams are unique.
#[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
pub(super) async fn merge_sort<S>(streams: Vec<S>)
where
    S: PkAndRowStream + Unpin,
{
    let mut heap = BinaryHeap::with_capacity(streams.len());
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

    fn gen_pk_and_row(i: u8) -> StorageResult<(Vec<u8>, Row)> {
        Ok((vec![i], Row(vec![Some(ScalarImpl::Int64(i as _))])))
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
