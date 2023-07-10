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

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::error::Error;

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::row::OwnedRow;

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
