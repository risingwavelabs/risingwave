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

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::error::Error;

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;

use super::{KeyedChangeLogRow, KeyedRow};

pub trait NodePeek<R> {
    fn vnode_key(&self) -> &[u8];
    fn into_row(self: Box<Self>) -> R;
}

impl<K: AsRef<[u8]>> NodePeek<KeyedRow<K>> for KeyedRow<K> {
    fn vnode_key(&self) -> &[u8] {
        self.key()
    }

    fn into_row(self: Box<Self>) -> KeyedRow<K> {
        *self
    }
}

impl<K: AsRef<[u8]>> NodePeek<KeyedChangeLogRow<K>> for KeyedChangeLogRow<K> {
    fn vnode_key(&self) -> &[u8] {
        self.key()
    }

    fn into_row(self: Box<Self>) -> KeyedChangeLogRow<K> {
        *self
    }
}

struct Node<'a, S, R> {
    stream: S,

    /// The next item polled from `stream` previously. Since the `eq` and `cmp` must be synchronous
    /// functions, we need to implement peeking manually.
    peeked: Box<dyn NodePeek<R> + 'a + Send + Sync>,
}

impl<S, R> PartialEq for Node<'_, S, R> {
    fn eq(&self, other: &Self) -> bool {
        match self.peeked.vnode_key() == other.peeked.vnode_key() {
            true => unreachable!("primary key from different iters should be unique"),
            false => false,
        }
    }
}
impl<S, R> Eq for Node<'_, S, R> {}

impl<S, R> PartialOrd for Node<'_, S, R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S, R> Ord for Node<'_, S, R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The heap is a max heap, so we need to reverse the order.
        self.peeked
            .vnode_key()
            .cmp(other.peeked.vnode_key())
            .reverse()
    }
}

#[try_stream(ok=KO, error=E)]
pub async fn merge_sort<'a, KO, E, NP, R>(streams: Vec<R>)
where
    // K: AsRef<[u8]> + 'a,
    KO: 'a,
    NP: NodePeek<KO> + 'a + Send + Sync,
    E: Error + 'a,
    R: Stream<Item = Result<NP, E>> + 'a + Unpin,
{
    let mut heap = BinaryHeap::new();
    for mut stream in streams {
        if let Some(peeked) = stream.next().await.transpose()? {
            heap.push(Node {
                stream,
                peeked: Box::new(peeked),
            });
        }
    }
    while let Some(mut node) = heap.peek_mut() {
        // Note: If the `next` returns `Err`, we'll fail to yield the previous item.
        yield match node.stream.next().await.transpose()? {
            // There still remains data in the stream, take and update the peeked value.
            Some(new_peeked) => {
                std::mem::replace(&mut node.peeked, Box::new(new_peeked)).into_row()
            }
            // This stream is exhausted, remove it from the heap.
            None => PeekMut::pop(node).peeked.into_row(),
        };
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::EpochWithGap;

    use super::*;
    use crate::error::StorageResult;

    fn gen_pk_and_row(i: u8) -> StorageResult<KeyedRow<Vec<u8>>> {
        let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
        key.extend(vec![i]);
        Ok(KeyedRow::new(
            TableKey(key),
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
            let expected = gen_pk_and_row(i as u8).unwrap();
            let actual = result.unwrap();
            assert_eq!(actual.key(), expected.key());
            assert_eq!(actual.into_owned_row(), expected.into_owned_row());
        }
    }
}
