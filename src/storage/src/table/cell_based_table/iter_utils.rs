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
use risingwave_common::array::Row;

use super::PkAndRowStream;
use crate::table::cell_based_table::StorageError;

struct Node<I: PkAndRowStream> {
    iter: I,
    peeked: (Vec<u8>, Row),
}

impl<I: PkAndRowStream> PartialEq for Node<I> {
    fn eq(&self, other: &Self) -> bool {
        match self.peeked.0 == other.peeked.0 {
            true => panic!("primary key should be unique"),
            false => false,
        }
    }
}
impl<I: PkAndRowStream> Eq for Node<I> {}

impl<I: PkAndRowStream> PartialOrd for Node<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<I: PkAndRowStream> Ord for Node<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.peeked.0.cmp(&other.peeked.0).reverse()
    }
}

#[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
pub(super) async fn merge_sort<I>(iterators: Vec<I>)
where
    I: PkAndRowStream + Unpin,
{
    let mut heap = BinaryHeap::with_capacity(iterators.len());
    for mut iter in iterators {
        if let Some(peeked) = iter.next().await.transpose()? {
            heap.push(Node { iter, peeked });
        }
    }

    while let Some(mut node) = heap.peek_mut() {
        yield match node.iter.next().await.transpose()? {
            Some(new_peeked) => std::mem::replace(&mut node.peeked, new_peeked),
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
        let iterators = vec![
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
        ];

        let merge_sorted = merge_sort(iterators);

        #[for_await]
        for (i, result) in merge_sorted.enumerate() {
            assert_eq!(result.unwrap(), gen_pk_and_row(i as u8).unwrap());
        }
    }
}
