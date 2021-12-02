use async_trait::async_trait;
use std::{collections::BinaryHeap, fmt::Debug};

use bytes::BytesMut;

use crate::storage::hummock::{value::HummockValue, HummockResult};

use super::HummockIterator;

struct HeapNode {
    iterator_idx: usize,
    key: BytesMut,
    val: BytesMut,
}
impl Debug for HeapNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapNode")
            .field("iterator_idx", &self.iterator_idx)
            .field("key", &std::str::from_utf8(&self.key))
            .field("val", &std::str::from_utf8(&self.key))
            .finish()
    }
}

pub struct MergeIterator {
    iterators: Vec<Box<dyn HummockIterator + Send>>,
    heap: BinaryHeap<HeapNode>,
    cur_node: Option<HeapNode>,
    heap_built: bool,
}

impl HeapNode {
    fn new(idx: usize, key: &[u8], val: HummockValue<&[u8]>) -> Self {
        let mut buf = vec![];
        val.encode(&mut buf);
        HeapNode {
            iterator_idx: idx,
            key: BytesMut::from(key),
            val: BytesMut::from(buf.as_slice()),
        }
    }

    fn pair(&self) -> (&[u8], HummockValue<&[u8]>) {
        (&self.key, HummockValue::from_slice(&self.val).unwrap())
    }
}
impl Eq for HeapNode {}
impl PartialEq for HeapNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.cmp(self))
    }
}
impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl MergeIterator {
    pub fn new(iterators: Vec<Box<dyn HummockIterator + Send>>) -> Self {
        Self {
            iterators,
            heap: BinaryHeap::new(),
            cur_node: None,
            heap_built: false,
        }
    }
    async fn next_inner(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>> {
        if !self.heap_built {
            self.build_heap().await?;
        }
        if self.heap.is_empty() {
            return Ok(None);
        }
        self.cur_node = Some(self.heap.pop().unwrap());
        let cur_node = self.cur_node.as_ref().unwrap();
        let iter = &mut self.iterators[cur_node.iterator_idx];
        if let Some((key, val)) = iter.next().await? {
            self.heap
                .push(HeapNode::new(cur_node.iterator_idx, key, val));
        }
        return Ok(Some(cur_node.pair()));
    }

    async fn rewind_inner(&mut self) -> HummockResult<()> {
        futures::future::join_all(self.iterators.iter_mut().map(|x| x.rewind())).await;
        self.heap.clear();
        self.build_heap().await
    }

    async fn seek_inner(&mut self, key: &[u8]) -> HummockResult<()> {
        futures::future::join_all(self.iterators.iter_mut().map(|x| x.seek(key))).await;
        self.heap.clear();
        self.build_heap().await
    }

    async fn build_heap(&mut self) -> HummockResult<()> {
        self.heap_built = true;
        for (iterator_idx, iter) in &mut self.iterators.iter_mut().enumerate() {
            if let Some((key, val)) = iter.next().await? {
                self.heap.push(HeapNode::new(iterator_idx, key, val));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HummockIterator for MergeIterator {
    async fn next(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>> {
        self.next_inner().await
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.rewind_inner().await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.seek_inner(key).await
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::storage::hummock::{
        iterator::tests::test::{
            default_builder_opt_for_test, gen_test_table_base, test_key_of, test_value_of,
            TEST_KEYS_COUNT,
        },
        iterator::HummockIterator,
        table::TableIterator,
    };

    use super::MergeIterator;

    #[tokio::test]
    async fn test_basic() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<Box<dyn HummockIterator + Send>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        let mut mi = MergeIterator::new(iters);
        let mut i = 0;
        loop {
            let (k, v) = mi.next().await.unwrap().unwrap();
            assert_eq!(k, test_key_of(0, i).as_slice());
            assert_eq!(v.into_put_value().unwrap(), test_value_of(0, i).as_slice());
            i += 1;

            if i == TEST_KEYS_COUNT * 3 {
                assert!(mi.next().await.unwrap().is_none());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_seek() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<Box<dyn HummockIterator + Send>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        // right edge case
        let mut mi = MergeIterator::new(iters);
        mi.seek(test_key_of(0, 3 * TEST_KEYS_COUNT).as_slice())
            .await
            .unwrap();
        let res = mi.next().await.unwrap();
        assert!(res.is_none());

        // normal case
        mi.seek(test_key_of(0, 4).as_slice()).await.unwrap();
        let (k, v) = mi.next().await.unwrap().unwrap();
        assert_eq!(k, test_key_of(0, 4).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 4).as_slice());

        mi.seek(test_key_of(0, 17).as_slice()).await.unwrap();
        let (k, v) = mi.next().await.unwrap().unwrap();
        assert_eq!(k, test_key_of(0, 17).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 17).as_slice());

        // left edge case
        mi.seek(test_key_of(0, 0).as_slice()).await.unwrap();
        let (k, v) = mi.next().await.unwrap().unwrap();
        assert_eq!(k, test_key_of(0, 0).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 0).as_slice());
    }
}
