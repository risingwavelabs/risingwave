use async_trait::async_trait;
use std::{collections::BinaryHeap, fmt::Debug};

use bytes::BytesMut;

use crate::hummock::{
    key_range::VersionComparator, value::HummockValue, HummockError, HummockResult,
};

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
            .finish()
    }
}

pub struct SortedIterator {
    iterators: Vec<Box<dyn HummockIterator + Send + Sync>>,
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
        Some(VersionComparator::compare_key(&other.key, &self.key))
    }
}
impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be changed.
        VersionComparator::compare_key(&other.key, &self.key)
    }
}

impl SortedIterator {
    pub fn new(iterators: Vec<Box<dyn HummockIterator + Send + Sync>>) -> Self {
        Self {
            iterators,
            heap: BinaryHeap::new(),
            cur_node: None,
            heap_built: false,
        }
    }

    /// Merge different sorted tables by using merge sort.
    async fn next_inner(&mut self) -> HummockResult<()> {
        if !self.heap_built {
            self.build_heap().await?;
        }
        if self.heap.is_empty() {
            return Err(HummockError::EOF);
        }
        self.cur_node = Some(self.heap.pop().unwrap());

        let cur_node = self.cur_node.as_ref().unwrap();
        let iter = &mut self.iterators[cur_node.iterator_idx];

        if iter.is_valid() {
            // put the next kv pair into the heap

            let key = iter.key()?;
            let val = iter.value()?;
            self.heap
                .push(HeapNode::new(cur_node.iterator_idx, key, val));
            iter.next().await.unwrap_or(());
        }

        Ok(())
    }

    pub fn key(&self) -> HummockResult<&[u8]> {
        match &self.cur_node {
            Some(node) => Ok(node.pair().0),
            None => Err(HummockError::EOF),
        }
    }

    pub fn value(&self) -> HummockResult<HummockValue<&[u8]>> {
        match &self.cur_node {
            Some(node) => Ok(node.pair().1),
            None => Err(HummockError::EOF),
        }
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
            if iter.is_valid() {
                let key = iter.key()?;
                let val = iter.value()?;
                self.heap.push(HeapNode::new(iterator_idx, key, val));
                iter.next().await?;
            }
        }

        let node = self.heap.pop();
        if node.is_none() {
            return Err(HummockError::EOF);
        }
        let node = node.unwrap();
        let index = node.iterator_idx;
        self.cur_node = Some(node);
        let iter = &mut self.iterators[index];

        if iter.is_valid() {
            let key = iter.key()?;
            let val = iter.value()?;
            self.heap.push(HeapNode::new(index, key, val));
            iter.next().await.unwrap_or(());
        }

        Ok(())
    }
}

#[async_trait]
impl HummockIterator for SortedIterator {
    async fn next(&mut self) -> HummockResult<()> {
        self.next_inner().await
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.rewind_inner().await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.seek_inner(key).await
    }

    fn key(&self) -> HummockResult<&[u8]> {
        self.key()
    }

    fn value(&self) -> HummockResult<HummockValue<&[u8]>> {
        self.value()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::hummock::{
        iterator::test_utils::{
            default_builder_opt_for_test, gen_test_table_base, iterator_test_key_of, test_value_of,
            TEST_KEYS_COUNT,
        },
        iterator::HummockIterator,
        table::TableIterator,
        HummockError,
    };

    use super::SortedIterator;

    #[tokio::test]
    async fn test_basic() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<Box<dyn HummockIterator + Send + Sync>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        let mut mi = SortedIterator::new(iters);
        let mut i = 0;
        mi.rewind().await.unwrap();
        loop {
            let key = mi.key().unwrap();
            let val = mi.value().unwrap();
            assert_eq!(key, iterator_test_key_of(0, i).as_slice());
            assert_eq!(
                val.into_put_value().unwrap(),
                test_value_of(0, i).as_slice()
            );
            i += 1;

            if i == TEST_KEYS_COUNT * 3 {
                assert!(matches!(mi.next().await, Err(HummockError::EOF)));
                break;
            }

            mi.next().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_seek() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<Box<dyn HummockIterator + Send + Sync>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        // right edge case
        let mut mi = SortedIterator::new(iters);
        let res = mi
            .seek(iterator_test_key_of(0, 3 * TEST_KEYS_COUNT).as_slice())
            .await;
        assert!(res.is_err());

        // normal case
        mi.seek(iterator_test_key_of(0, 4).as_slice())
            .await
            .unwrap();
        let k = mi.key().unwrap();
        let v = mi.value().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 4).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 4).as_slice());

        mi.seek(iterator_test_key_of(0, 17).as_slice())
            .await
            .unwrap();
        let k = mi.key().unwrap();
        let v = mi.value().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 17).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 17).as_slice());

        // left edge case
        mi.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        let k = mi.key().unwrap();
        let v = mi.value().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 0).as_slice());
    }
}
