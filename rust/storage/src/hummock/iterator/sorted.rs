use async_trait::async_trait;
use std::{collections::BinaryHeap, fmt::Debug};

use bytes::BytesMut;

use crate::hummock::{key_range::VersionComparator, value::HummockValue, HummockResult};

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
        // we use max-heap to implement min-heap, so the order is inverse
        Some(VersionComparator::compare_key(&other.key, &self.key))
    }
}
impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // we use max-heap to implement min-heap, so the order is inverse
        VersionComparator::compare_key(&other.key, &self.key)
    }
}

pub struct SortedIterator {
    iterators: Vec<Box<dyn HummockIterator + Send>>,
    heap: BinaryHeap<HeapNode>,
    cur_node: Option<HeapNode>,
    heap_built: bool,
}

impl SortedIterator {
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

        // add the next node of the current node's table iterator to the heap
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

    /// put all table iterator's first node to the heap
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
impl HummockIterator for SortedIterator {
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
mod tests {
    use std::sync::Arc;

    use crate::{
        hummock::{
            cloud::gen_remote_table,
            format::key_with_ts,
            iterator::tests::test::{
                default_builder_opt_for_test, gen_test_table_base, iterator_test_key_of,
                test_value_of, TEST_KEYS_COUNT,
            },
            iterator::HummockIterator,
            table::TableIterator,
            value::HummockValue,
            TableBuilder,
        },
        object::{InMemObjectStore, ObjectStore},
    };

    use super::SortedIterator;

    #[tokio::test]
    async fn test_delete() {
        let mut b = TableBuilder::new(default_builder_opt_for_test());
        //-----  delete after write  -----
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 1).as_bytes().to_vec(),
                466,
            )
            .as_slice(),
            HummockValue::Put(test_value_of(0, 2)),
        );
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 1).as_bytes().to_vec(),
                233,
            )
            .as_slice(),
            HummockValue::Delete,
        );
        // get remote table
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let (data, meta) = b.finish();
        let table0 = gen_remote_table(obj_client, 0, data, meta, None)
            .await
            .unwrap();

        let iters: Vec<Box<dyn HummockIterator + Send>> =
            vec![Box::new(TableIterator::new(Arc::new(table0)))];
        let mut si = SortedIterator::new(iters);

        // should have 2 valid kv pairs
        si.next().await.unwrap();
        si.next().await.unwrap();
        assert!(si.next().await.unwrap().is_none());
    }

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

        let mut si = SortedIterator::new(iters);
        let mut i = 0;
        loop {
            let (k, v) = si.next().await.unwrap().unwrap();
            assert_eq!(k, iterator_test_key_of(0, i).as_slice());
            assert_eq!(v.into_put_value().unwrap(), test_value_of(0, i).as_slice());
            i += 1;

            if i == TEST_KEYS_COUNT * 3 {
                assert!(si.next().await.unwrap().is_none());
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
        let mut si = SortedIterator::new(iters);
        si.seek(iterator_test_key_of(0, 3 * TEST_KEYS_COUNT).as_slice())
            .await
            .unwrap();
        let res = si.next().await.unwrap();
        assert!(res.is_none());

        // normal case
        si.seek(iterator_test_key_of(0, 4).as_slice())
            .await
            .unwrap();
        let (k, v) = si.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 4).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 4).as_slice());

        si.seek(iterator_test_key_of(0, 17).as_slice())
            .await
            .unwrap();
        let (k, v) = si.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 17).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 17).as_slice());

        // left edge case
        si.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        let (k, v) = si.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 0).as_slice());
    }
}
