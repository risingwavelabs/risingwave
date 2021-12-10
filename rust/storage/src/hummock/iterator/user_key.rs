use super::{HummockIterator, SortedIterator};
use crate::hummock::{format::user_key, value::HummockValue, HummockResult};
use bytes::Bytes;

pub struct UserKeyIterator {
    iterator: SortedIterator,
    last_key: Vec<u8>,
    last_val: Vec<u8>,
}

impl UserKeyIterator {
    pub fn new(iterator: SortedIterator) -> Self {
        Self {
            iterator,
            last_key: Vec::new(),
            last_val: Vec::new(),
        }
    }
}

impl UserKeyIterator {
    async fn next(&mut self) -> HummockResult<Option<(&[u8], &[u8])>> {
        // TODO(Sunt): this implementation is inefficient, refactor me.
        loop {
            match self.iterator.next().await? {
                Some((key, val)) => {
                    let key = Bytes::copy_from_slice(key);
                    let key = user_key(&key);
                    if self.last_key != key {
                        self.last_key.clear();
                        self.last_key.extend_from_slice(key);

                        if val == HummockValue::Delete {
                            continue;
                        }
                        self.last_val.clear();
                        self.last_val
                            .extend_from_slice(val.into_put_value().unwrap());

                        return Ok(Some((self.last_key.as_slice(), self.last_val.as_slice())));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.last_key = Vec::new();
        self.iterator.rewind().await
    }

    async fn seek(&mut self, _key: &[u8]) -> HummockResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        hummock::{
            cloud::gen_remote_table,
            format::{key_with_ts, user_key},
            iterator::test_utils::{
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

    use super::{SortedIterator, UserKeyIterator};

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

        let si = SortedIterator::new(iters);
        let mut uki = UserKeyIterator::new(si);

        let mut i = 0;
        loop {
            let (k, v) = uki.next().await.unwrap().unwrap();
            assert_eq!(k, user_key(iterator_test_key_of(0, i).as_slice()));
            assert_eq!(v, test_value_of(0, i).as_slice());

            i += 1;

            if i == TEST_KEYS_COUNT * 3 {
                assert!(uki.next().await.unwrap().is_none());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let mut b = TableBuilder::new(default_builder_opt_for_test());
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 1).as_bytes().to_vec(),
                200,
            )
            .as_slice(),
            HummockValue::Put(test_value_of(0, 1)),
        );
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 2).as_bytes().to_vec(),
                400,
            )
            .as_slice(),
            HummockValue::Put(test_value_of(0, 2)),
        );
        // get remote table
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let (data, meta) = b.finish();
        let table0 = gen_remote_table(obj_client, 0, data, meta, None)
            .await
            .unwrap();

        let mut b = TableBuilder::new(default_builder_opt_for_test());
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 1).as_bytes().to_vec(),
                300,
            )
            .as_slice(),
            HummockValue::Delete,
        );
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 2).as_bytes().to_vec(),
                100,
            )
            .as_slice(),
            HummockValue::Delete,
        );
        // get remote table
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let (data, meta) = b.finish();
        let table1 = gen_remote_table(obj_client, 0, data, meta, None)
            .await
            .unwrap();

        let iters: Vec<Box<dyn HummockIterator + Send>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
        ];
        let si = SortedIterator::new(iters);
        let mut uki = UserKeyIterator::new(si);

        // verify
        let (k, v) = uki.next().await.unwrap().unwrap();
        assert_eq!(k, user_key(iterator_test_key_of(0, 2).as_slice()));
        assert_eq!(v, test_value_of(0, 2));

        // only one valid kv pair
        let t = uki.next().await.unwrap();
        assert!(t.is_none());
    }
}
