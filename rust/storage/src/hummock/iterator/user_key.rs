use super::{HummockIterator, SortedIterator};
use crate::hummock::{format::user_key, value::HummockValue, HummockError, HummockResult};

/// ``UserKeyIterator`` can be used be user directly.
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

    /// Get the next kv pair.
    /// Return in the form of (``user_key``, ``user_value``), just are the same as user input.
    ///
    /// Returned result:
    /// - if `Ok(None)` is returned, it means that the iterator successfully and safely reaches its
    ///   end.
    /// - if `Ok(Some((user_key, user_value)))` is returned, it means that this kv pair in the
    ///   `Some` is the next kv pair.
    /// - if `Err(_) ` is returned, it means that some error happended.
    ///
    /// The returned key is:
    /// - de-duplicated. Thus it will not output the same key, unless the `rewind` or `seek` mthods
    ///   are called.
    /// - no overhead. Thus no version in it, and only the `user_key` will be returned.
    ///
    /// The returned value is:
    /// - no overhead. Thus what user
    /// - always newest. Thus if users write multi kv pairs with the same key, only the latest or we
    ///   say the newest one will be returned. If the final write about the key is a Delete command,
    ///   then the kv pair will never be returned.
    pub async fn next(&mut self) -> HummockResult<Option<(&[u8], &[u8])>> {
        loop {
            // we need to ensure that the iterator is valid before enters the loop
            let key = self.iterator.key().unwrap();
            let key = user_key(key);
            // since the table is sorted, if the key equals to the last_key,
            // then its timestamp is not biggest among keys with the same user_key
            if self.last_key != key {
                self.last_key.clear();
                self.last_key.extend_from_slice(key);

                match self.iterator.value().unwrap() {
                    HummockValue::Put(val) => {
                        self.last_val.clear();
                        self.last_val.extend_from_slice(val);

                        return Ok(Some((self.last_key.as_slice(), self.last_val.as_slice())));
                    }
                    // It means that the key is deleted from the storage.
                    // Deleted kv and the previous verisons (if any) of the key should not be
                    // returned to user.
                    HummockValue::Delete => {}
                }
            }

            match self.iterator.next().await {
                Ok(()) => {}
                Err(HummockError::EOF) => {
                    // already reach to the end of the iterator
                    return Ok(None);
                }
                Err(err) => {
                    // other error
                    return Err(err);
                }
            }
        }
    }

    /// Reset the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.last_key.clear();
        self.iterator.rewind().await
    }

    /// Reset the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.last_key.clear();
        self.iterator.seek(key).await
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
        let iters: Vec<Box<dyn HummockIterator + Send + Sync>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        let mut si = SortedIterator::new(iters);
        si.rewind().await.unwrap();
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
                100,
            )
            .as_slice(),
            HummockValue::Put(test_value_of(0, 1)),
        );
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 2).as_bytes().to_vec(),
                300,
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

        let mut b = TableBuilder::new(default_builder_opt_for_test());
        b.add(
            key_with_ts(
                format!("{:03}_key_test_{:05}", 0, 1).as_bytes().to_vec(),
                200,
            )
            .as_slice(),
            HummockValue::Delete,
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
        let table1 = gen_remote_table(obj_client, 0, data, meta, None)
            .await
            .unwrap();

        let iters: Vec<Box<dyn HummockIterator + Send + Sync>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
        ];
        let mut si = SortedIterator::new(iters);
        si.rewind().await.unwrap();
        let mut uki = UserKeyIterator::new(si);

        // verify
        let (k, v) = uki.next().await.unwrap().expect("need have kv pair");
        assert_eq!(k, user_key(iterator_test_key_of(0, 2).as_slice()));
        assert_eq!(v, test_value_of(0, 2));

        // only one valid kv pair
        let t = uki.next().await.unwrap();
        assert!(t.is_none());
    }

    #[tokio::test]
    async fn test_seek() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<Box<dyn HummockIterator + Sync + Send>> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        // right edge case
        let mut si = SortedIterator::new(iters);
        si.rewind().await.unwrap();
        let mut uki = UserKeyIterator::new(si);
        let res = uki
            .seek(iterator_test_key_of(0, 3 * TEST_KEYS_COUNT).as_slice())
            .await;
        assert!(res.is_err());

        // normal case
        uki.seek(iterator_test_key_of(0, 4).as_slice())
            .await
            .unwrap();
        let (k, v) = uki.next().await.unwrap().unwrap();
        assert_eq!(k, user_key(iterator_test_key_of(0, 4).as_slice()));
        assert_eq!(v, test_value_of(0, 4).as_slice());

        uki.seek(iterator_test_key_of(0, 17).as_slice())
            .await
            .unwrap();
        let (k, v) = uki.next().await.unwrap().unwrap();
        assert_eq!(k, user_key(iterator_test_key_of(0, 17).as_slice()));
        assert_eq!(v, test_value_of(0, 17).as_slice());

        // left edge case
        uki.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        let (k, v) = uki.next().await.unwrap().unwrap();
        assert_eq!(k, user_key(iterator_test_key_of(0, 0).as_slice()));
        assert_eq!(v, test_value_of(0, 0).as_slice());
    }
}
