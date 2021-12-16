use super::{HummockIterator, SortedIterator};
use crate::hummock::format::{key_with_ts, user_key as to_user_key};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;

/// ``UserKeyIterator`` can be used by user directly.
pub struct UserKeyIterator {
    iterator: SortedIterator,
    /// last user key
    last_key: Vec<u8>,
    /// last user value
    last_val: Vec<u8>,

    // flag for whether the iterator reach over the range. Use when key_range is non-None only.
    out_of_range: bool,
    key_range: Option<(Vec<u8>, Vec<u8>)>,
}

// TODO: decide whether this should also impl `HummockIterator`
impl UserKeyIterator {
    pub fn new(iterator: SortedIterator, key_range: Option<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            iterator,
            key_range,
            out_of_range: false,
            last_key: Vec::new(),
            last_val: Vec::new(),
        }
    }

    /// Get the iterator move to the next step.
    ///
    /// Returned result:
    /// - if `Ok(())` is returned, it means that the iterator successfully move to the next position
    ///   (may reach to the end and thus not valid)
    /// - if `Err(_) ` is returned, it means that some error happended.
    pub async fn next(&mut self) -> HummockResult<()> {
        while self.iterator.is_valid() {
            let key = to_user_key(self.iterator.key());

            // handle multi-version
            if self.last_key.as_slice() != key {
                self.last_key.clear();
                self.last_key.extend_from_slice(key);

                // handle delete operation
                match self.iterator.value() {
                    HummockValue::Put(val) => {
                        self.last_val.clear();
                        self.last_val.extend_from_slice(val);

                        // handle range scan
                        if let Some((_, end_key)) = &self.key_range {
                            self.out_of_range = key > end_key.as_slice();
                        }

                        return Ok(());
                    }
                    // It means that the key is deleted from the storage.
                    // Deleted kv and the previous verisons (if any) of the key should not be
                    // returned to user.
                    HummockValue::Delete => {}
                }
            }

            self.iterator.next().await?;
        }

        Ok(()) // not valid, EOF
    }

    /// Return the key with the newest version. Thus no version in it, and only the `user_key` will
    /// be returned.
    ///
    /// The returned key is de-duplicated and thus it will not output the same key, unless the
    /// `rewind` or `seek` mthods are called.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_key.as_slice()
    }

    /// The returned value is in the form of user value.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_val.as_slice()
    }

    /// Reset the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // handle range scan
        if let Some((begin_key, _)) = &self.key_range {
            let full_key = &key_with_ts(begin_key.clone(), u64::MAX);
            self.iterator.seek(full_key).await?;
        } else {
            self.iterator.rewind().await?;
        }

        // handle multi-version
        self.last_key.clear();
        // handle range scan when key > end_key
        self.next().await
    }

    /// Reset the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        // handle range scan when key < begin_key
        let user_key = match &self.key_range {
            Some((begin_key, _)) if begin_key.as_slice() > user_key => begin_key.clone(),
            _ => Vec::from(user_key),
        };

        let full_key = &key_with_ts(user_key, u64::MAX);
        self.iterator.seek(full_key).await?;

        // handle multi-version
        self.last_key.clear();
        // handle range scan when key > end_key
        self.next().await
    }

    pub fn is_valid(&self) -> bool {
        // handle range scan
        // key >= begin_key is guaranteed by seek/rewind function
        (!self.out_of_range) && self.iterator.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::cloud::gen_remote_table;
    use crate::hummock::format::{key_with_ts, user_key};
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_test_table_base, iterator_test_key_of, test_value_of,
        TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::BoxedHummockIterator;
    use crate::hummock::table::{Table, TableIterator};
    use crate::hummock::value::HummockValue;
    use crate::hummock::TableBuilder;
    use crate::object::{InMemObjectStore, ObjectStore};

    #[tokio::test]
    async fn test_basic() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        let si = SortedIterator::new(iters);
        let mut uki = UserKeyIterator::new(si, None);
        uki.rewind().await.unwrap();

        let mut i = 0;
        while uki.is_valid() {
            let k = uki.key();
            let v = uki.value();
            assert_eq!(k, user_key(iterator_test_key_of(0, i).as_slice()));
            assert_eq!(v, test_value_of(0, i).as_slice());

            i += 1;

            uki.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!uki.is_valid());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_seek() {
        let table2 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3).await;
        let table1 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 1).await;
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), &|x| x * 3 + 2).await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
            Box::new(TableIterator::new(Arc::new(table2))),
        ];

        let si = SortedIterator::new(iters);
        let mut uki = UserKeyIterator::new(si, None);

        // right edge case
        uki.seek(user_key(&iterator_test_key_of(0, 3 * TEST_KEYS_COUNT)))
            .await
            .unwrap();
        assert!(!uki.is_valid());

        // normal case
        uki.seek(user_key(&iterator_test_key_of(0, 4)))
            .await
            .unwrap();
        let k = uki.key();
        let v = uki.value();
        assert_eq!(k, user_key(iterator_test_key_of(0, 4).as_slice()));
        assert_eq!(v, test_value_of(0, 4).as_slice());

        uki.seek(user_key(&iterator_test_key_of(0, 17)))
            .await
            .unwrap();
        let k = uki.key();
        let v = uki.value();
        assert_eq!(k, user_key(iterator_test_key_of(0, 17).as_slice()));
        assert_eq!(v, test_value_of(0, 17).as_slice());

        // left edge case
        uki.seek(user_key(&iterator_test_key_of(0, 0)))
            .await
            .unwrap();
        let k = uki.key();
        let v = uki.value();
        assert_eq!(k, user_key(iterator_test_key_of(0, 0).as_slice()));
        assert_eq!(v, test_value_of(0, 0).as_slice());
    }

    #[tokio::test]
    async fn test_delete() {
        // key=[table, idx, ts], value
        let kv_pairs: Vec<(usize, usize, u64, HummockValue<Vec<u8>>)> = vec![
            (0, 1, 100, HummockValue::Put(test_value_of(0, 1))),
            (0, 2, 300, HummockValue::Delete),
        ];
        let table0 = add_kv_pair(kv_pairs).await;

        let kv_pairs: Vec<(usize, usize, u64, HummockValue<Vec<u8>>)> = vec![
            (0, 1, 200, HummockValue::Delete),
            (0, 2, 400, HummockValue::Put(test_value_of(0, 2))),
        ];
        let table1 = add_kv_pair(kv_pairs).await;

        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
        ];
        let si = SortedIterator::new(iters);
        let mut uki = UserKeyIterator::new(si, None);
        uki.rewind().await.unwrap();

        // verify
        let k = uki.key();
        let v = uki.value();
        assert_eq!(k, user_key(iterator_test_key_of(0, 2).as_slice()));
        assert_eq!(v, test_value_of(0, 2));

        // only one valid kv pair
        uki.next().await.unwrap();
        assert!(!uki.is_valid());
    }

    #[tokio::test]
    async fn test_range_scan() {
        // key=[table, idx, ts], value
        let kv_pairs: Vec<(usize, usize, u64, HummockValue<Vec<u8>>)> = vec![
            (0, 1, 200, HummockValue::Delete),
            (0, 1, 100, HummockValue::Put(test_value_of(0, 1))),
            (0, 2, 300, HummockValue::Put(test_value_of(0, 2))),
            (0, 2, 200, HummockValue::Delete),
            (0, 2, 100, HummockValue::Delete),
            (0, 3, 100, HummockValue::Put(test_value_of(0, 3))),
            (0, 5, 200, HummockValue::Delete),
            (0, 5, 100, HummockValue::Put(test_value_of(0, 5))),
            (0, 6, 100, HummockValue::Put(test_value_of(0, 6))),
            (0, 7, 200, HummockValue::Delete),
            (0, 7, 100, HummockValue::Put(test_value_of(0, 7))),
            (0, 8, 100, HummockValue::Put(test_value_of(0, 8))),
        ];
        let table = add_kv_pair(kv_pairs).await;
        let iters: Vec<BoxedHummockIterator> = vec![Box::new(TableIterator::new(Arc::new(table)))];
        let si = SortedIterator::new(iters);

        let begin_key = user_key(key_range_test_key(0, 2, 0).as_slice()).to_vec();
        let end_key = user_key(key_range_test_key(0, 7, 0).as_slice()).to_vec();
        let key_range = Some((begin_key, end_key));

        let mut uki = UserKeyIterator::new(si, key_range);

        // ----- basic iterate -----
        uki.rewind().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        uki.next().await.unwrap();
        assert!(!uki.is_valid());

        // ----- before-begin-range iterate -----
        uki.seek(user_key(iterator_test_key_of(0, 1).as_slice()))
            .await
            .unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        uki.next().await.unwrap();
        assert!(!uki.is_valid());

        // ----- begin-range iterate -----
        uki.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        uki.next().await.unwrap();
        assert_eq!(uki.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        uki.next().await.unwrap();
        assert!(!uki.is_valid());

        // ----- end-range iterate -----
        uki.seek(user_key(iterator_test_key_of(0, 7).as_slice()))
            .await
            .unwrap();
        assert!(!uki.is_valid());

        // ----- after-end-range iterate -----
        uki.seek(user_key(iterator_test_key_of(0, 8).as_slice()))
            .await
            .unwrap();
        assert!(!uki.is_valid());
    }

    // key=[table, idx, ts], value
    async fn add_kv_pair(kv_pairs: Vec<(usize, usize, u64, HummockValue<Vec<u8>>)>) -> Table {
        let mut b = TableBuilder::new(default_builder_opt_for_test());
        for kv in kv_pairs {
            b.add(key_range_test_key(kv.0, kv.1, kv.2).as_slice(), kv.3);
        }
        let (data, meta) = b.finish();
        // get remote table
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        gen_remote_table(obj_client, 0, data, meta, None)
            .await
            .unwrap()
    }

    fn key_range_test_key(table: usize, idx: usize, ts: u64) -> Vec<u8> {
        key_with_ts(
            format!("{:03}_key_test_{:05}", table, idx)
                .as_bytes()
                .to_vec(),
            ts,
        )
    }
}
