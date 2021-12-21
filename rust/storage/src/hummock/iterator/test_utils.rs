use std::sync::Arc;

use crate::hummock::cloud::gen_remote_table;
use crate::hummock::key::{key_with_ts, user_key};
use crate::hummock::{HummockResult, HummockValue, Table, TableBuilder, TableBuilderOptions};
use crate::object::{InMemObjectStore, ObjectStore};
type IndexMapper = Box<dyn Fn(u64, usize) -> Vec<u8> + Send + Sync>;

pub struct TestIteratorConfig {
    key_mapper: IndexMapper,
    value_mapper: IndexMapper,
    id: u64,
    total: usize,
    table_builder_opt: TableBuilderOptions,
}

impl TestIteratorConfig {
    fn gen_key(&self, idx: usize) -> Vec<u8> {
        (self.key_mapper)(self.id, idx)
    }
    fn gen_value(&self, idx: usize) -> Vec<u8> {
        (self.value_mapper)(self.id, idx)
    }
}

impl Default for TestIteratorConfig {
    fn default() -> Self {
        Self {
            key_mapper: Box::new(move |id, index| {
                format!("{:03}_key_test_{:05}", id, index)
                    .as_bytes()
                    .to_vec()
            }),
            value_mapper: Box::new(move |id, index| {
                format!("{:03}_value_test_{:05}", id, index)
                    .as_bytes()
                    .iter()
                    .cycle()
                    .cloned()
                    .take(index % 100 + 1) // so that the table is not too big
                    .collect_vec()
            }),
            id: 0,
            total: TEST_KEYS_COUNT,
            table_builder_opt: default_builder_opt_for_test(),
        }
    }
}
/// Test iterator stores a buffer of key-value pairs `Vec<(Bytes, Bytes)>` and yields the data
/// stored in the buffer.
pub struct TestIterator {
    cfg: Arc<TestIteratorConfig>,
    data: Vec<(Bytes, Bytes)>,
    cur_idx: usize,
}
pub struct TestValidator {
    cfg: Arc<TestIteratorConfig>,
    key: Vec<u8>,
    value: Vec<u8>,
}
#[derive(Default)]
pub struct TestIteratorBuilder {
    cfg: TestIteratorConfig,
}

impl TestIteratorBuilder {
    pub fn total(&mut self, t: usize) -> &mut Self {
        self.cfg.total = t;
        self
    }

    pub fn map_key(&mut self, m: IndexMapper) -> &mut Self {
        self.cfg.key_mapper = m;
        self
    }

    pub fn map_value(&mut self, m: IndexMapper) -> &mut Self {
        self.cfg.value_mapper = m;
        self
    }

    pub fn id(&mut self, id: u64) -> &mut Self {
        self.cfg.id = id;
        self
    }

    pub fn finish(&mut self) -> (TestIterator, TestValidator) {
        TestIterator::new(Arc::new(std::mem::take(&mut self.cfg)))
    }
}

impl TestValidator {
    fn new(cfg: Arc<TestIteratorConfig>) -> Self {
        Self {
            cfg,
            key: vec![],
            value: vec![],
        }
    }
    #[inline]
    pub fn assert_key(&self, idx: usize, key: &[u8]) {
        let expected = self.cfg.gen_key(idx);
        assert!(key == expected.as_slice());
    }

    #[inline]
    pub fn assert_user_key(&self, idx: usize, key: &[u8]) {
        let expected = self.cfg.gen_key(idx);

        let expected = user_key(&expected);
        assert!(key == expected);
    }

    #[inline]
    pub fn assert_hummock_value(&self, idx: usize, value: HummockValue<&[u8]>) {
        let real = value.into_put_value().unwrap();
        self.assert_value(idx, real)
    }

    #[inline]
    pub fn assert_value(&self, idx: usize, value: &[u8]) {
        let expected = self.cfg.gen_value(idx);
        let real = value;
        assert!(real == expected.as_slice());
    }

    #[inline]
    pub fn key(&self, idx: usize) -> Vec<u8> {
        self.cfg.gen_key(idx)
    }

    #[inline]
    pub fn value(&self, idx: usize) -> Vec<u8> {
        self.cfg.gen_value(idx)
    }
}

macro_rules! test_key {
    ($val:expr, $idx:expr) => {
        $val.key($idx).as_slice()
    };
}

pub(crate) use test_key;

impl TestIterator {
    fn new(cfg: Arc<TestIteratorConfig>) -> (Self, TestValidator) {
        let data = (0..cfg.total)
            .map(|x| (Bytes::from(cfg.gen_key(x)), Bytes::from(cfg.gen_value(x))))
            .collect_vec();

        let test_iter = TestIterator {
            cfg: cfg.clone(),
            cur_idx: 0,
            data,
        };

        let test_validator = TestValidator::new(cfg);

        (test_iter, test_validator)
    }
    fn seek_inner(&mut self, key: &[u8]) -> HummockResult<()> {
        self.cur_idx = self.data.partition_point(|x| key > x.0);
        Ok(())
    }
    pub fn key(&self) -> &[u8] {
        self.data[self.cur_idx].0.as_ref()
    }
    pub fn value(&self) -> HummockValue<&[u8]> {
        HummockValue::Put(self.data[self.cur_idx].1.as_ref())
    }
}

#[async_trait]
impl HummockIterator for TestIterator {
    async fn next(&mut self) -> HummockResult<()> {
        self.cur_idx += 1;
        Ok(())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.cur_idx = 0;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.seek_inner(key)
    }

    fn is_valid(&self) -> bool {
        self.cur_idx < self.data.len()
    }

    fn key(&self) -> &[u8] {
        self.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.value()
    }
}

pub const TEST_KEYS_COUNT: usize = 10;
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;

use super::HummockIterator;

pub fn default_builder_opt_for_test() -> TableBuilderOptions {
    TableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 16384,               // 16KB
        table_capacity: 256 * (1 << 20), // 256MB
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    }
}

/// The key of an index in the test table
pub fn iterator_test_key_of(table: u64, idx: usize) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_ts(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        233,
    )
}

/// The value of an index in the test table
pub fn test_value_of(table: u64, idx: usize) -> Vec<u8> {
    format!("{:03}_value_test_{:05}", table, idx)
        .as_bytes()
        .to_vec()
}

pub async fn gen_test_table(table_idx: u64, opts: TableBuilderOptions) -> Table {
    gen_test_table_base(table_idx, opts, &|x| x).await
}

/// Generate a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_test_table_base(
    table_idx: u64,
    opts: TableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
) -> Table {
    let mut b = TableBuilder::new(opts);

    for i in 0..TEST_KEYS_COUNT {
        b.add(
            &iterator_test_key_of(table_idx, idx_mapping(i)),
            HummockValue::Put(test_value_of(table_idx, idx_mapping(i))),
        );
    }

    // get remote table
    let (data, meta) = b.finish();
    let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    gen_remote_table(obj_client, 0, data, meta, None)
        .await
        .unwrap()
}

#[cfg(test)]
mod metatest {
    use super::*;
    #[tokio::test]
    async fn test_basic() {
        let (_, val) = TestIteratorBuilder::default()
            .id(0)
            .map_key(Box::new(move |id, x| iterator_test_key_of(id, x * 3)))
            .finish();

        let expected = iterator_test_key_of(0, 9);
        assert!(expected.as_slice() == test_key!(val, 3));
    }
}
