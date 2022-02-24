#![allow(dead_code)]

use std::sync::Arc;

use super::variants::*;
use crate::hummock::key::{key_with_epoch, user_key, Epoch};
use crate::hummock::{
    cloud, HummockResult, HummockValue, SSTable, SSTableBuilder, SSTableBuilderOptions,
};
use crate::object::{InMemObjectStore, ObjectStore};

pub trait IndexMapper: Fn(u64, usize) -> Vec<u8> + Send + Sync + 'static {}
impl<T> IndexMapper for T where T: Fn(u64, usize) -> Vec<u8> + Send + Sync + 'static {}
type BoxedIndexMapper = Box<dyn IndexMapper>;

/// `assert_eq` two `Vec<u8>` with human-readable format.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {{
        use bytes::Bytes;
        assert_eq!(
            Bytes::copy_from_slice(&$left),
            Bytes::copy_from_slice(&$right)
        )
    }};
}

pub struct TestIteratorConfig {
    key_mapper: BoxedIndexMapper,
    value_mapper: BoxedIndexMapper,
    id: u64,
    total: usize,
    table_builder_opt: SSTableBuilderOptions,
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
            key_mapper: Box::new(|id, index| {
                format!("{:03}_key_test_{:05}", id, index)
                    .as_bytes()
                    .to_vec()
            }),
            value_mapper: Box::new(|id, index| {
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
pub struct TestIteratorInner<const DIRECTION: usize> {
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
pub struct TestIteratorBuilder<const DIRECTION: usize> {
    cfg: TestIteratorConfig,
}

impl<const DIRECTION: usize> TestIteratorBuilder<DIRECTION> {
    pub fn total(mut self, t: usize) -> Self {
        self.cfg.total = t;
        self
    }

    pub fn map_key(mut self, m: impl IndexMapper) -> Self {
        self.cfg.key_mapper = Box::new(m);
        self
    }

    pub fn map_value(mut self, m: impl IndexMapper) -> Self {
        self.cfg.value_mapper = Box::new(m);
        self
    }

    pub fn id(mut self, id: u64) -> Self {
        self.cfg.id = id;
        self
    }

    pub fn finish(self) -> (TestIteratorInner<DIRECTION>, TestValidator) {
        TestIteratorInner::<DIRECTION>::new(Arc::new(self.cfg))
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
        assert_eq!(key, expected.as_slice());
    }

    #[inline]
    pub fn assert_user_key(&self, idx: usize, key: &[u8]) {
        let expected = self.cfg.gen_key(idx);

        let expected = user_key(&expected);
        assert_eq!(key, expected);
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
        assert_eq!(real, expected.as_slice());
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

use risingwave_pb::hummock::SstableMeta;
pub(crate) use test_key;

pub type TestIterator = TestIteratorInner<FORWARD>;
pub type ReverseTestIterator = TestIteratorInner<BACKWARD>;

impl<const DIRECTION: usize> TestIteratorInner<DIRECTION> {
    /// Caller should make sure that `gen_key`
    /// would generate keys arranged by the same order as `DIRECTION`.
    fn new(cfg: Arc<TestIteratorConfig>) -> (Self, TestValidator) {
        let data = (0..cfg.total)
            .map(|x| (Bytes::from(cfg.gen_key(x)), Bytes::from(cfg.gen_value(x))))
            .collect_vec();

        let test_iter = TestIteratorInner {
            cfg: cfg.clone(),
            cur_idx: 0,
            data,
        };

        let test_validator = TestValidator::new(cfg);

        (test_iter, test_validator)
    }
    fn seek_inner(&mut self, key: &[u8]) -> HummockResult<()> {
        self.cur_idx = match DIRECTION {
            FORWARD => self.data.partition_point(|x| x.0 < key),
            BACKWARD => self.data.partition_point(|x| x.0 > key),
            _ => unreachable!(),
        };
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
impl<const DIRECTION: usize> HummockIterator for TestIteratorInner<DIRECTION> {
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

pub fn default_builder_opt_for_test() -> SSTableBuilderOptions {
    SSTableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,                // 4KB
        table_capacity: 256 * (1 << 20), // 256MB
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    }
}

/// Generate keys like `001_key_test_00002` with epoch 233.
pub fn iterator_test_key_of(table: u64, idx: usize) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        233,
    )
}

/// Generate keys like `001_key_test_00002` with epoch `epoch`.
pub fn iterator_test_key_of_epoch(table: u64, idx: usize, epoch: Epoch) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        epoch,
    )
}

/// The value of an index in the test table
pub fn test_value_of(table: u64, idx: usize) -> Vec<u8> {
    format!("{:03}_value_test_{:05}", table, idx)
        .as_bytes()
        .to_vec()
}

pub async fn gen_test_sstable(table_idx: u64, opts: SSTableBuilderOptions) -> SSTable {
    gen_test_sstable_base(table_idx, opts, |x| x).await
}

/// Generate a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_test_sstable_base(
    table_idx: u64,
    opts: SSTableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
) -> SSTable {
    const REMOTE_DIR: &str = "test";
    let mut b = SSTableBuilder::new(opts);

    for i in 0..TEST_KEYS_COUNT {
        b.add(
            &iterator_test_key_of(table_idx, idx_mapping(i)),
            HummockValue::Put(&test_value_of(table_idx, idx_mapping(i))),
        );
    }

    // get remote table
    let (data, meta) = b.finish();
    let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    upload_and_load_sst(obj_client, 0, meta, data, REMOTE_DIR)
        .await
        .unwrap()
}

pub async fn upload_and_load_sst(
    obj_client: Arc<dyn ObjectStore>,
    sst_id: u64,
    meta: SstableMeta,
    data: Bytes,
    path: &str,
) -> HummockResult<SSTable> {
    cloud::upload(&obj_client, sst_id, &meta, data, path).await?;
    Ok(SSTable {
        id: sst_id,
        meta,
        obj_client: obj_client,
        data_path: cloud::get_sst_data_path(path, sst_id),
        block_cache: Arc::new(moka::future::Cache::new(65536)),
    })
}

#[cfg(test)]
mod metatest {
    use super::*;
    #[tokio::test]
    async fn test_basic() {
        let (_, val) = TestIteratorBuilder::<FORWARD>::default()
            .id(0)
            .map_key(|id, x| iterator_test_key_of(id, x * 3))
            .finish();

        let expected = iterator_test_key_of(0, 9);
        assert!(expected.as_slice() == test_key!(val, 3));
    }
}
