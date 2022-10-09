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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::VIRTUAL_NODE_SIZE;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table;
use tokio::sync::Notify;

use crate::key::{get_table_id, TABLE_PREFIX_LEN};

/// `FilterKeyExtractor` generally used to extract key which will store in BloomFilter
pub trait FilterKeyExtractor: Send + Sync {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8];
}

pub enum FilterKeyExtractorImpl {
    Schema(SchemaFilterKeyExtractor),
    FullKey(FullKeyFilterKeyExtractor),
    Dummy(DummyFilterKeyExtractor),
    Multi(MultiFilterKeyExtractor),
    FixedLength(FixedLengthFilterKeyExtractor),
}

impl FilterKeyExtractorImpl {
    pub fn from_table(table_catalog: &Table) -> Self {
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let pk_indices: Vec<usize> = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect();

        let match_read_pattern =
            !dist_key_indices.is_empty() && pk_indices.starts_with(&dist_key_indices);
        if !match_read_pattern {
            // for now frontend had not infer the table_id_to_filter_key_extractor, so we
            // use FullKeyFilterKeyExtractor
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor::default())
        } else {
            FilterKeyExtractorImpl::Schema(SchemaFilterKeyExtractor::new(table_catalog))
        }
    }
}

macro_rules! impl_filter_key_extractor {
    ($( { $variant_name:ident } ),*) => {
        impl FilterKeyExtractorImpl {
            pub fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8]{
                match self {
                    $( Self::$variant_name(inner) => inner.extract(full_key), )*
                }
            }
        }
    }
}

macro_rules! for_all_filter_key_extractor_variants {
    ($macro:ident) => {
        $macro! {
            { Schema },
            { FullKey },
            { Dummy },
            { Multi },
            { FixedLength }
        }
    };
}

for_all_filter_key_extractor_variants! { impl_filter_key_extractor }

#[derive(Default)]
pub struct FullKeyFilterKeyExtractor;

impl FilterKeyExtractor for FullKeyFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        full_key
    }
}

#[derive(Default)]
pub struct DummyFilterKeyExtractor;
impl FilterKeyExtractor for DummyFilterKeyExtractor {
    fn extract<'a>(&self, _full_key: &'a [u8]) -> &'a [u8] {
        &[]
    }
}

/// [`SchemaFilterKeyExtractor`] build from `table_catalog` and extract a `full_key` to prefix for
#[derive(Default)]
pub struct FixedLengthFilterKeyExtractor {
    fixed_length: usize,
}

impl FilterKeyExtractor for FixedLengthFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        &full_key[0..self.fixed_length]
    }
}

impl FixedLengthFilterKeyExtractor {
    pub fn new(fixed_length: usize) -> Self {
        Self { fixed_length }
    }
}

/// [`SchemaFilterKeyExtractor`] build from `table_catalog` and transform a `full_key` to prefix for
/// `prefix_bloom_filter`
pub struct SchemaFilterKeyExtractor {
    /// Each stateful operator has its own read pattern, partly using prefix scan.
    /// Prefix key length can be decoded through its `DataType` and `OrderType` which obtained from
    /// `TableCatalog`. `read_pattern_prefix_column` means the count of column to decode prefix
    /// from storage key.
    read_pattern_prefix_column: usize,
    deserializer: OrderedRowSerde,
    // TODO:need some bench test for same prefix case like join (if we need a prefix_cache for same
    // prefix_key)
}

impl FilterKeyExtractor for SchemaFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        if full_key.len() < TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE {
            return full_key;
        }

        let (_table_prefix, key) = full_key.split_at(TABLE_PREFIX_LEN);
        let (_vnode_prefix, pk) = key.split_at(VIRTUAL_NODE_SIZE);

        // if the key with table_id deserializer fail from schema, that should panic here for early
        // detection
        let pk_prefix_len = self
            .deserializer
            .deserialize_prefix_len_with_column_indices(pk, 0..self.read_pattern_prefix_column)
            .unwrap();

        let prefix_len = TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len;
        &full_key[0..prefix_len]
    }
}

impl SchemaFilterKeyExtractor {
    pub fn new(table_catalog: &Table) -> Self {
        let read_pattern_prefix_column = table_catalog.distribution_key.len();
        assert_ne!(0, read_pattern_prefix_column);

        // column_index in pk
        let pk_indices: Vec<usize> = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect();

        let data_types = pk_indices
            .iter()
            .map(|column_idx| &table_catalog.columns[*column_idx])
            .map(|col| ColumnDesc::from(col.column_desc.as_ref().unwrap()).data_type)
            .collect();

        let order_types: Vec<OrderType> = table_catalog
            .pk
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();

        Self {
            read_pattern_prefix_column,
            deserializer: OrderedRowSerde::new(data_types, order_types),
        }
    }
}

#[derive(Default)]
pub struct MultiFilterKeyExtractor {
    id_to_filter_key_extractor: HashMap<u32, Arc<FilterKeyExtractorImpl>>,
    // cached state
    // last_filter_key_extractor_state: Mutex<Option<(u32, Arc<FilterKeyExtractorImpl>)>>,
}

impl MultiFilterKeyExtractor {
    pub fn register(&mut self, table_id: u32, filter_key_extractor: Arc<FilterKeyExtractorImpl>) {
        self.id_to_filter_key_extractor
            .insert(table_id, filter_key_extractor);
    }

    pub fn size(&self) -> usize {
        self.id_to_filter_key_extractor.len()
    }
}

impl Debug for MultiFilterKeyExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiFilterKeyExtractor size {} ", self.size())
    }
}

impl FilterKeyExtractor for MultiFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        if full_key.len() < TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE {
            return full_key;
        }

        let table_id = get_table_id(full_key).unwrap();
        self.id_to_filter_key_extractor
            .get(&table_id)
            .unwrap()
            .extract(full_key)
    }
}

#[derive(Default)]
struct FilterKeyExtractorManagerInner {
    table_id_to_filter_key_extractor: RwLock<HashMap<u32, Arc<FilterKeyExtractorImpl>>>,
    notify: Notify,
}

impl FilterKeyExtractorManagerInner {
    fn update(&self, table_id: u32, filter_key_extractor: Arc<FilterKeyExtractorImpl>) {
        self.table_id_to_filter_key_extractor
            .write()
            .insert(table_id, filter_key_extractor);

        self.notify.notify_waiters();
    }

    fn sync(&self, filter_key_extractor_map: HashMap<u32, Arc<FilterKeyExtractorImpl>>) {
        let mut guard = self.table_id_to_filter_key_extractor.write();
        guard.clear();
        guard.extend(filter_key_extractor_map);
        self.notify.notify_waiters();
    }

    fn remove(&self, table_id: u32) {
        self.table_id_to_filter_key_extractor
            .write()
            .remove(&table_id);

        self.notify.notify_waiters();
    }

    async fn acquire(&self, mut table_id_set: HashSet<u32>) -> FilterKeyExtractorImpl {
        if table_id_set.is_empty() {
            // table_id_set is empty
            // the table in sst has been deleted

            // use full key as default
            return FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor::default());
        }

        #[cfg(debug_assertions)]
        {
            // for some unit-test not config table_id_set
            if table_id_set.iter().any(|table_id| *table_id == 0) {
                return FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor::default());
            }
        }

        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        while !table_id_set.is_empty() {
            let notified = self.notify.notified();

            {
                let guard = self.table_id_to_filter_key_extractor.read();
                table_id_set.drain_filter(|table_id| match guard.get(table_id) {
                    Some(filter_key_extractor) => {
                        multi_filter_key_extractor
                            .register(*table_id, filter_key_extractor.clone());
                        true
                    }

                    None => false,
                });
            }

            if !table_id_set.is_empty() {
                notified.await;
            }
        }

        FilterKeyExtractorImpl::Multi(multi_filter_key_extractor)
    }
}

/// `FilterKeyExtractorManager` is a wrapper for inner, and provide a protected read and write
/// interface, its thread safe
#[derive(Default)]
pub struct FilterKeyExtractorManager {
    inner: FilterKeyExtractorManagerInner,
}

impl FilterKeyExtractorManager {
    /// Insert (`table_id`, `filter_key_extractor`) as mapping to `HashMap` for `acquire`
    pub fn update(&self, table_id: u32, filter_key_extractor: Arc<FilterKeyExtractorImpl>) {
        self.inner.update(table_id, filter_key_extractor);
    }

    /// Remove a mapping by `table_id`
    pub fn remove(&self, table_id: u32) {
        self.inner.remove(table_id);
    }

    /// Sync all filter key extractors by snapshot
    pub fn sync(&self, filter_key_extractor_map: HashMap<u32, Arc<FilterKeyExtractorImpl>>) {
        self.inner.sync(filter_key_extractor_map)
    }

    /// Acquire a `MultiFilterKeyExtractor` by `table_id_set`
    /// Internally, try to get all `filter_key_extractor` from `hashmap`. Will block the caller if
    /// `table_id` does not util version update (notify), and retry to get
    pub async fn acquire(&self, table_id_set: HashSet<u32>) -> FilterKeyExtractorImpl {
        self.inner.acquire(table_id_set).await
    }
}

pub type FilterKeyExtractorManagerRef = Arc<FilterKeyExtractorManager>;

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::mem;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{BufMut, BytesMut};
    use itertools::Itertools;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::config::constant::hummock::PROPERTIES_RETENTION_SECOND_KEY;
    use risingwave_common::types::ScalarImpl::{self};
    use risingwave_common::types::{DataType, VIRTUAL_NODE_SIZE};
    use risingwave_common::util::ordered::OrderedRowSerde;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::plan_common::{ColumnCatalog as ProstColumnCatalog, ColumnOrder};
    use tokio::task;

    use super::{DummyFilterKeyExtractor, FilterKeyExtractor, SchemaFilterKeyExtractor};
    use crate::filter_key_extractor::{
        FilterKeyExtractorImpl, FilterKeyExtractorManager, FullKeyFilterKeyExtractor,
        MultiFilterKeyExtractor,
    };
    use crate::key::TABLE_PREFIX_LEN;

    #[test]
    fn test_default_filter_key_extractor() {
        let dummy_filter_key_extractor = DummyFilterKeyExtractor::default();
        let full_key = "full_key".as_bytes();
        let output_key = dummy_filter_key_extractor.extract(full_key);

        assert_eq!("".as_bytes(), output_key);

        let full_key_filter_key_extractor = FullKeyFilterKeyExtractor::default();
        let output_key = full_key_filter_key_extractor.extract(full_key);

        assert_eq!(full_key, output_key);
    }

    fn build_table_with_prefix_column_num(column_count: u32) -> ProstTable {
        ProstTable {
            is_index: false,
            id: 0,
            schema_id: 0,
            database_id: 0,
            name: "test".to_string(),
            columns: vec![
                ProstColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc {
                            data_type: DataType::Int64,
                            column_id: ColumnId::new(0),
                            name: "_row_id".to_string(),
                            field_descs: vec![],
                            type_name: "".to_string(),
                        })
                            .into(),
                    ),
                    is_hidden: true,
                },
                ProstColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc {
                            data_type: DataType::Int64,
                            column_id: ColumnId::new(0),
                            name: "col_1".to_string(),
                            field_descs: vec![],
                            type_name: "Int64".to_string(),
                        })
                            .into(),
                    ),
                    is_hidden: false,
                },
                ProstColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc {
                            data_type: DataType::Float64,
                            column_id: ColumnId::new(0),
                            name: "col_2".to_string(),
                            field_descs: vec![],
                            type_name: "Float64".to_string(),
                        })
                            .into(),
                    ),
                    is_hidden: false,
                },
                ProstColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc {
                            data_type: DataType::Varchar,
                            column_id: ColumnId::new(0),
                            name: "col_3".to_string(),
                            field_descs: vec![],
                            type_name: "Varchar".to_string(),
                        })
                            .into(),
                    ),
                    is_hidden: false,
                },
            ],
            pk: vec![
                ColumnOrder {
                    order_type: 1, // Ascending
                    index: 1,
                },
                ColumnOrder {
                    order_type: 1, // Ascending
                    index: 3,
                },
            ],
            stream_key: vec![0],
            dependent_relations: vec![],
            distribution_key: (0..column_count as i32).collect_vec(),
            optional_associated_source_id: None,
            appendonly: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties: HashMap::from([(
                String::from(PROPERTIES_RETENTION_SECOND_KEY),
                String::from("300"),
            )]),
            fragment_id: 0,
            vnode_col_idx: None,
            value_indices: vec![0],
            definition: "".into(),
        }
    }

    #[test]
    fn test_schema_filter_key_extractor() {
        let prost_table = build_table_with_prefix_column_num(1);
        let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);

        let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];
        let schema = vec![DataType::Int64, DataType::Varchar];
        let serializer = OrderedRowSerde::new(schema, order_types);
        let row = Row(vec![
            Some(ScalarImpl::Int64(100)),
            Some(ScalarImpl::Utf8("abc".to_string())),
        ]);
        let mut row_bytes = vec![];
        serializer.serialize(&row, &mut row_bytes);

        let table_prefix = {
            let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
            buf.put_u8(b't');
            buf.put_u32(1);
            buf.to_vec()
        };

        let vnode_prefix = "v".as_bytes();
        assert_eq!(VIRTUAL_NODE_SIZE, vnode_prefix.len());

        let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
        let output_key = schema_filter_key_extractor.extract(&full_key);
        assert_eq!(
            TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + 1 + mem::size_of::<i64>(),
            output_key.len()
        );
    }

    #[test]
    fn test_multi_filter_key_extractor() {
        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        // let last_state = multi_filter_key_extractor.last_filter_key_extractor_state();
        // assert!(last_state.is_none());

        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(1);
            let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);
            multi_filter_key_extractor.register(
                1,
                Arc::new(FilterKeyExtractorImpl::Schema(schema_filter_key_extractor)),
            );
            let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];
            let schema = vec![DataType::Int64, DataType::Varchar];
            let serializer = OrderedRowSerde::new(schema, order_types);
            let row = Row(vec![
                Some(ScalarImpl::Int64(100)),
                Some(ScalarImpl::Utf8("abc".to_string())),
            ]);
            let mut row_bytes = vec![];
            serializer.serialize(&row, &mut row_bytes);

            let table_prefix = {
                let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
                buf.put_u8(b't');
                buf.put_u32(1);
                buf.to_vec()
            };

            let vnode_prefix = "v".as_bytes();
            assert_eq!(VIRTUAL_NODE_SIZE, vnode_prefix.len());

            let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
            let output_key = multi_filter_key_extractor.extract(&full_key);

            let data_types = vec![DataType::Int64];
            let order_types = vec![OrderType::Ascending];
            let deserializer = OrderedRowSerde::new(data_types, order_types);

            let pk_prefix_len = deserializer
                .deserialize_prefix_len_with_column_indices(&row_bytes, 0..=0)
                .unwrap();
            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len,
                output_key.len()
            );

            // let last_state = multi_filter_key_extractor.last_filter_key_extractor_state();
            // assert!(last_state.is_some());
            // assert_eq!(1, last_state.as_ref().unwrap().0);
        }

        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(2);
            let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);
            multi_filter_key_extractor.register(
                2,
                Arc::new(FilterKeyExtractorImpl::Schema(schema_filter_key_extractor)),
            );
            let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];
            let schema = vec![DataType::Int64, DataType::Varchar];
            let serializer = OrderedRowSerde::new(schema, order_types);
            let row = Row(vec![
                Some(ScalarImpl::Int64(100)),
                Some(ScalarImpl::Utf8("abc".to_string())),
            ]);
            let mut row_bytes = vec![];
            serializer.serialize(&row, &mut row_bytes);

            let table_prefix = {
                let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
                buf.put_u8(b't');
                buf.put_u32(2);
                buf.to_vec()
            };

            let vnode_prefix = "v".as_bytes();
            assert_eq!(VIRTUAL_NODE_SIZE, vnode_prefix.len());

            let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
            let output_key = multi_filter_key_extractor.extract(&full_key);

            let data_types = vec![DataType::Int64, DataType::Varchar];
            let order_types = vec![OrderType::Ascending, OrderType::Ascending];
            let deserializer = OrderedRowSerde::new(data_types, order_types);

            let pk_prefix_len = deserializer
                .deserialize_prefix_len_with_column_indices(&row_bytes, 0..=1)
                .unwrap();

            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len,
                output_key.len()
            );

            // let last_state = multi_filter_key_extractor.last_filter_key_extractor_state();
            // assert!(last_state.is_some());
            // assert_eq!(2, last_state.as_ref().unwrap().0);
        }

        {
            let full_key_filter_key_extractor = FullKeyFilterKeyExtractor::default();
            multi_filter_key_extractor.register(
                3,
                Arc::new(FilterKeyExtractorImpl::FullKey(
                    full_key_filter_key_extractor,
                )),
            );

            let table_prefix = {
                let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
                buf.put_u8(b't');
                buf.put_u32(3);
                buf.to_vec()
            };

            let vnode_prefix = "v".as_bytes();
            assert_eq!(VIRTUAL_NODE_SIZE, vnode_prefix.len());

            let row_bytes = "full_key".as_bytes();

            let full_key = [&table_prefix, vnode_prefix, row_bytes].concat();
            let output_key = multi_filter_key_extractor.extract(&full_key);
            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + row_bytes.len(),
                output_key.len()
            );

            // let last_state = multi_filter_key_extractor.last_filter_key_extractor_state();
            // assert!(last_state.is_some());
            // assert_eq!(3, last_state.as_ref().unwrap().0);
        }
    }

    #[tokio::test]
    async fn test_filter_key_extractor_manager() {
        let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
        let filter_key_extractor_manager_ref = filter_key_extractor_manager.clone();
        let filter_key_extractor_manager_ref2 = filter_key_extractor_manager_ref.clone();

        task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            filter_key_extractor_manager_ref.update(
                1,
                Arc::new(FilterKeyExtractorImpl::Dummy(
                    DummyFilterKeyExtractor::default(),
                )),
            );
        });

        let remaining_table_id_set = HashSet::from([1]);
        let multi_filter_key_extractor = filter_key_extractor_manager_ref2
            .acquire(remaining_table_id_set)
            .await;

        match multi_filter_key_extractor {
            FilterKeyExtractorImpl::Multi(multi_filter_key_extractor) => {
                assert_eq!(1, multi_filter_key_extractor.size());
            }

            _ => {
                unreachable!()
            }
        }
    }
}
