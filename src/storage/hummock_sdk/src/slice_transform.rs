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

use std::collections::HashMap;
use std::fmt::Debug;

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::VIRTUAL_NODE_SIZE;
use risingwave_common::util::ordered::OrderedRowDeserializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table;

use crate::key::{get_table_id, TABLE_PREFIX_LEN};

/// Slice Transform generally used to transform key which will store in BloomFilter
pub trait SliceTransform: Clone + Send + Sync {
    fn transform<'a>(&mut self, full_key: &'a [u8]) -> &'a [u8];
}

#[derive(Clone)]
pub enum SliceTransformImpl {
    Schema(SchemaSliceTransform),
    FullKey(FullKeySliceTransform),
    Dummy(DummySliceTransform),
    Multi(MultiSliceTransform),
}

macro_rules! impl_slice_transform {
    ([], $( { $variant_name:ident } ),*) => {
        impl SliceTransformImpl {
            pub fn transform<'a>(&mut self, full_key: &'a [u8]) -> &'a [u8]{
                match self {
                    $( Self::$variant_name(inner) => inner.transform(full_key), )*
                }
            }
        }
    }
}

macro_rules! for_all_slice_transform_variants {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x), *],
            { Schema },
            { FullKey },
            { Dummy },
            { Multi }
        }
    };
}

for_all_slice_transform_variants! { impl_slice_transform }

#[derive(Default, Clone)]
pub struct FullKeySliceTransform;

impl SliceTransform for FullKeySliceTransform {
    fn transform<'a>(&mut self, full_key: &'a [u8]) -> &'a [u8] {
        full_key
    }
}

#[derive(Default, Clone)]
pub struct DummySliceTransform;
impl SliceTransform for DummySliceTransform {
    fn transform<'a>(&mut self, _full_key: &'a [u8]) -> &'a [u8] {
        &[]
    }
}

/// [`SchemaSliceTransform`] build from table_catalog and transform a `full_key` to prefix for
/// prefix_bloom_filter
#[derive(Clone)]
pub struct SchemaSliceTransform {
    /// Each stateful operator has its own read pattern, partly using prefix scan.
    /// Perfix key length can be decoded through its `DataType` and `OrderType` which obtained from
    /// `TableCatalog`. `read_pattern_prefix_column` means the count of column to decode prefix
    /// from storage key.
    read_pattern_prefix_column: u32,
    deserializer: OrderedRowDeserializer,
    // TODO:need some bench test for same prefix case like join (if we need a prefix_cache for same
    // prefix_key)
}

impl SliceTransform for SchemaSliceTransform {
    fn transform<'a>(&mut self, full_key: &'a [u8]) -> &'a [u8] {
        debug_assert!(full_key.len() >= TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE);

        let (_table_prefix, key) = full_key.split_at(TABLE_PREFIX_LEN);
        let (_vnode_prefix, pk) = key.split_at(VIRTUAL_NODE_SIZE);

        // if the key with table_id deserializer fail from schema, that shoud panic here for early
        // detection
        let pk_prefix_len = self
            .deserializer
            .deserialize_prefix_len_with_column_indices(
                pk,
                0..self.read_pattern_prefix_column as usize,
            )
            .unwrap();

        let prefix_len = TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len;
        &full_key[0..prefix_len]
    }
}

impl SchemaSliceTransform {
    pub fn new(table_catalog: &Table) -> Self {
        assert_ne!(0, table_catalog.read_pattern_prefix_column);

        // column_index in pk
        let pk_indices: Vec<usize> = table_catalog
            .order_key
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect();

        let data_types = pk_indices
            .iter()
            .map(|column_idx| &table_catalog.columns[*column_idx])
            .map(|col| ColumnDesc::from(col.column_desc.as_ref().unwrap()).data_type)
            .collect();

        let order_types: Vec<OrderType> = table_catalog
            .order_key
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();

        Self {
            read_pattern_prefix_column: table_catalog.read_pattern_prefix_column,
            deserializer: OrderedRowDeserializer::new(data_types, order_types),
        }
    }
}

#[derive(Default, Clone)]
pub struct MultiSliceTransform {
    id_to_slice_transform: HashMap<u32, SliceTransformImpl>,

    // cached state
    last_slice_transform_state: Option<(u32, Box<SliceTransformImpl>)>,
}

impl MultiSliceTransform {
    pub fn register(&mut self, table_id: u32, slice_transform: SliceTransformImpl) {
        self.id_to_slice_transform.insert(table_id, slice_transform);
    }

    fn update_state(&mut self, new_table_id: u32) {
        self.last_slice_transform_state = Some((
            new_table_id,
            Box::new(
                self.id_to_slice_transform
                    .get(&new_table_id)
                    .unwrap()
                    .clone(),
            ),
        ));
    }

    pub fn size(&self) -> usize {
        self.id_to_slice_transform.len()
    }

    #[cfg(test)]
    fn last_slice_transform_state(&self) -> &Option<(u32, Box<SliceTransformImpl>)> {
        &self.last_slice_transform_state
    }
}

impl Debug for MultiSliceTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiSliceTransform size {} ", self.size())
    }
}

impl SliceTransform for MultiSliceTransform {
    fn transform<'a>(&mut self, full_key: &'a [u8]) -> &'a [u8] {
        assert!(full_key.len() > TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE);

        let table_id = get_table_id(full_key).unwrap();
        match self.last_slice_transform_state.as_ref() {
            Some(last_slice_transform_state) => {
                if table_id != last_slice_transform_state.0 {
                    self.update_state(table_id);
                }
            }

            None => {
                self.update_state(table_id);
            }
        }

        self.last_slice_transform_state
            .as_mut()
            .unwrap()
            .1
            .transform(full_key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::mem;

    use bytes::{BufMut, BytesMut};
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::ScalarImpl::{self};
    use risingwave_common::types::{DataType, VIRTUAL_NODE_SIZE};
    use risingwave_common::util::ordered::{OrderedRowDeserializer, OrderedRowSerializer};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::plan_common::{ColumnCatalog as ProstColumnCatalog, ColumnOrder};

    use super::{DummySliceTransform, SchemaSliceTransform, SliceTransform};
    use crate::key::TABLE_PREFIX_LEN;
    use crate::slice_transform::{FullKeySliceTransform, MultiSliceTransform, SliceTransformImpl};

    #[test]
    fn test_default_slice_transform() {
        let mut dummy_slice_transform = DummySliceTransform::default();
        let full_key = "full_key".as_bytes();
        let output_key = dummy_slice_transform.transform(full_key);

        assert_eq!("".as_bytes(), output_key);

        let mut full_key_slice_transform = FullKeySliceTransform::default();
        let output_key = full_key_slice_transform.transform(full_key);

        assert_eq!(full_key, output_key);
    }

    fn build_table_with_prefix_column_num(column_count: u32) -> ProstTable {
        #[expect(clippy::needless_borrow)]
        ProstTable {
            is_index: false,
            index_on_id: 0,
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
            order_key: vec![
                ColumnOrder {
                    order_type: 1, // Ascending
                    index: 1,
                },
                ColumnOrder {
                    order_type: 1, // Ascending
                    index: 3,
                },
            ],
            pk: vec![0],
            dependent_relations: vec![],
            distribution_key: vec![],
            optional_associated_source_id: None,
            appendonly: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            mapping: None,
            properties: HashMap::from([(String::from("ttl"), String::from("300"))]),
            read_pattern_prefix_column: column_count, // 1 column
        }
    }

    #[test]
    fn test_schema_slice_transform() {
        let prost_table = build_table_with_prefix_column_num(1);
        let mut schema_slice_transform = SchemaSliceTransform::new(&prost_table);

        let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];

        let serializer = OrderedRowSerializer::new(order_types);
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
        let output_key = schema_slice_transform.transform(&full_key);
        assert_eq!(
            TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + 1 + mem::size_of::<i64>(),
            output_key.len()
        );
    }

    #[test]
    fn test_multi_slice_transform() {
        let mut multi_slice_transform = MultiSliceTransform::default();
        let last_state = multi_slice_transform.last_slice_transform_state();
        assert!(last_state.is_none());

        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(1);
            let schema_slice_transform = SchemaSliceTransform::new(&prost_table);
            multi_slice_transform.register(1, SliceTransformImpl::Schema(schema_slice_transform));
            let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];

            let serializer = OrderedRowSerializer::new(order_types);
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
            let output_key = multi_slice_transform.transform(&full_key);

            let data_types = vec![DataType::Int64];
            let order_types = vec![OrderType::Ascending];
            let deserializer = OrderedRowDeserializer::new(data_types, order_types);

            let pk_prefix_len = deserializer
                .deserialize_prefix_len_with_column_indices(&row_bytes, 0..=0)
                .unwrap();
            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len,
                output_key.len()
            );

            let last_state = multi_slice_transform.last_slice_transform_state();
            assert!(last_state.is_some());
            assert_eq!(1, last_state.as_ref().unwrap().0);
        }

        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(2);
            let schema_slice_transform = SchemaSliceTransform::new(&prost_table);
            multi_slice_transform.register(2, SliceTransformImpl::Schema(schema_slice_transform));
            let order_types: Vec<OrderType> = vec![OrderType::Ascending, OrderType::Ascending];

            let serializer = OrderedRowSerializer::new(order_types);
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
            let output_key = multi_slice_transform.transform(&full_key);

            let data_types = vec![DataType::Int64, DataType::Varchar];
            let order_types = vec![OrderType::Ascending, OrderType::Ascending];
            let deserializer = OrderedRowDeserializer::new(data_types, order_types);

            let pk_prefix_len = deserializer
                .deserialize_prefix_len_with_column_indices(&row_bytes, 0..=1)
                .unwrap();

            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + pk_prefix_len,
                output_key.len()
            );

            let last_state = multi_slice_transform.last_slice_transform_state();
            assert!(last_state.is_some());
            assert_eq!(2, last_state.as_ref().unwrap().0);
        }

        {
            let full_key_slice_transform = FullKeySliceTransform::default();
            multi_slice_transform
                .register(3, SliceTransformImpl::FullKey(full_key_slice_transform));

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
            let output_key = multi_slice_transform.transform(&full_key);
            assert_eq!(
                TABLE_PREFIX_LEN + VIRTUAL_NODE_SIZE + row_bytes.len(),
                output_key.len()
            );

            let last_state = multi_slice_transform.last_slice_transform_state();
            assert!(last_state.is_some());
            assert_eq!(3, last_state.as_ref().unwrap().0);
        }
    }
}
