// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable).
use std::sync::Arc;

use either::for_both;
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::row::{OwnedRow, RowDeserializer as BasicDeserializer};
use risingwave_common::types::*;
use risingwave_common::util::value_encoding::column_aware_row_encoding::{
    ColumnAwareSerde, Deserializer, Serializer,
};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_common::util::value_encoding::{
    BasicSerde, BasicSerializer, DatumFromProtoExt, EitherSerde, ValueRowDeserializer,
    ValueRowSerdeKind, ValueRowSerializer,
};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::plan_common::DefaultColumnDesc;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

/// Part of `ValueRowSerde` that implements `new` a serde given `column_ids` and `schema`
pub trait ValueRowSerdeNew: Clone {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> Self;
}

/// The compound trait used in `StateTableInner`, implemented by `BasicSerde` and `ColumnAwareSerde`
pub trait ValueRowSerde:
    ValueRowSerializer + ValueRowDeserializer + ValueRowSerdeNew + Sync + Send + 'static
{
    fn kind(&self) -> ValueRowSerdeKind;
}

impl ValueRowSerdeNew for EitherSerde {
    fn new(_value_indices: Arc<[usize]>, _table_columns: Arc<[ColumnDesc]>) -> EitherSerde {
        unreachable!("should construct manually")
    }
}

impl ValueRowSerdeNew for BasicSerde {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> BasicSerde {
        BasicSerde {
            serializer: BasicSerializer {},
            deserializer: BasicDeserializer::new(
                value_indices
                    .iter()
                    .map(|idx| table_columns[*idx].data_type.clone())
                    .collect_vec(),
            ),
        }
    }
}

impl ValueRowSerde for EitherSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        for_both!(&self.0, s => s.kind())
    }
}

impl ValueRowSerde for BasicSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::Basic
    }
}

impl ValueRowSerdeNew for ColumnAwareSerde {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> ColumnAwareSerde {
        let column_ids = value_indices
            .iter()
            .map(|idx| table_columns[*idx].column_id)
            .collect_vec();
        let schema = value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect_vec();
        if cfg!(debug_assertions) {
            let duplicates = column_ids.iter().duplicates().collect_vec();
            if !duplicates.is_empty() {
                panic!("duplicated column ids: {duplicates:?}");
            }
        }

        let partial_columns = value_indices.iter().map(|idx| &table_columns[*idx]);
        let column_with_default = partial_columns.enumerate().filter_map(|(i, c)| {
            if let Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                snapshot_value,
                expr,
            })) = c.generated_or_default_column.clone()
            {
                // TODO: may not panic on error
                let value = if let Some(snapshot_value) = snapshot_value {
                    // If there's a `snapshot_value`, we can use it directly.
                    Datum::from_protobuf(&snapshot_value, &c.data_type)
                        .expect("invalid default value")
                } else {
                    // For backward compatibility, default columns in old tables may not have `snapshot_value`.
                    // In this case, we need to evaluate the expression to get the default value.
                    // It's okay since we previously banned impure expressions in default columns.
                    build_from_prost(&expr.expect("expr should not be none"))
                        .expect("build_from_prost error")
                        .eval_row(&OwnedRow::empty())
                        .now_or_never()
                        .expect("constant expression should not be async")
                        .expect("eval_row failed")
                };
                Some((i, value))
            } else {
                None
            }
        });

        let serializer = Serializer::new(&column_ids, schema.clone());
        let deserializer = Deserializer::new(&column_ids, schema.into(), column_with_default);
        ColumnAwareSerde {
            serializer,
            deserializer,
        }
    }
}

impl ValueRowSerde for ColumnAwareSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::ColumnAware
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::Row;
    use risingwave_common::types::ScalarImpl::*;
    use risingwave_common::util::value_encoding::column_aware_row_encoding;
    use risingwave_common::util::value_encoding::column_aware_row_encoding::try_drop_invalid_columns;

    use super::*;

    #[test]
    fn test_row_encoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let row2 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abd".into()))]);
        let row3 = OwnedRow::new(vec![Some(Int16(6)), Some(Utf8("abc".into()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        let serializer = column_aware_row_encoding::Serializer::new(
            &column_ids,
            [DataType::Int16, DataType::Varchar],
        );
        for row in &rows {
            let row_bytes = serializer.serialize(row);
            array.push(row_bytes);
        }
        let zero_le_bytes = 0_i32.to_le_bytes();
        let one_le_bytes = 1_i32.to_le_bytes();

        assert_eq!(
            array[0],
            [
                0b10000001, // flag mid WW mid BB
                2,
                0,
                0,
                0,                // column nums
                zero_le_bytes[0], // start id 0
                zero_le_bytes[1],
                zero_le_bytes[2],
                zero_le_bytes[3],
                one_le_bytes[0], // start id 1
                one_le_bytes[1],
                one_le_bytes[2],
                one_le_bytes[3],
                0, // offset0: 0
                2, // offset1: 2
                5, // i16: 5
                0,
                3, // str: abc
                0,
                0,
                0,
                b'a',
                b'b',
                b'c'
            ]
        );
    }
    #[test]
    fn test_row_decoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let serializer = column_aware_row_encoding::Serializer::new(
            &column_ids,
            [DataType::Int16, DataType::Varchar],
        );
        let row_bytes = serializer.serialize(row1);
        let data_types = vec![DataType::Int16, DataType::Varchar];
        let deserializer = column_aware_row_encoding::Deserializer::new(
            &column_ids[..],
            Arc::from(data_types.into_boxed_slice()),
            std::iter::empty(),
        );
        let decoded = deserializer.deserialize(&row_bytes[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![Some(Int16(5)), Some(Utf8("abc".into()))]
        );
    }
    #[test]
    fn test_row_hard1() {
        let row = OwnedRow::new(vec![Some(Int16(233)); 20000]);
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..20000),
            Arc::from_iter(
                (0..20000).map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int16)),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), vec![Some(Int16(233)); 20000]);
    }
    #[test]
    fn test_row_hard2() {
        let mut data = vec![Some(Int16(233)); 5000];
        data.extend(vec![None; 5000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 5000]);
        data.extend(vec![None; 5000]);
        let row = OwnedRow::new(data.clone());
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..20000),
            Arc::from_iter(
                (0..10000)
                    .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int16))
                    .chain(
                        (10000..20000)
                            .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Varchar)),
                    ),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }
    #[test]
    fn test_row_hard3() {
        let mut data = vec![Some(Int64(233)); 500000];
        data.extend(vec![None; 250000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 250000]);
        let row = OwnedRow::new(data.clone());
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..1000000),
            Arc::from_iter(
                (0..500000)
                    .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int64))
                    .chain(
                        (500000..1000000)
                            .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Varchar)),
                    ),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }

    #[test]
    fn test_drop_column() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1), ColumnId::new(2)];
        let row1 = OwnedRow::new(vec![
            Some(Int16(5)),
            Some(Utf8("abc".into())),
            Some(Utf8("ABC".into())),
        ]);
        let serializer = column_aware_row_encoding::Serializer::new(
            &column_ids,
            [DataType::Int16, DataType::Varchar, DataType::Varchar],
        );
        let row_bytes = serializer.serialize(row1);

        // no columns is dropped
        assert!(
            try_drop_invalid_columns(&row_bytes, &[0, 1, 2, 3, 4].into_iter().collect()).is_none()
        );

        // column id 1 is dropped
        let row_bytes_dropped =
            try_drop_invalid_columns(&row_bytes, &[0, 2].into_iter().collect()).unwrap();
        let deserializer = column_aware_row_encoding::Deserializer::new(
            &[ColumnId::new(0), ColumnId::new(2)],
            Arc::from(vec![DataType::Int16, DataType::Varchar].into_boxed_slice()),
            std::iter::empty(),
        );
        let decoded = deserializer.deserialize(&row_bytes_dropped[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![Some(Int16(5)), Some(Utf8("ABC".into()))]
        );

        // all columns are dropped
        let row_bytes_all_dropped = try_drop_invalid_columns(&row_bytes, &HashSet::new()).unwrap();
        assert_eq!(row_bytes_all_dropped.len(), 5); // 1 byte flag + 4 bytes for length (0)
        assert_eq!(&row_bytes_all_dropped[1..], [0, 0, 0, 0]);
    }

    #[test]
    fn test_deserialize_partial_columns() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1), ColumnId::new(2)];
        let row1 = OwnedRow::new(vec![
            Some(Int16(5)),
            Some(Utf8("abc".into())),
            Some(Utf8("ABC".into())),
        ]);
        let serializer = column_aware_row_encoding::Serializer::new(
            &column_ids,
            [DataType::Int16, DataType::Varchar, DataType::Varchar],
        );
        let row_bytes = serializer.serialize(row1);

        let deserializer = column_aware_row_encoding::Deserializer::new(
            &[ColumnId::new(2), ColumnId::new(0)],
            Arc::from(vec![DataType::Varchar, DataType::Int16].into_boxed_slice()),
            std::iter::empty(),
        );
        let decoded = deserializer.deserialize(&row_bytes[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![Some(Utf8("ABC".into())), Some(Int16(5))]
        );
    }

    #[test]
    fn test_deserialize_partial_columns_with_default_columns() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1), ColumnId::new(2)];
        let row1 = OwnedRow::new(vec![
            Some(Int16(5)),
            Some(Utf8("abc".into())),
            Some(Utf8("ABC".into())),
        ]);
        let serializer = column_aware_row_encoding::Serializer::new(
            &column_ids,
            [DataType::Int16, DataType::Varchar, DataType::Varchar],
        );
        let row_bytes = serializer.serialize(row1);

        // default column of ColumnId::new(3)
        let default_columns = vec![(1, Some(Utf8("new column".into())))];

        let deserializer = column_aware_row_encoding::Deserializer::new(
            &[ColumnId::new(2), ColumnId::new(3), ColumnId::new(0)],
            Arc::from(
                vec![DataType::Varchar, DataType::Varchar, DataType::Int16].into_boxed_slice(),
            ),
            default_columns.into_iter(),
        );
        let decoded = deserializer.deserialize(&row_bytes[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![
                Some(Utf8("ABC".into())),
                Some(Utf8("new column".into())),
                Some(Int16(5))
            ]
        );
    }

    #[test]
    fn test_row_composite_types() {
        let inner_struct: DataType =
            StructType::new([("f2", DataType::Int32), ("f3", DataType::Boolean)])
                .with_ids([ColumnId::new(11), ColumnId::new(12)])
                .into();
        let list = DataType::List(Box::new(inner_struct.clone()));
        let map = MapType::from_kv(DataType::Varchar, list.clone()).into();
        let outer_struct = StructType::new([("f1", DataType::Int32), ("map", map)])
            .with_ids([ColumnId::new(1), ColumnId::new(2)])
            .into();

        let inner_struct_value = StructValue::new(vec![Some(Int32(6)), Some(Bool(true))]);
        let list_value =
            ListValue::from_datum_iter(&inner_struct, [Some(Struct(inner_struct_value))]);
        let map_value = MapValue::try_from_kv(
            ListValue::from_datum_iter(&DataType::Varchar, [Some(Utf8("key".into()))]),
            ListValue::from_datum_iter(&list, [Some(List(list_value))]),
        )
        .unwrap();
        let outer_struct_value = StructValue::new(vec![Some(Int32(5)), Some(Map(map_value))]);

        let datum = Some(Struct(outer_struct_value));
        let row1 = [datum];

        let column_ids = &[ColumnId::new(0)];
        let data_types = vec![outer_struct];

        let serializer = column_aware_row_encoding::Serializer::new(column_ids, data_types.clone());
        let row_bytes = serializer.serialize(row1.clone());

        assert_eq!(
            row_bytes,
            [
                0b10000001, // flag mid WW mid BB
                1, 0, 0, 0, // column num = 1
                0, 0, 0, 0,  // column id = 0 "outer"
                0,  // offset 0 = 0
                60, // struct length = 60,
                0, 0, 0, 0b10000001, // recursive col-aware flag
                2, 0, 0, 0, // field num = 2
                1, 0, 0, 0, // field id = 1 "f1"
                2, 0, 0, 0, // field id = 2 "map"
                0, // offset 1 = 0
                4, // offset 2 = 4
                5, 0, 0, 0, // "f1": i32 = 5
                1, 0, 0, 0, // map length = 1
                3, 0, 0, 0, // map key string length = 3
                b'k', b'e', b'y', // map key = "key"
                1,    // map value is non NULL
                1, 0, 0, 0, // map value list length = 1
                1, // map value list element (struct) is non NULL
                20, 0, 0, 0,          // struct length = 20
                0b10000001, // recursive col-aware flag
                2, 0, 0, 0, // field num = 2
                11, 0, 0, 0, // field id = 11 "f2"
                12, 0, 0, 0, // field id = 12 "f3"
                0, // offset 11 = 0
                4, // offset 12 = 4
                6, 0, 0, 0, // "f2": i32 = 6
                1, // "f3": bool = true
            ]
        );

        let deserializer = column_aware_row_encoding::Deserializer::new(
            column_ids,
            data_types.into(),
            std::iter::empty(),
        );
        let decoded = deserializer.deserialize(&row_bytes).unwrap();

        assert_eq!(OwnedRow::new(decoded), row1.to_owned_row());
    }
}
