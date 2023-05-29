// Copyright 2023 RisingWave Labs
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
use itertools::Itertools;
use risingwave_common::catalog::ColumnId;
use risingwave_common::row::RowDeserializer as BasicDeserializer;
use risingwave_common::types::*;
use risingwave_common::util::value_encoding::column_aware_row_encoding::{
    ColumnAwareSerde, Deserializer, Serializer,
};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_common::util::value_encoding::{
    BasicSerde, BasicSerializer, EitherSerde, ValueRowDeserializer, ValueRowSerdeKind,
    ValueRowSerializer,
};

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

/// Part of `ValueRowSerde` that implements `new` a serde given `column_ids` and `schema`
pub trait ValueRowSerdeNew: Clone {
    fn new(column_ids: &[ColumnId], schema: Arc<[DataType]>) -> Self;
    fn set_default_columns(&mut self, _default_columns: impl Iterator<Item = (usize, Datum)>) {
        unimplemented!("set_default_columns should only be called on ColumnAwareSerde")
    }
}

/// The compound trait used in `StateTableInner`, implemented by `BasicSerde` and `ColumnAwareSerde`
pub trait ValueRowSerde:
    ValueRowSerializer + ValueRowDeserializer + ValueRowSerdeNew + Sync + Send + 'static
{
    fn kind(&self) -> ValueRowSerdeKind;
}

impl ValueRowSerdeNew for EitherSerde {
    fn new(_column_ids: &[ColumnId], _schema: Arc<[DataType]>) -> EitherSerde {
        unreachable!("should construct manually")
    }

    fn set_default_columns(&mut self, _default_columns: impl Iterator<Item = (usize, Datum)>) {
        unimplemented!("set_default_columns should only be called on ColumnAwareSerde")
    }
}

impl ValueRowSerdeNew for BasicSerde {
    fn new(_column_ids: &[ColumnId], schema: Arc<[DataType]>) -> BasicSerde {
        BasicSerde {
            serializer: BasicSerializer {},
            deserializer: BasicDeserializer::new(schema.as_ref().to_owned()),
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
    fn new(column_ids: &[ColumnId], schema: Arc<[DataType]>) -> ColumnAwareSerde {
        if cfg!(debug_assertions) {
            let duplicates = column_ids.iter().duplicates().collect_vec();
            if !duplicates.is_empty() {
                panic!("duplicated column ids: {duplicates:?}");
            }
        }

        let serializer = Serializer::new(column_ids);
        let deserializer = Deserializer::new(column_ids, schema);
        ColumnAwareSerde {
            serializer,
            deserializer,
        }
    }

    fn set_default_columns(&mut self, default_columns: impl Iterator<Item = (usize, Datum)>) {
        self.deserializer.set_default_column_values(default_columns)
    }
}

impl ValueRowSerde for ColumnAwareSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::ColumnAware
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl::*;
    use risingwave_common::util::value_encoding::column_aware_row_encoding;

    use super::*;

    #[test]
    fn test_row_encoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let row2 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abd".into()))]);
        let row3 = OwnedRow::new(vec![Some(Int16(6)), Some(Utf8("abc".into()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        let serializer = column_aware_row_encoding::Serializer::new(&column_ids);
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
        let serializer = column_aware_row_encoding::Serializer::new(&column_ids);
        let row_bytes = serializer.serialize(row1);
        let data_types = vec![DataType::Int16, DataType::Varchar];
        let deserializer = column_aware_row_encoding::Deserializer::new(
            &column_ids[..],
            Arc::from(data_types.into_boxed_slice()),
        );
        let decoded = deserializer.deserialize(&row_bytes[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![Some(Int16(5)), Some(Utf8("abc".into()))]
        );
    }
    #[test]
    fn test_row_hard1() {
        let column_ids = (0..20000).map(ColumnId::new).collect_vec();
        let row = OwnedRow::new(vec![Some(Int16(233)); 20000]);
        let data_types = vec![DataType::Int16; 20000];
        let serde = ColumnAwareSerde::new(&column_ids, Arc::from(data_types.into_boxed_slice()));
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), vec![Some(Int16(233)); 20000]);
    }
    #[test]
    fn test_row_hard2() {
        let column_ids = (0..20000).map(ColumnId::new).collect_vec();
        let mut data = vec![Some(Int16(233)); 5000];
        data.extend(vec![None; 5000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 5000]);
        data.extend(vec![None; 5000]);
        let row = OwnedRow::new(data.clone());
        let mut data_types = vec![DataType::Int16; 10000];
        data_types.extend(vec![DataType::Varchar; 10000]);
        let serde = ColumnAwareSerde::new(&column_ids, Arc::from(data_types.into_boxed_slice()));
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }
    #[test]
    fn test_row_hard3() {
        let column_ids = (0..1000000).map(ColumnId::new).collect_vec();
        let mut data = vec![Some(Int64(233)); 500000];
        data.extend(vec![None; 250000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 250000]);
        let row = OwnedRow::new(data.clone());
        let mut data_types = vec![DataType::Int64; 500000];
        data_types.extend(vec![DataType::Varchar; 500000]);
        let serde = ColumnAwareSerde::new(&column_ids, Arc::from(data_types.into_boxed_slice()));
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }
}
