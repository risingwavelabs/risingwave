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
//
use std::cmp::Reverse;

use bytes::Buf;
use itertools::Itertools;
use memcomparable::from_slice;
use serde::Deserialize;

use super::OrderedDatum::{NormalOrder, ReversedOrder};
use super::OrderedRow;
use crate::array::{ArrayImpl, Row, RowRef};
use crate::catalog::ColumnId;
use crate::error::{ErrorCode, Result, RwError};
use crate::types::{
    deserialize_datum_from, serialize_datum_into, serialize_datum_ref_into, DataType, Datum,
    Decimal, ScalarImpl,
};
use crate::util::sort_util::{OrderPair, OrderType};

/// The special `cell_id` reserved for a whole null row is `i32::MIN`.
pub const NULL_ROW_SPECIAL_CELL_ID: ColumnId = ColumnId::new(i32::MIN);

/// We can use memcomparable serialization to serialize data
/// and flip the bits if the order of that datum is descending.
/// As this is normally used for sorted keys, deserialization is
/// not implemented for now.
/// The number of `datum` in the row should be the same as
/// the length of `orders`.
pub struct OrderedArraysSerializer {
    order_pairs: Vec<OrderPair>,
}

impl OrderedArraysSerializer {
    pub fn new(order_pairs: Vec<OrderPair>) -> Self {
        Self { order_pairs }
    }

    pub fn serialize(&self, data: &[&ArrayImpl], append_to: &mut Vec<Vec<u8>>) {
        for row_idx in 0..data[0].len() {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            for order_pair in &self.order_pairs {
                let order = order_pair.order_type;
                let pk_index = order_pair.column_idx;
                serializer.set_reverse(order == OrderType::Descending);
                serialize_datum_ref_into(&data[pk_index].value_at(row_idx), &mut serializer)
                    .unwrap();
            }
            append_to.push(serializer.into_inner());
        }
    }
}

/// `OrderedRowSerializer` expects that the input row contains exactly the values needed to be
/// serialized, not more and not less. This is because `Row` always needs to be constructed from
/// chunk manually.
#[derive(Clone)]
pub struct OrderedRowSerializer {
    order_types: Vec<OrderType>,
}

impl OrderedRowSerializer {
    pub fn new(order_types: Vec<OrderType>) -> Self {
        Self { order_types }
    }

    pub fn serialize(&self, row: &Row, append_to: &mut Vec<u8>) {
        for (datum, order_type) in row.0.iter().zip_eq(self.order_types.iter()) {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serializer.set_reverse(*order_type == OrderType::Descending);
            serialize_datum_into(datum, &mut serializer).unwrap();
            append_to.extend(serializer.into_inner());
        }
    }

    pub fn serialize_row_ref(&self, row: &RowRef<'_>, append_to: &mut Vec<u8>) {
        for (datum, order_type) in row.0.iter().zip_eq(self.order_types.iter()) {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serializer.set_reverse(*order_type == OrderType::Descending);
            serialize_datum_ref_into(datum, &mut serializer).unwrap();
            append_to.extend(serializer.into_inner());
        }
    }
}

/// Deserializer of the `Row`.
#[derive(Clone)]
pub struct OrderedRowDeserializer {
    data_types: Vec<DataType>,
    order_types: Vec<OrderType>,
}

impl OrderedRowDeserializer {
    pub fn new(schema: Vec<DataType>, order_types: Vec<OrderType>) -> Self {
        assert_eq!(schema.len(), order_types.len());
        Self {
            data_types: schema,
            order_types,
        }
    }

    pub fn deserialize(&self, data: &[u8]) -> Result<OrderedRow> {
        let mut values = Vec::with_capacity(self.data_types.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for (data_type, order_type) in self.data_types.iter().zip_eq(self.order_types.iter()) {
            deserializer.set_reverse(*order_type == OrderType::Descending);
            let datum = deserialize_datum_from(data_type, &mut deserializer)?;
            let datum = match order_type {
                OrderType::Ascending => NormalOrder(datum),
                OrderType::Descending => ReversedOrder(Reverse(datum)),
            };
            values.push(datum);
        }
        Ok(OrderedRow(values))
    }
}

type KeyBytes = Vec<u8>;
type ValueBytes = Vec<u8>;

/// Serialize a row of data using cell-based serialization, and return corresponding vector of key
/// and value. If all data of this row are null, there will be one cell of column id `-1` to
/// represent a row of all null values.
pub fn serialize_pk_and_row(
    pk_buf: &[u8],
    row: &Option<Row>,
    column_ids: &[ColumnId],
) -> Result<Vec<(KeyBytes, Option<ValueBytes>)>> {
    if let Some(values) = row.as_ref() {
        assert_eq!(values.0.len(), column_ids.len());
    }
    let mut result = vec![];
    let mut all_null = true;
    for (index, column_id) in column_ids.iter().enumerate() {
        let key = [pk_buf, serialize_column_id(column_id)?.as_slice()].concat();
        match row {
            Some(values) => match &values[index] {
                None => {
                    // This is when the datum is null. If all the datum in a row is null,
                    // we serialize this null row specially by only using one cell encoding.
                }
                datum => {
                    all_null = false;
                    let value = serialize_cell(datum)?;
                    result.push((key, Some(value)));
                }
            },
            None => {
                // A `None` of row means deleting that row, while the a `None` of datum represents a
                // null.
                all_null = false;
                result.push((key, None));
            }
        }
    }
    if all_null {
        // Here we use a special column id -1 to represent a row consisting of all null values.
        // `MViewTable` has a `get` interface which accepts a cell id. A null row in this case
        // would return null datum as it has only a single cell with column id == -1 and `get`
        // gets nothing.
        let key = [
            pk_buf,
            serialize_column_id(&NULL_ROW_SPECIAL_CELL_ID)?.as_slice(),
        ]
        .concat();
        let value = serialize_cell(&None)?;
        result.push((key, Some(value)));
    }
    Ok(result)
}

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Result<Vec<u8>> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    Ok(result)
}

pub fn serialize_column_id(column_id: &ColumnId) -> Result<Vec<u8>> {
    use serde::Serialize;
    let mut serializer = memcomparable::Serializer::new(vec![]);
    column_id.get_id().serialize(&mut serializer)?;
    let buf = serializer.into_inner();
    debug_assert_eq!(buf.len(), 4);
    Ok(buf)
}

/// Serialize datum into cell bytes (Not order guarantee, used in value encoding).
pub fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    if let Some(ScalarImpl::Decimal(decimal)) = cell {
        return serialize_decimal(decimal);
    }
    serialize_datum_into(cell, &mut serializer)?;
    Ok(serializer.into_inner())
}

fn serialize_decimal(decimal: &Decimal) -> Result<Vec<u8>> {
    let (mut mantissa, mut scale) = decimal.mantissa_scale_for_serialization();
    if mantissa < 0 {
        mantissa = -mantissa;
        // We use the most significant bit of `scale` to denote whether decimal is negative or not.
        scale += 1 << 7;
    }
    let mut byte_array = vec![1, scale];
    while mantissa != 0 {
        let byte = (mantissa % 100) as u8;
        byte_array.push(byte);
        mantissa /= 100;
    }
    Ok(byte_array)
}

/// Deserialize cell bytes into datum (Not order guarantee, used in value decoding).
pub fn deserialize_cell(
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ty: &DataType,
) -> Result<Datum> {
    match ty {
        &DataType::Decimal => deserialize_decimal(deserializer),
        _ => Ok(deserialize_datum_from(ty, deserializer)?),
    }
}

pub fn deserialize_column_id(bytes: &[u8]) -> Result<ColumnId> {
    assert_eq!(bytes.len(), 4);
    let column_id = from_slice::<i32>(bytes)?;
    Ok(column_id.into())
}

fn deserialize_decimal(deserializer: &mut memcomparable::Deserializer<impl Buf>) -> Result<Datum> {
    // None denotes NULL which is a valid value while Err means invalid encoding.
    let null_tag = u8::deserialize(&mut *deserializer)?;
    match null_tag {
        0 => {
            return Ok(None);
        }
        1 => {}
        _ => {
            return Err(RwError::from(ErrorCode::InternalError(format!(
                "Invalid null tag: {}",
                null_tag
            ))));
        }
    }
    let bytes = deserializer.read_decimal()?;
    let mut scale = bytes[0];
    let neg = if (scale & 1 << 7) > 0 {
        scale &= !(1 << 7);
        true
    } else {
        false
    };
    let mut mantissa: i128 = 0;
    for (exp, byte) in bytes.iter().skip(1).enumerate() {
        mantissa += (*byte as i128) * 100i128.pow(exp as u32);
    }
    if neg {
        mantissa = -mantissa;
    }
    Ok(Some(ScalarImpl::Decimal(Decimal::from_i128_with_scale(
        mantissa,
        scale as u32,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{I16Array, Utf8Array};
    use crate::array_nonnull;
    use crate::types::ScalarImpl::{Int16, Utf8};

    #[test]
    fn test_ordered_row_serializer() {
        let orders = vec![OrderType::Descending, OrderType::Ascending];
        let serializer = OrderedRowSerializer::new(orders);
        let row1 = Row(vec![Some(Int16(5)), Some(Utf8("abc".to_string()))]);
        let row2 = Row(vec![Some(Int16(5)), Some(Utf8("abd".to_string()))]);
        let row3 = Row(vec![Some(Int16(6)), Some(Utf8("abc".to_string()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        for row in &rows {
            let mut row_bytes = vec![];
            serializer.serialize(row, &mut row_bytes);
            array.push(row_bytes);
        }
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !6i16.to_be_bytes()[1]);
        assert_eq!(&array[1][3..], [1, 1, b'a', b'b', b'c', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(&array[2][3..], [1, 1, b'a', b'b', b'd', 0, 0, 0, 0, 0, 3u8]);
    }

    #[test]
    fn test_ordered_arrays_serializer() {
        let orders = vec![
            OrderPair::new(0, OrderType::Descending),
            OrderPair::new(1, OrderType::Ascending),
        ];
        let serializer = OrderedArraysSerializer::new(orders);
        let array0 = array_nonnull! { I16Array, [3i16,2,2] }.into();
        let array1 = array_nonnull! { I16Array, [1i16,2,3] }.into();
        let input_arrays = vec![&array0, &array1];
        let mut array = vec![];
        serializer.serialize(&input_arrays, &mut array);
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !3i16.to_be_bytes()[1]);
        assert_eq!(array[1][5], 2i16.to_be_bytes()[1]);
        assert_eq!(array[2][5], 3i16.to_be_bytes()[1]);

        // test negative numbers
        let array0 = array_nonnull! { I16Array, [-32768i16, -32768, -32767] }.into();
        let array1 = array_nonnull! { I16Array, [-2i16, -1, -1] }.into();
        let input_arrays = vec![&array0, &array1];
        let mut array = vec![];
        serializer.serialize(&input_arrays, &mut array);
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !(-32767i16).to_be_bytes()[1]);
        assert_eq!(array[1][5], (-2i16).to_be_bytes()[1]);
        assert_eq!(array[2][5], (-1i16).to_be_bytes()[1]);

        // test variable-size types, i.e. string
        let array0 = array_nonnull! { Utf8Array, ["ab", "ab", "abc"] }.into();
        let array1 = array_nonnull! { Utf8Array, ["jmz", "mjz", "mzj"] }.into();
        let input_arrays = vec![&array0, &array1];
        let mut array = vec![];
        serializer.serialize(&input_arrays, &mut array);
        array.sort();
        // option 1 bytes || string 10 bytes
        assert_eq!(
            array[0][..11],
            [
                !(1u8),
                !(1u8),
                !(b'a'),
                !(b'b'),
                !(b'c'),
                255,
                255,
                255,
                255,
                255,
                !(3u8)
            ]
        );
        assert_eq!(array[1][11..], [1, 1, b'j', b'm', b'z', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(array[2][11..], [1, 1, b'm', b'j', b'z', 0, 0, 0, 0, 0, 3u8]);
    }

    #[test]
    fn test_ordered_row_deserializer() {
        let order_types = vec![OrderType::Descending, OrderType::Ascending];
        let serializer = OrderedRowSerializer::new(order_types.clone());
        let schema = vec![DataType::Varchar, DataType::Int16];
        let row1 = Row(vec![Some(Utf8("abc".to_string())), Some(Int16(5))]);
        let row2 = Row(vec![Some(Utf8("abd".to_string())), Some(Int16(5))]);
        let row3 = Row(vec![Some(Utf8("abc".to_string())), Some(Int16(6))]);
        let rows = vec![row1.clone(), row2.clone(), row3.clone()];
        let deserializer = OrderedRowDeserializer::new(schema, order_types.clone());
        let mut array = vec![];
        for row in &rows {
            let mut row_bytes = vec![];
            serializer.serialize(row, &mut row_bytes);
            array.push(row_bytes);
        }
        assert_eq!(
            deserializer.deserialize(&array[0]).unwrap(),
            OrderedRow::new(row1, &order_types)
        );
        assert_eq!(
            deserializer.deserialize(&array[1]).unwrap(),
            OrderedRow::new(row2, &order_types)
        );
        assert_eq!(
            deserializer.deserialize(&array[2]).unwrap(),
            OrderedRow::new(row3, &order_types)
        );
    }
}
