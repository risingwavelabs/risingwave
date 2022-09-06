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

use std::borrow::Cow;
use std::cmp::Reverse;

use itertools::Itertools;

use super::OrderedDatum::{NormalOrder, ReversedOrder};
use super::OrderedRow;
use crate::array::ArrayImpl;
use crate::error::Result;
use crate::row::{Row, RowRef};
use crate::types::{
    deserialize_datum_from, serialize_datum_into, serialize_datum_ref_into, DataType, Datum,
    DatumRef,
};
use crate::util::sort_util::{OrderPair, OrderType};

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

    #[must_use]
    pub fn prefix(&self, len: usize) -> Cow<Self> {
        if len == self.order_types.len() {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(Self {
                order_types: self.order_types[..len].to_vec(),
            })
        }
    }

    pub fn serialize(&self, row: &Row, append_to: &mut Vec<u8>) {
        self.serialize_datums(row.values(), append_to)
    }

    pub fn serialize_ref(&self, row_ref: RowRef<'_>, append_to: &mut Vec<u8>) {
        self.serialize_datum_refs(row_ref.values(), append_to)
    }

    pub fn serialize_datums<'a>(
        &self,
        datums: impl Iterator<Item = &'a Datum>,
        append_to: &mut Vec<u8>,
    ) {
        for (datum, order_type) in datums.zip_eq(self.order_types.iter()) {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serializer.set_reverse(*order_type == OrderType::Descending);
            serialize_datum_into(datum, &mut serializer).unwrap();
            append_to.extend(serializer.into_inner());
        }
    }

    pub fn serialize_datum_refs<'a>(
        &self,
        datum_refs: impl Iterator<Item = DatumRef<'a>>,
        append_to: &mut Vec<u8>,
    ) {
        for (datum, order_type) in datum_refs.zip_eq(self.order_types.iter()) {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serializer.set_reverse(*order_type == OrderType::Descending);
            serialize_datum_ref_into(&datum, &mut serializer).unwrap();
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

    pub fn get_order_types(&self) -> &[OrderType] {
        &self.order_types
    }

    pub fn deserialize_prefix_len_with_column_indices(
        &self,
        key: &[u8],
        column_indices: impl Iterator<Item = usize>,
    ) -> memcomparable::Result<usize> {
        use crate::types::ScalarImpl;
        let mut len: usize = 0;
        for index in column_indices {
            let data_type = &self.data_types[index];
            let order_type = &self.order_types[index];
            let data = &key[len..];
            let mut deserializer = memcomparable::Deserializer::new(data);
            deserializer.set_reverse(*order_type == OrderType::Descending);

            len += ScalarImpl::encoding_data_size(data_type, &mut deserializer)?;
        }

        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::array::{I16Array, Utf8Array};
    use crate::array_nonnull;
    use crate::types::chrono_wrapper::*;
    use crate::types::interval;
    use crate::types::ScalarImpl::{self, *};

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
        assert_eq!(&array[1][3..], [0, 1, b'a', b'b', b'c', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(&array[2][3..], [0, 1, b'a', b'b', b'd', 0, 0, 0, 0, 0, 3u8]);
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
                !(0u8),
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
        assert_eq!(array[1][11..], [0, 1, b'j', b'm', b'z', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(array[2][11..], [0, 1, b'm', b'j', b'z', 0, 0, 0, 0, 0, 3u8]);
    }

    #[test]
    fn test_ordered_row_deserializer() {
        pub use crate::types::decimal::Decimal;
        use crate::types::ScalarImpl::{self, *};
        {
            // basic
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

        {
            // decimal

            let order_types = vec![OrderType::Descending, OrderType::Ascending];
            let serializer = OrderedRowSerializer::new(order_types.clone());
            let schema = vec![DataType::Varchar, DataType::Decimal];

            let row1 = Row(vec![
                Some(Utf8("abc".to_string())),
                Some(ScalarImpl::Decimal(Decimal::NaN)),
            ]);
            let row2 = Row(vec![
                Some(Utf8("abd".to_string())),
                Some(ScalarImpl::Decimal(Decimal::PositiveINF)),
            ]);
            let row3 = Row(vec![
                Some(Utf8("abc".to_string())),
                Some(ScalarImpl::Decimal(Decimal::NegativeINF)),
            ]);
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

    #[test]
    fn test_deserialize_with_column_indices() {
        let order_types = vec![OrderType::Descending, OrderType::Ascending];
        let serializer = OrderedRowSerializer::new(order_types.clone());
        let schema = vec![DataType::Varchar, DataType::Int16];
        let row1 = Row(vec![Some(Utf8("abc".to_string())), Some(Int16(5))]);
        let rows = vec![row1.clone()];
        let deserializer = OrderedRowDeserializer::new(schema, order_types);
        let mut array = vec![];
        for row in &rows {
            let mut row_bytes = vec![];
            serializer.serialize(row, &mut row_bytes);
            array.push(row_bytes);
        }

        {
            let row_0_idx_0_len = deserializer
                .deserialize_prefix_len_with_column_indices(&array[0], 0..=0)
                .unwrap();

            let schema = vec![DataType::Varchar];
            let order_types = vec![OrderType::Descending];
            let deserializer = OrderedRowDeserializer::new(schema, order_types.clone());
            let prefix_slice = &array[0][0..row_0_idx_0_len];
            assert_eq!(
                deserializer.deserialize(prefix_slice).unwrap(),
                OrderedRow::new(Row(vec![Some(Utf8("abc".to_string()))]), &order_types)
            );
        }

        {
            let row_0_idx_1_len = deserializer
                .deserialize_prefix_len_with_column_indices(&array[0], 0..=1)
                .unwrap();

            let order_types = vec![OrderType::Descending, OrderType::Ascending];
            let schema = vec![DataType::Varchar, DataType::Int16];
            let deserializer = OrderedRowDeserializer::new(schema, order_types.clone());
            let prefix_slice = &array[0][0..row_0_idx_1_len];
            assert_eq!(
                deserializer.deserialize(prefix_slice).unwrap(),
                OrderedRow::new(row1, &order_types)
            );
        }
    }

    #[test]
    fn test_encoding_data_size() {
        use std::mem::size_of;

        use crate::types::interval::IntervalUnit;
        use crate::types::OrderedF64;

        let order_types = vec![OrderType::Ascending];
        let serializer = OrderedRowSerializer::new(order_types);

        // test fixed_size
        {
            {
                // test None
                let row = Row(vec![None]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Int16, &mut deserializer).unwrap();
                assert_eq!(1, encoding_data_size);
            }

            {
                // float64
                let row = Row(vec![Some(ScalarImpl::Float64(6.4.into()))]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Float64, &mut deserializer).unwrap();
                let data_size = size_of::<OrderedF64>();
                assert_eq!(8, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // bool
                let row = Row(vec![Some(ScalarImpl::Bool(false))]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Boolean, &mut deserializer).unwrap();

                let data_size = size_of::<u8>();
                assert_eq!(1, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // ts
                let row = Row(vec![Some(ScalarImpl::NaiveDateTime(
                    NaiveDateTimeWrapper::default(),
                ))]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Timestamp, &mut deserializer)
                        .unwrap();
                let data_size = size_of::<NaiveDateTimeWrapper>();
                assert_eq!(12, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // tz
                let row = Row(vec![Some(ScalarImpl::Int64(1111111111))]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Timestampz, &mut deserializer)
                        .unwrap();
                let data_size = size_of::<i64>();
                assert_eq!(8, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // interval
                let row = Row(vec![Some(ScalarImpl::Interval(
                    interval::IntervalUnit::default(),
                ))]);
                let mut row_bytes = vec![];
                serializer.serialize(&row, &mut row_bytes);
                let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                let encoding_data_size =
                    ScalarImpl::encoding_data_size(&DataType::Interval, &mut deserializer).unwrap();
                let data_size = size_of::<IntervalUnit>();
                assert_eq!(16, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }
        }

        {
            // test dynamic_size
            {
                // test decimal
                pub use crate::types::decimal::Decimal;

                {
                    let d = Decimal::from_str("41721.900909090909090909090909").unwrap();
                    let row = Row(vec![Some(ScalarImpl::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serializer.serialize(&row, &mut row_bytes);
                    let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                    let encoding_data_size =
                        ScalarImpl::encoding_data_size(&DataType::Decimal, &mut deserializer)
                            .unwrap();
                    // [nulltag, flag, decimal_chunk, 0]
                    assert_eq!(18, encoding_data_size);
                }

                {
                    let d = Decimal::from_str("1").unwrap();
                    let row = Row(vec![Some(ScalarImpl::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serializer.serialize(&row, &mut row_bytes);
                    let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                    let encoding_data_size =
                        ScalarImpl::encoding_data_size(&DataType::Decimal, &mut deserializer)
                            .unwrap();
                    // [nulltag, flag, decimal_chunk, 0]
                    assert_eq!(4, encoding_data_size);
                }

                {
                    let d = Decimal::from_str("inf").unwrap();
                    let row = Row(vec![Some(ScalarImpl::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serializer.serialize(&row, &mut row_bytes);
                    let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                    let encoding_data_size =
                        ScalarImpl::encoding_data_size(&DataType::Decimal, &mut deserializer)
                            .unwrap();

                    assert_eq!(3, encoding_data_size); // [1, 35, 0]
                }

                {
                    let d = Decimal::from_str("nan").unwrap();
                    let row = Row(vec![Some(ScalarImpl::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serializer.serialize(&row, &mut row_bytes);
                    let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                    let encoding_data_size =
                        ScalarImpl::encoding_data_size(&DataType::Decimal, &mut deserializer)
                            .unwrap();
                    assert_eq!(3, encoding_data_size); // [1, 6, 0]
                }

                {
                    // TODO(test list / struct)
                }

                {
                    // test varchar
                    let varchar = "abcdefghijklmn";
                    let row = Row(vec![Some(Utf8(varchar.to_string()))]);
                    let mut row_bytes = vec![];
                    serializer.serialize(&row, &mut row_bytes);
                    let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                    let encoding_data_size =
                        ScalarImpl::encoding_data_size(&DataType::Varchar, &mut deserializer)
                            .unwrap();
                    // [1, 1, 97, 98, 99, 100, 101, 102, 103, 104, 9, 105, 106, 107, 108, 109, 110,
                    // 0, 0, 6]
                    assert_eq!(6 + varchar.len(), encoding_data_size);
                }

                {
                    {
                        // test varchar Descending
                        let order_types = vec![OrderType::Descending];
                        let serializer = OrderedRowSerializer::new(order_types);
                        let varchar = "abcdefghijklmnopq";
                        let row = Row(vec![Some(Utf8(varchar.to_string()))]);
                        let mut row_bytes = vec![];
                        serializer.serialize(&row, &mut row_bytes);
                        let mut deserializer = memcomparable::Deserializer::new(&row_bytes[..]);
                        deserializer.set_reverse(true);
                        let encoding_data_size =
                            ScalarImpl::encoding_data_size(&DataType::Varchar, &mut deserializer)
                                .unwrap();

                        // [254, 254, 158, 157, 156, 155, 154, 153, 152, 151, 246, 150, 149, 148,
                        // 147, 146, 145, 144, 143, 246, 142, 255, 255, 255, 255, 255, 255, 255,
                        // 254]
                        assert_eq!(12 + varchar.len(), encoding_data_size);
                    }
                }
            }
        }
    }
}
