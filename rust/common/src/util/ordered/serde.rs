use std::cmp::Reverse;

use bytes::BufMut;
use itertools::Itertools;

use super::OrderedDatum::{NormalOrder, ReversedOrder};
use super::OrderedRow;
use crate::array::{ArrayImpl, Row};
use crate::error::Result;
use crate::types::{
    deserialize_datum_from, serialize_datum_into, serialize_datum_ref_into, DataType, Datum,
};
use crate::util::sort_util::OrderType;

type OrderPair = (OrderType, usize);

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

    pub fn order_based_scehmaed_serialize(
        &self,
        data: &[&ArrayImpl],
        append_to: &mut Vec<Vec<u8>>,
    ) {
        for row_idx in 0..data[0].len() {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            for order_pair in &self.order_pairs {
                let order = order_pair.0;
                let pk_index = order_pair.1;
                serializer.set_reverse(order == OrderType::Descending);
                serialize_datum_ref_into(&data[pk_index].value_at(row_idx), &mut serializer)
                    .unwrap();
            }
            append_to.push(serializer.into_inner());
        }
    }
}

pub struct OrderedRowsSerializer {
    order_pairs: Vec<OrderPair>,
}

impl OrderedRowsSerializer {
    pub fn new(order_pairs: Vec<OrderPair>) -> Self {
        Self { order_pairs }
    }

    pub fn order_based_scehmaed_serialize(&self, data: &[&Row], append_to: &mut Vec<Vec<u8>>) {
        for row in data {
            let mut row_bytes = vec![];
            for (order, pk_index) in &self.order_pairs {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                serializer.set_reverse(*order == OrderType::Descending);
                serialize_datum_into(&row.0[*pk_index], &mut serializer).unwrap();
                row_bytes.extend(serializer.into_inner());
            }
            append_to.push(row_bytes);
        }
    }
}

/// Deserializer of the `Row`.
#[derive(Clone)]
pub struct OrderedRowDeserializer {
    data_type_kinds: Vec<DataType>,
    order_types: Vec<OrderType>,
}

impl OrderedRowDeserializer {
    pub fn new(schema: Vec<DataType>, order_types: Vec<OrderType>) -> Self {
        assert_eq!(schema.len(), order_types.len());
        Self {
            data_type_kinds: schema,
            order_types,
        }
    }

    pub fn deserialize(&self, data: &[u8]) -> Result<OrderedRow> {
        let mut values = Vec::with_capacity(self.data_type_kinds.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for (data_type, order_type) in self.data_type_kinds.iter().zip_eq(self.order_types.iter()) {
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

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowsSerializer) -> Result<Vec<u8>> {
    let mut result = vec![];
    serializer.order_based_scehmaed_serialize(&[pk], &mut result);
    Ok(std::mem::take(&mut result[0]))
}

pub fn serialize_cell_idx(cell_idx: u32) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(4);
    buf.put_u32_le(cell_idx);
    debug_assert_eq!(buf.len(), 4);
    Ok(buf)
}

pub fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    serialize_datum_into(cell, &mut serializer)?;
    Ok(serializer.into_inner())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::array::{I16Array, Utf8Array};
    use crate::array_nonnull;
    use crate::types::ScalarImpl::{Int16, Utf8};

    #[test]
    fn test_ordered_row_serializer() {
        let orders = vec![(OrderType::Descending, 0), (OrderType::Ascending, 1)];
        let serializer = OrderedRowsSerializer::new(orders);
        let row1 = Row(vec![Some(Int16(5)), Some(Utf8("abc".to_string()))]);
        let row2 = Row(vec![Some(Int16(5)), Some(Utf8("abd".to_string()))]);
        let row3 = Row(vec![Some(Int16(6)), Some(Utf8("abc".to_string()))]);
        let mut array = vec![];
        serializer.order_based_scehmaed_serialize(&[&row1, &row2, &row3], &mut array);
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !6i16.to_be_bytes()[1]);
        assert_eq!(&array[1][3..], [1, 1, b'a', b'b', b'c', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(&array[2][3..], [1, 1, b'a', b'b', b'd', 0, 0, 0, 0, 0, 3u8]);
    }

    #[test]
    fn test_ordered_arrays_serializer() {
        let orders = vec![(OrderType::Descending, 0), (OrderType::Ascending, 1)];
        let serializer = OrderedArraysSerializer::new(orders);
        let array0 = array_nonnull! { I16Array, [3i16,2,2] }.into();
        let array1 = array_nonnull! { I16Array, [1i16,2,3] }.into();
        let input_arrays = vec![&array0, &array1];
        let mut array = vec![];
        serializer.order_based_scehmaed_serialize(&input_arrays, &mut array);
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
        serializer.order_based_scehmaed_serialize(&input_arrays, &mut array);
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
        serializer.order_based_scehmaed_serialize(&input_arrays, &mut array);
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
        let pk_indices = vec![0, 1];
        let order_pairs = order_types
            .clone()
            .into_iter()
            .zip_eq(pk_indices.into_iter())
            .collect::<Vec<_>>();
        let serializer = OrderedRowsSerializer::new(order_pairs);
        let schema = vec![DataType::Varchar, DataType::Int16];
        let row1 = Row(vec![Some(Utf8("abc".to_string())), Some(Int16(5))]);
        let row2 = Row(vec![Some(Utf8("abd".to_string())), Some(Int16(5))]);
        let row3 = Row(vec![Some(Utf8("abc".to_string())), Some(Int16(6))]);
        let deserializer = OrderedRowDeserializer::new(schema, order_types.clone());
        let mut array = vec![];
        serializer.order_based_scehmaed_serialize(&[&row1, &row2, &row3], &mut array);
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
