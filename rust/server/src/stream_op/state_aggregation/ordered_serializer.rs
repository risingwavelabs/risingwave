use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::types::{serialize_datum_into, serialize_datum_ref_into};
use risingwave_common::util::sort_util::OrderType;

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
            for (datum, &(order, _)) in row.0.iter().zip(self.order_pairs.iter()) {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                serializer.set_reverse(order == OrderType::Descending);
                serialize_datum_into(datum, &mut serializer).unwrap();
                row_bytes.extend(serializer.into_inner());
            }
            append_to.push(row_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I16Array, Utf8Array};
    use risingwave_common::types::ScalarImpl::{Int16, Utf8};

    use super::*;

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
}
