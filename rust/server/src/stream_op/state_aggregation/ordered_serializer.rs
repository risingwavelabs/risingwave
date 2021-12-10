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
            let mut bytes = vec![];
            for order_pair in &self.order_pairs {
                let mut serializer = memcomparable::Serializer::default();
                let order = order_pair.0;
                let pk_index = order_pair.1;
                serialize_datum_ref_into(&data[pk_index].value_at(row_idx), &mut serializer)
                    .unwrap();
                let mut bytes_datum = serializer.into_inner();
                if order == OrderType::Descending {
                    bytes_datum.iter_mut().for_each(|byte| {
                        *byte = !(*byte);
                    });
                }
                bytes.extend_from_slice(&bytes_datum);
            }
            append_to.push(bytes);
        }
    }
}

pub struct OrderedRowSerializer {
    order_pairs: Vec<OrderPair>,
}

impl OrderedRowSerializer {
    pub fn new(order_pairs: Vec<OrderPair>) -> Self {
        Self { order_pairs }
    }

    pub fn order_based_scehmaed_serialize(&self, data: &Row, append_to: &mut Vec<Vec<u8>>) {
        for (datum, order_type) in data.0.iter().zip(self.order_pairs.iter()) {
            let mut serializer = memcomparable::Serializer::default();
            serialize_datum_into(datum, &mut serializer).unwrap();
            let mut datum_bytes = serializer.into_inner();
            if order_type.0 == OrderType::Descending {
                datum_bytes.iter_mut().for_each(|byte| *byte = !(*byte));
            }
            append_to.push(datum_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use risingwave_common::array::{I16Array, Utf8Array};

    #[test]
    fn test_sort_key_serializer() {
        let orders = vec![(OrderType::Descending, 0), (OrderType::Ascending, 1)];
        let serializer = OrderedArraysSerializer::new(orders);
        let array0 = array_nonnull! { I16Array, [3,2,2] }.into();
        let array1 = array_nonnull! { I16Array, [1,2,3] }.into();
        let input_arrays = vec![&array0, &array1];
        let mut array = vec![];
        serializer.order_based_scehmaed_serialize(&input_arrays, &mut array);
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !3i16.to_be_bytes()[1]);
        assert_eq!(array[1][5], 2i16.to_be_bytes()[1]);
        assert_eq!(array[2][5], 3i16.to_be_bytes()[1]);

        // test negative numbers
        let array0 = array_nonnull! { I16Array, [-32768, -32768, -32767] }.into();
        let array1 = array_nonnull! { I16Array, [-2, -1, -1] }.into();
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
        // option 1 bytes || string 9 bytes
        assert_eq!(
            array[0][..10],
            [
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
        assert_eq!(array[1][10..], [1, b'j', b'm', b'z', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(array[2][10..], [1, b'm', b'j', b'z', 0, 0, 0, 0, 0, 3u8]);
    }
}
