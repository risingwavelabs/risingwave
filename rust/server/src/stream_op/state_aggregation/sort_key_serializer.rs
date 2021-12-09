use risingwave_common::array::Row;
use risingwave_common::util::sort_util::OrderType;

/// We can use memcomparable serialization to serialize data
/// and flip the bits if the order of that datum is descending.
/// As this is normally used for sorted keys, deserialization is
/// not implemented for now.
pub trait OrderedSchemaedSerializable: Send + Sync + 'static {
    type Input: Clone + Send + Sync + 'static;

    /// Serialize a row to `Vec<u8>`.
    fn order_based_scehmaed_serialize(&self, data: &Self::Input) -> Vec<u8>;
}

/// The number of `datum` in the row should be the same as
/// the length of `orders`.
pub struct SortedKeySerializer {
    orders: Vec<OrderType>,
}

impl SortedKeySerializer {
    pub fn new(orders: Vec<OrderType>) -> Self {
        Self { orders }
    }
}

impl OrderedSchemaedSerializable for SortedKeySerializer {
    type Input = Row;

    fn order_based_scehmaed_serialize(&self, data: &Row) -> Vec<u8> {
        data.serialize_with_order(&self.orders).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use risingwave_common::types::ScalarImpl;

    #[test]
    fn test_sort_key_serializer() {
        let orders = vec![OrderType::Descending, OrderType::Ascending];
        let serializer = SortedKeySerializer::new(orders);
        let row1 = Row(vec![Some(ScalarImpl::Int16(3)), Some(ScalarImpl::Int16(1))]);
        let row2 = Row(vec![Some(ScalarImpl::Int16(2)), Some(ScalarImpl::Int16(2))]);
        let row3 = Row(vec![Some(ScalarImpl::Int16(2)), Some(ScalarImpl::Int16(3))]);
        let bytes1 = serializer.order_based_scehmaed_serialize(&row1);
        let bytes2 = serializer.order_based_scehmaed_serialize(&row2);
        let bytes3 = serializer.order_based_scehmaed_serialize(&row3);
        let mut array = vec![bytes1, bytes2, bytes3];
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !3i16.to_be_bytes()[1]);
        assert_eq!(array[1][5], 2i16.to_be_bytes()[1]);
        assert_eq!(array[2][5], 3i16.to_be_bytes()[1]);

        // test negative numbers
        let row1 = Row(vec![
            Some(ScalarImpl::Int16(-32768)),
            Some(ScalarImpl::Int16(-2)),
        ]);
        let row2 = Row(vec![
            Some(ScalarImpl::Int16(-32768)),
            Some(ScalarImpl::Int16(-1)),
        ]);
        let row3 = Row(vec![
            Some(ScalarImpl::Int16(-32767)),
            Some(ScalarImpl::Int16(-1)),
        ]);
        let bytes1 = serializer.order_based_scehmaed_serialize(&row1);
        let bytes2 = serializer.order_based_scehmaed_serialize(&row2);
        let bytes3 = serializer.order_based_scehmaed_serialize(&row3);
        let mut array = vec![bytes1, bytes2, bytes3];
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !(-32767i16).to_be_bytes()[1]);
        assert_eq!(array[1][5], (-2i16).to_be_bytes()[1]);
        assert_eq!(array[2][5], (-1i16).to_be_bytes()[1]);

        // test variable-size types, i.e. string
        let row1 = Row(vec![
            Some(ScalarImpl::Utf8("ab".to_string())),
            Some(ScalarImpl::Utf8("jmz".to_string())),
        ]);
        let row2 = Row(vec![
            Some(ScalarImpl::Utf8("ab".to_string())),
            Some(ScalarImpl::Utf8("mjz".to_string())),
        ]);
        let row3 = Row(vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            Some(ScalarImpl::Utf8("mzj".to_string())),
        ]);
        let bytes1 = serializer.order_based_scehmaed_serialize(&row1);
        let bytes2 = serializer.order_based_scehmaed_serialize(&row2);
        let bytes3 = serializer.order_based_scehmaed_serialize(&row3);
        let mut array = vec![bytes1, bytes2, bytes3];
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
