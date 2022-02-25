use std::sync::Arc;

use itertools::Itertools;

use crate::array::{ArrayImpl, DataChunk};
use crate::error::Result;
use crate::types::{serialize_datum_ref_into, DataType};
use crate::util::sort_util::{OrderPair, OrderType};

struct EncodedColumn(pub Vec<Vec<u8>>);

/// This function is used to check whether we can perform encoding on this type.
/// TODO: based on `memcomparable`, we may support more data type in the future.
pub fn is_type_encodable(t: DataType) -> bool {
    matches!(
        t,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Varchar
            | DataType::Char
    )
}

fn encode_array(array: &ArrayImpl, order: &OrderType) -> Result<EncodedColumn> {
    let mut data = Vec::with_capacity(array.len());

    for datum in array.iter() {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(order == &OrderType::Descending);
        serialize_datum_ref_into(&datum, &mut serializer)?;
        data.push(serializer.into_inner());
    }

    Ok(EncodedColumn(data))
}

/// This function is used to accelerate the comparison of tuples. It takes datachunk and
/// user-defined order as input, yield encoded binary string with order preserved for each tuple in
/// the datachunk.
///
/// TODO: specify the order for `NULL`.
pub fn encode_chunk(chunk: &DataChunk, order_pairs: Arc<Vec<OrderPair>>) -> Arc<Vec<Vec<u8>>> {
    let encoded_columns = order_pairs
        .iter()
        .map(|o| encode_array(chunk.column_at(o.column_idx).array_ref(), &o.order_type).unwrap())
        .collect_vec();

    let mut encoded_chunk = vec![vec![]; chunk.capacity()];
    for encoded_column in encoded_columns {
        for (encoded_row, data) in encoded_chunk.iter_mut().zip_eq(encoded_column.0) {
            encoded_row.extend(data);
        }
    }

    Arc::new(encoded_chunk)
}
