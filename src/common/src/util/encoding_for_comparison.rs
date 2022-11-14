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

use itertools::Itertools;

use crate::array::{ArrayImpl, DataChunk, Row};
use crate::error::Result;
use crate::types::{serialize_datum_ref_into, DataType, ScalarRefImpl};
use crate::util::sort_util::{OrderPair, OrderType};

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
    )
}

fn encode_value(value: Option<ScalarRefImpl<'_>>, order: &OrderType) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    serializer.set_reverse(order == &OrderType::Descending);
    serialize_datum_ref_into(&value, &mut serializer)?;
    Ok(serializer.into_inner())
}

fn encode_array(array: &ArrayImpl, order: &OrderType) -> Result<Vec<Vec<u8>>> {
    let mut data = Vec::with_capacity(array.len());
    for datum in array.iter() {
        data.push(encode_value(datum, order)?);
    }
    Ok(data)
}

/// This function is used to accelerate the comparison of tuples. It takes datachunk and
/// user-defined order as input, yield encoded binary string with order preserved for each tuple in
/// the datachunk.
///
/// TODO: specify the order for `NULL`.
pub fn encode_chunk(chunk: &DataChunk, order_pairs: &[OrderPair]) -> Vec<Vec<u8>> {
    let encoded_columns = order_pairs
        .iter()
        .map(|o| encode_array(chunk.column_at(o.column_idx).array_ref(), &o.order_type).unwrap())
        .collect_vec();

    let mut encoded_chunk = vec![vec![]; chunk.capacity()];
    for encoded_column in encoded_columns {
        for (encoded_row, data) in encoded_chunk.iter_mut().zip_eq(encoded_column) {
            encoded_row.extend(data);
        }
    }

    encoded_chunk
}

pub fn encode_row(row: &Row, order_pairs: &[OrderPair]) -> Vec<u8> {
    let mut encoded_row = vec![];
    order_pairs.iter().for_each(|o| {
        let value = row[o.column_idx].as_ref();
        encoded_row
            .extend(encode_value(value.map(|x| x.as_scalar_ref_impl()), &o.order_type).unwrap());
    });
    encoded_row
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::{encode_chunk, encode_row, encode_value};
    use crate::array::{DataChunk, Row};
    use crate::types::{DataType, ScalarImpl};
    use crate::util::sort_util::{OrderPair, OrderType};

    #[test]
    fn test_encode_row() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v10_cloned = v10.clone();
        let v11 = Some(ScalarImpl::Utf8("hello".to_string()));
        let v11_cloned = v11.clone();
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".to_string()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = Row::new(vec![v10, v11, v12]);
        let row2 = Row::new(vec![v20, v21, v22]);
        let order_pairs = vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Descending),
        ];

        let encoded_row1 = encode_row(&row1, &order_pairs);
        let encoded_v10 = encode_value(
            v10_cloned.as_ref().map(|x| x.as_scalar_ref_impl()),
            &OrderType::Ascending,
        )
        .unwrap();
        let encoded_v11 = encode_value(
            v11_cloned.as_ref().map(|x| x.as_scalar_ref_impl()),
            &OrderType::Descending,
        )
        .unwrap();
        let concated_encoded_row1 = encoded_v10
            .into_iter()
            .chain(encoded_v11.into_iter())
            .collect_vec();
        assert_eq!(encoded_row1, concated_encoded_row1);

        let encoded_row2 = encode_row(&row2, &order_pairs);
        assert!(encoded_row1 < encoded_row2);
    }

    #[test]
    fn test_encode_chunk() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v11 = Some(ScalarImpl::Utf8("hello".to_string()));
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".to_string()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = Row::new(vec![v10, v11, v12]);
        let row2 = Row::new(vec![v20, v21, v22]);
        let chunk = DataChunk::from_rows(
            &[row1.clone(), row2.clone()],
            &[DataType::Int32, DataType::Varchar, DataType::Float32],
        );
        let order_pairs = vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Descending),
        ];

        let encoded_row1 = encode_row(&row1, &order_pairs);
        let encoded_row2 = encode_row(&row2, &order_pairs);
        let encoded_chunk = encode_chunk(&chunk, &order_pairs);
        assert_eq!(&encoded_chunk, &[encoded_row1, encoded_row2]);
    }
}
