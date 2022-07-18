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

fn encode_value(value: Option<ScalarRefImpl>, order: &OrderType) -> Result<Vec<u8>> {
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

// TODO(rc): add tests
