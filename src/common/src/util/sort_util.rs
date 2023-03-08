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

use std::cmp::{Ord, Ordering};
use std::sync::Arc;

use risingwave_pb::common::{PbColumnOrder, PbDirection, PbOrderType};

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub enum OrderType {
    Ascending,
    Descending,
}

impl OrderType {
    // TODO(rc): from `PbOrderType`
    pub fn from_protobuf(order_type: &PbDirection) -> OrderType {
        match order_type {
            PbDirection::Ascending => OrderType::Ascending,
            PbDirection::Descending => OrderType::Descending,
            PbDirection::Unspecified => unreachable!(),
        }
    }

    // TODO(rc): to `PbOrderType`
    pub fn to_protobuf(self) -> PbDirection {
        match self {
            OrderType::Ascending => PbDirection::Ascending,
            OrderType::Descending => PbDirection::Descending,
        }
    }
}

/// Column index with an order type (ASC or DESC). Used to represent a sort key (`Vec<OrderPair>`).
///
/// Corresponds to protobuf [`PbColumnOrder`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderPair {
    pub column_idx: usize,
    pub order_type: OrderType,
}

impl OrderPair {
    pub fn new(column_idx: usize, order_type: OrderType) -> Self {
        Self {
            column_idx,
            order_type,
        }
    }

    pub fn from_protobuf(column_order: &PbColumnOrder) -> Self {
        OrderPair {
            column_idx: column_order.column_index as _,
            order_type: OrderType::from_protobuf(
                &column_order.get_order_type().unwrap().direction(),
            ),
        }
    }

    pub fn to_protobuf(&self) -> PbColumnOrder {
        PbColumnOrder {
            column_index: self.column_idx as _,
            order_type: Some(PbOrderType {
                direction: self.order_type.to_protobuf() as _,
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HeapElem {
    pub order_pairs: Arc<Vec<OrderPair>>,
    pub chunk: DataChunk,
    pub chunk_idx: usize,
    pub elem_idx: usize,
    /// DataChunk can be encoded to accelerate the comparison.
    /// Use `risingwave_common::util::encoding_for_comparison::encode_chunk`
    /// to perform encoding, otherwise the comparison will be performed
    /// column by column.
    pub encoded_chunk: Option<Arc<Vec<Vec<u8>>>>,
}

impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = if let (Some(lhs_encoded_chunk), Some(rhs_encoded_chunk)) =
            (self.encoded_chunk.as_ref(), other.encoded_chunk.as_ref())
        {
            lhs_encoded_chunk[self.elem_idx]
                .as_slice()
                .cmp(rhs_encoded_chunk[other.elem_idx].as_slice())
        } else {
            compare_rows_in_chunk(
                &self.chunk,
                self.elem_idx,
                &other.chunk,
                other.elem_idx,
                self.order_pairs.as_ref(),
            )
            .unwrap()
        };
        ord.reverse()
    }
}

impl PartialOrd for HeapElem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapElem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapElem {}

fn compare_values<T>(lhs: Option<&T>, rhs: Option<&T>, order_type: &OrderType) -> Ordering
where
    T: Ord,
{
    let ord = match (lhs, rhs) {
        (Some(l), Some(r)) => l.cmp(r),
        (None, None) => Ordering::Equal,
        // TODO(yuchao): `null first` / `null last` is not supported yet.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
    };
    if *order_type == OrderType::Descending {
        ord.reverse()
    } else {
        ord
    }
}

fn compare_values_in_array<'a, T>(
    lhs_array: &'a T,
    lhs_idx: usize,
    rhs_array: &'a T,
    rhs_idx: usize,
    order_type: &'a OrderType,
) -> Ordering
where
    T: Array,
    <T as Array>::RefItem<'a>: Ord,
{
    compare_values(
        lhs_array.value_at(lhs_idx).as_ref(),
        rhs_array.value_at(rhs_idx).as_ref(),
        order_type,
    )
}

pub fn compare_rows_in_chunk(
    lhs_data_chunk: &DataChunk,
    lhs_idx: usize,
    rhs_data_chunk: &DataChunk,
    rhs_idx: usize,
    order_pairs: &[OrderPair],
) -> Result<Ordering> {
    for order_pair in order_pairs.iter() {
        let lhs_array = lhs_data_chunk.column_at(order_pair.column_idx).array();
        let rhs_array = rhs_data_chunk.column_at(order_pair.column_idx).array();
        macro_rules! gen_match {
            ( $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
                match (lhs_array.as_ref(), rhs_array.as_ref()) {
                    $((ArrayImpl::$variant_name(lhs_inner), ArrayImpl::$variant_name(rhs_inner)) => Ok(compare_values_in_array(lhs_inner, lhs_idx, rhs_inner, rhs_idx, &order_pair.order_type)),)*
                    (l_arr, r_arr) => Err(InternalError(format!("Unmatched array types, lhs array is: {}, rhs array is: {}", l_arr.get_ident(), r_arr.get_ident()))),
                }?
            }
        }
        let res = for_all_variants! { gen_match };
        if res != Ordering::Equal {
            return Ok(res);
        }
    }
    Ok(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use itertools::Itertools;

    use super::{OrderPair, OrderType};
    use crate::array::{DataChunk, ListValue, StructValue};
    use crate::row::{OwnedRow, Row};
    use crate::types::{DataType, ScalarImpl};
    use crate::util::sort_util::compare_rows_in_chunk;

    #[test]
    fn test_compare_rows_in_chunk() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v11 = Some(ScalarImpl::Utf8("hello".into()));
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".into()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = OwnedRow::new(vec![v10, v11, v12]);
        let row2 = OwnedRow::new(vec![v20, v21, v22]);
        let chunk = DataChunk::from_rows(
            &[row1, row2],
            &[DataType::Int32, DataType::Varchar, DataType::Float32],
        );
        let order_pairs = vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Descending),
        ];

        assert_eq!(
            Ordering::Equal,
            compare_rows_in_chunk(&chunk, 0, &chunk, 0, &order_pairs).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows_in_chunk(&chunk, 0, &chunk, 1, &order_pairs).unwrap()
        );
    }

    #[test]
    fn test_compare_all_types() {
        let row1 = OwnedRow::new(vec![
            Some(ScalarImpl::Int16(16)),
            Some(ScalarImpl::Int32(32)),
            Some(ScalarImpl::Int64(64)),
            Some(ScalarImpl::Float32(3.2.into())),
            Some(ScalarImpl::Float64(6.4.into())),
            Some(ScalarImpl::Utf8("hello".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Decimal(10.into())),
            Some(ScalarImpl::Interval(Default::default())),
            Some(ScalarImpl::NaiveDate(Default::default())),
            Some(ScalarImpl::NaiveDateTime(Default::default())),
            Some(ScalarImpl::NaiveTime(Default::default())),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Float32(3.0.into())),
            ]))),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Int32(2)),
            ]))),
        ]);
        let row2 = OwnedRow::new(vec![
            Some(ScalarImpl::Int16(16)),
            Some(ScalarImpl::Int32(32)),
            Some(ScalarImpl::Int64(64)),
            Some(ScalarImpl::Float32(3.2.into())),
            Some(ScalarImpl::Float64(6.4.into())),
            Some(ScalarImpl::Utf8("hello".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Decimal(10.into())),
            Some(ScalarImpl::Interval(Default::default())),
            Some(ScalarImpl::NaiveDate(Default::default())),
            Some(ScalarImpl::NaiveDateTime(Default::default())),
            Some(ScalarImpl::NaiveTime(Default::default())),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Float32(33333.0.into())), // larger than row1
            ]))),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Int32(2)),
            ]))),
        ]);

        let order_pairs = (0..row1.len())
            .map(|i| OrderPair::new(i, OrderType::Ascending))
            .collect_vec();

        let chunk = DataChunk::from_rows(
            &[row1, row2],
            &[
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Varchar,
                DataType::Boolean,
                DataType::Decimal,
                DataType::Interval,
                DataType::Date,
                DataType::Timestamp,
                DataType::Time,
                DataType::new_struct(vec![DataType::Int32, DataType::Float32], vec![]),
                DataType::List {
                    datatype: Box::new(DataType::Int32),
                },
            ],
        );
        assert_eq!(
            Ordering::Equal,
            compare_rows_in_chunk(&chunk, 0, &chunk, 0, &order_pairs).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows_in_chunk(&chunk, 0, &chunk, 1, &order_pairs).unwrap()
        );
    }
}
