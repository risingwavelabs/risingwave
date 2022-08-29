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

use std::cmp::{Ord, Ordering};
use std::sync::Arc;

use risingwave_pb::plan_common::{ColumnOrder, OrderType as ProstOrderType};

use crate::array::{Array, ArrayImpl, DataChunk, Row};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::ScalarImpl;

pub const K_PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum OrderType {
    Ascending,
    Descending,
}

impl OrderType {
    pub fn from_prost(order_type: &ProstOrderType) -> OrderType {
        match order_type {
            ProstOrderType::Ascending => OrderType::Ascending,
            ProstOrderType::Descending => OrderType::Descending,
            ProstOrderType::OrderUnspecified => unreachable!(),
        }
    }

    pub fn to_prost(&self) -> ProstOrderType {
        match self {
            OrderType::Ascending => ProstOrderType::Ascending,
            OrderType::Descending => ProstOrderType::Descending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn from_prost(column_order: &ColumnOrder) -> Self {
        let order_type: ProstOrderType = ProstOrderType::from_i32(column_order.order_type).unwrap();
        OrderPair {
            order_type: OrderType::from_prost(&order_type),
            column_idx: column_order.index as usize,
        }
    }

    pub fn to_protobuf(&self) -> ColumnOrder {
        ColumnOrder {
            order_type: self.order_type.to_prost() as i32,
            index: self.column_idx as u32,
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

fn compare_values<'a, T>(lhs: Option<&T>, rhs: Option<&T>, order_type: &'a OrderType) -> Ordering
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

pub fn compare_rows(lhs: &Row, rhs: &Row, order_pairs: &[OrderPair]) -> Result<Ordering> {
    for order_pair in order_pairs.iter() {
        let lhs = lhs[order_pair.column_idx].as_ref();
        let rhs = rhs[order_pair.column_idx].as_ref();

        macro_rules! gen_match {
            ($lhs: ident, $rhs: ident, [$( $tt: ident), *]) => {
                match ($lhs, $rhs) {
                    $((Some(ScalarImpl::$tt(l)), Some(ScalarImpl::$tt(r))) => Ok(compare_values(Some(l), Some(r), &order_pair.order_type)),)*
                    $((Some(ScalarImpl::$tt(l)), None) => Ok(compare_values(Some(l), None, &order_pair.order_type)),)*
                    $((None, Some(ScalarImpl::$tt(r))) => Ok(compare_values(None, Some(r), &order_pair.order_type)),)*
                    (None, None) => Ok(compare_values::<()>(None, None, &order_pair.order_type)),
                    (Some(l), Some(r)) => Err(InternalError(format!("Unmatched scalar types, lhs is: {:?}, rhs is: {:?}", l, r))),
                }?
            }
        }

        let res = gen_match!(
            lhs,
            rhs,
            [
                Int16,
                Int32,
                Int64,
                Float32,
                Float64,
                Utf8,
                Bool,
                Decimal,
                Interval,
                NaiveDate,
                NaiveDateTime,
                NaiveTime,
                Struct,
                List
            ]
        );

        if res != Ordering::Equal {
            return Ok(res);
        }
    }
    Ok(Ordering::Equal)
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
            ($lhs: ident, $rhs: ident, [$( $tt: ident), *]) => {
                match ($lhs, $rhs) {
                    $((ArrayImpl::$tt(lhs_inner), ArrayImpl::$tt(rhs_inner)) => Ok(compare_values_in_array(lhs_inner, lhs_idx, rhs_inner, rhs_idx, &order_pair.order_type)),)*
                    (l_arr, r_arr) => Err(InternalError(format!("Unmatched array types, lhs array is: {}, rhs array is: {}", l_arr.get_ident(), r_arr.get_ident()))),
                }?
            }
        }
        let (lhs_array, rhs_array) = (lhs_array.as_ref(), rhs_array.as_ref());
        let res = gen_match!(
            lhs_array,
            rhs_array,
            [
                Int16,
                Int32,
                Int64,
                Float32,
                Float64,
                Utf8,
                Bool,
                Decimal,
                Interval,
                NaiveDate,
                NaiveDateTime,
                NaiveTime,
                Struct,
                List
            ]
        );
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

    use super::{compare_rows, OrderPair, OrderType};
    use crate::array::{DataChunk, ListValue, Row, StructValue};
    use crate::types::{DataType, ScalarImpl};
    use crate::util::sort_util::compare_rows_in_chunk;

    #[test]
    fn test_compare_rows() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v11 = Some(ScalarImpl::Utf8("hello".to_string()));
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

        assert_eq!(
            Ordering::Equal,
            compare_rows(&row1, &row1, &order_pairs).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows(&row1, &row2, &order_pairs).unwrap()
        );
    }

    #[test]
    fn test_compare_rows_in_chunk() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v11 = Some(ScalarImpl::Utf8("hello".to_string()));
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".to_string()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = Row::new(vec![v10, v11, v12]);
        let row2 = Row::new(vec![v20, v21, v22]);
        let chunk = DataChunk::from_rows(
            &[row1, row2],
            &[DataType::Int32, DataType::Varchar, DataType::Float32],
        )
        .unwrap();
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
        let row1 = Row::new(vec![
            Some(ScalarImpl::Int16(16)),
            Some(ScalarImpl::Int32(32)),
            Some(ScalarImpl::Int64(64)),
            Some(ScalarImpl::Float32(3.2.into())),
            Some(ScalarImpl::Float64(6.4.into())),
            Some(ScalarImpl::Utf8("hello".to_string())),
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
        let row2 = Row::new(vec![
            Some(ScalarImpl::Int16(16)),
            Some(ScalarImpl::Int32(32)),
            Some(ScalarImpl::Int64(64)),
            Some(ScalarImpl::Float32(3.2.into())),
            Some(ScalarImpl::Float64(6.4.into())),
            Some(ScalarImpl::Utf8("hello".to_string())),
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

        let order_pairs = (0..row1.size())
            .map(|i| OrderPair::new(i, OrderType::Ascending))
            .collect_vec();
        assert_eq!(
            Ordering::Equal,
            compare_rows(&row1, &row1, &order_pairs).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows(&row1, &row2, &order_pairs).unwrap()
        );

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
        )
        .unwrap();
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
