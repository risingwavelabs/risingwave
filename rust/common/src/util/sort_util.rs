use std::cmp::{Ord, Ordering};
use std::sync::Arc;

use prost::DecodeError;
use risingwave_pb::expr::InputRefExpr;
use risingwave_pb::plan::{ColumnOrder, OrderType as ProstOrderType};

use crate::array::{Array, ArrayImpl, DataChunk, DataChunkRef};
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::InputRefExpression;
use crate::types::{DataTypeKind, ScalarPartialOrd, ScalarRef};

pub const K_PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum OrderType {
    Ascending,
    Descending,
}

pub fn build_from_prost(order_type: &i32) -> Result<OrderType> {
    let res = match ProstOrderType::from_i32(*order_type) {
        Some(ProstOrderType::Ascending) => OrderType::Ascending,
        Some(ProstOrderType::Descending) => OrderType::Descending,
        _ => {
            return Err(RwError::from(ErrorCode::ProstError(DecodeError::new(
                "No such order type",
            ))))
        }
    };
    Ok(res)
}

#[derive(Debug)]
pub struct OrderPair {
    pub order_type: OrderType,
    pub order: Box<InputRefExpression>,
}

#[derive(Clone, Debug)]
pub struct HeapElem {
    pub order_pairs: Arc<Vec<OrderPair>>,
    pub chunk: DataChunkRef,
    pub chunk_idx: usize,
    pub elem_idx: usize,
    /// DataChunk can be encoded to accelerate the comparison.
    /// Use risingwave_common::util::encoding_for_comparison::encode_chunk
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
            compare_two_row(
                self.order_pairs.as_ref(),
                self.chunk.as_ref(),
                self.elem_idx,
                other.chunk.as_ref(),
                other.elem_idx,
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

fn compare_value_in_array<'a, T>(
    lhs_array: &'a T,
    lhs_idx: usize,
    rhs_array: &'a T,
    rhs_idx: usize,
    order_type: &'a OrderType,
) -> Ordering
where
    T: Array,
    <<T as Array>::RefItem<'a> as ScalarRef<'a>>::ScalarType: ScalarPartialOrd,
{
    let (lhs_val, rhs_val) = (
        lhs_array.value_at(lhs_idx).unwrap(),
        rhs_array.value_at(rhs_idx).unwrap(),
    );
    match lhs_val.to_owned_scalar().scalar_cmp(rhs_val).unwrap() {
        Ordering::Equal => Ordering::Equal,
        Ordering::Less => {
            if *order_type == OrderType::Ascending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        Ordering::Greater => {
            if *order_type == OrderType::Descending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    }
}

pub fn compare_two_row(
    order_pairs: &[OrderPair],
    lhs_datachunk: &DataChunk,
    lhs_idx: usize,
    rhs_datachunk: &DataChunk,
    rhs_idx: usize,
) -> Result<Ordering> {
    for order_pair in order_pairs.iter() {
        let lhs_array = order_pair.order.eval_immut(lhs_datachunk)?;
        let rhs_array = order_pair.order.eval_immut(rhs_datachunk)?;
        macro_rules! gen_match {
        ($lhs: ident, $rhs: ident, [$( $tt: ident), *]) => {
            match ($lhs, $rhs) {
                $((ArrayImpl::$tt(lhs_inner), ArrayImpl::$tt(rhs_inner)) => Ok(compare_value_in_array(lhs_inner, lhs_idx, rhs_inner, rhs_idx, &order_pair.order_type)),)*
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
                NaiveTime
            ]
        );
        if res != Ordering::Equal {
            return Ok(res);
        }
    }
    Ok(Ordering::Equal)
}

pub fn fetch_orders(column_orders: &[ColumnOrder]) -> Result<Vec<OrderPair>> {
    let mut order_pairs = Vec::<OrderPair>::new();
    for column_order in column_orders {
        let order_type: ProstOrderType = column_order.get_order_type()?;
        let return_type = DataTypeKind::from(column_order.get_return_type()?);
        let input_ref: &InputRefExpr = column_order.get_input_ref()?;
        let input_ref_expr = InputRefExpression::new(return_type, input_ref.column_idx as usize);
        order_pairs.push(OrderPair {
            order_type: match order_type {
                ProstOrderType::Ascending => Ok(OrderType::Ascending),
                ProstOrderType::Descending => Ok(OrderType::Descending),
                ProstOrderType::Invalid => Err(RwError::from(InternalError(String::from(
                    "Invalid OrderType",
                )))),
            }?,
            order: Box::new(input_ref_expr),
        });
    }
    Ok(order_pairs)
}
