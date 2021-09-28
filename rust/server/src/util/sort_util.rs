use crate::array2::{Array, ArrayImpl, DataChunk, DataChunkRef};
use crate::error::{ErrorCode::InternalError, Result, RwError};
use crate::expr::InputRefExpression;
use crate::types::{ScalarPartialOrd, ScalarRef};
use risingwave_proto::plan::{OrderByNode, OrderByNode_OrderType};
use std::cmp::{Ord, Ordering};
use std::convert::TryFrom;
use std::sync::Arc;

pub const K_PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(PartialEq)]
pub enum OrderType {
    Ascending,
    Descending,
}

pub struct OrderPair {
    pub order_type: OrderType,
    pub order: Box<InputRefExpression>,
}

pub struct HeapElem {
    pub order_pairs: Arc<Vec<OrderPair>>,
    pub chunk: DataChunkRef,
    pub chunk_idx: usize,
    pub elem_idx: usize,
}

impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_two_row(
            self.order_pairs.as_ref(),
            self.chunk.as_ref(),
            self.elem_idx,
            other.chunk.as_ref(),
            other.elem_idx,
        )
        .unwrap()
        {
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Less,
        }
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
                $((ArrayImpl::$tt(lhs_inner), ArrayImpl::$tt(rhs_inner)) => Ok(compare_value_in_array( lhs_inner, lhs_idx, rhs_inner, rhs_idx, &order_pair.order_type)),)*
                _ => Err(InternalError(String::from("Unmatched array types"))),
            }?
        }
    }
        let (lhs_array, rhs_array) = (lhs_array.as_ref(), rhs_array.as_ref());
        let res = gen_match!(
            lhs_array,
            rhs_array,
            [Int16, Int32, Int64, Float32, Float64, UTF8, Bool]
        );
        if res != Ordering::Equal {
            return Ok(res);
        }
    }
    Ok(Ordering::Equal)
}

pub fn fetch_orders_from_order_by_node(order_by_node: &OrderByNode) -> Result<Vec<OrderPair>> {
    ensure!(order_by_node.get_order_types().len() == order_by_node.get_orders().len());
    let mut order_pairs = Vec::<OrderPair>::new();
    for i in 0..order_by_node.get_order_types().len() {
        let order = InputRefExpression::try_from(&order_by_node.get_orders()[i])?;
        order_pairs.push(OrderPair {
            order_type: match order_by_node.get_order_types()[i] {
                OrderByNode_OrderType::ASCENDING => Ok(OrderType::Ascending),
                OrderByNode_OrderType::DESCENDING => Ok(OrderType::Descending),
                OrderByNode_OrderType::INVALID => Err(RwError::from(InternalError(String::from(
                    "Invalid OrderType",
                )))),
            }?,
            order: Box::new(order),
        });
    }
    Ok(order_pairs)
}
