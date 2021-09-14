use std::cmp::{Ord, Ordering};
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::iter::Iterator;
use std::sync::Arc;
use std::vec::Vec;

use protobuf::Message;

use super::BoxedExecutor;
use crate::array2::{
    column::Column, Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkRef,
};
use crate::error::{
    ErrorCode::{InternalError, ProtobufError},
    Result, RwError,
};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::InputRefExpression;
use crate::types::{DataTypeRef, ScalarPartialOrd, ScalarRef};

use risingwave_proto::plan::{
    OrderByNode as OrderByProto, OrderByNode_OrderType, PlanNode_PlanNodeType,
};

const K_PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(PartialEq)]
enum OrderType {
    Ascending,
    Descending,
}

struct OrderPair {
    order_type: OrderType,
    order: Box<InputRefExpression>,
}

pub(super) struct OrderByExecutor {
    child: BoxedExecutor,
    first_execution: bool,
    sorted_indices: Vec<Vec<usize>>,
    chunks: Vec<DataChunkRef>,
    vis_indices: Vec<usize>,
    min_heap: BinaryHeap<HeapElem>,
    order_pairs: Arc<Vec<OrderPair>>,
    data_types: Vec<DataTypeRef>,
}

struct HeapElem {
    order_pairs: Arc<Vec<OrderPair>>,
    chunk: DataChunkRef,
    chunk_idx: usize,
    elem_idx: usize,
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

fn compare_two_row(
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

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for OrderByExecutor {
    type Error = RwError;
    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::ORDER_BY);
        ensure!(source.plan_node().get_children().len() == 1);
        let order_by_node =
            OrderByProto::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(ProtobufError)?;
        ensure!(order_by_node.get_order_types().len() == order_by_node.get_orders().len());
        let mut order_pairs = Vec::<OrderPair>::new();
        for i in 0..order_by_node.get_order_types().len() {
            let order = InputRefExpression::try_from(&order_by_node.get_orders()[i])?;
            order_pairs.push(OrderPair {
                order_type: match order_by_node.get_order_types()[i] {
                    OrderByNode_OrderType::ASCENDING => Ok(OrderType::Ascending),
                    OrderByNode_OrderType::DESCENDING => Ok(OrderType::Descending),
                    OrderByNode_OrderType::INVALID => Err(RwError::from(InternalError(
                        String::from("Invalid OrderType"),
                    ))),
                }?,
                order: Box::new(order),
            });
        }
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child =
                ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
            return Ok(Self {
                order_pairs: Arc::new(order_pairs),
                child,
                first_execution: true,
                vis_indices: vec![],
                chunks: vec![],
                sorted_indices: vec![],
                min_heap: BinaryHeap::new(),
                data_types: vec![],
            });
        }
        Err(InternalError("OrderBy must have one child".to_string()).into())
    }
}

impl OrderByExecutor {
    fn push_heap_for_chunk(&mut self, idx: usize) {
        while self.vis_indices[idx] < self.chunks[idx].cardinality() {
            let skip: bool = match self.chunks[idx].visibility() {
                Some(visibility) => visibility
                    .is_set(self.sorted_indices[idx][self.vis_indices[idx]])
                    .map(|b| !b)
                    .unwrap_or(false),
                None => false,
            };
            if !skip {
                let elem = HeapElem {
                    order_pairs: self.order_pairs.clone(),
                    chunk: self.chunks[idx].clone(),
                    chunk_idx: idx,
                    elem_idx: self.sorted_indices[idx][self.vis_indices[idx]],
                };
                self.min_heap.push(elem);
                self.vis_indices[idx] += 1;
                break;
            }
            self.vis_indices[idx] += 1;
        }
    }
    fn get_order_index_from(&self, chunk: DataChunkRef) -> Vec<usize> {
        let mut index: Vec<usize> = (0..chunk.cardinality()).collect();
        index.sort_by(|ia, ib| {
            compare_two_row(
                self.order_pairs.as_ref(),
                chunk.as_ref(),
                *ia,
                chunk.as_ref(),
                *ib,
            )
            .unwrap_or(Ordering::Equal)
        });
        index
    }
    fn collect_child_data(&mut self) -> Result<()> {
        while let ExecutorResult::Batch(chunk) = self.child.execute()? {
            self.sorted_indices
                .push(self.get_order_index_from(chunk.clone()));
            self.chunks.push(chunk);
        }
        self.vis_indices = vec![0usize; self.chunks.len()];
        for idx in 0..self.chunks.len() {
            self.push_heap_for_chunk(idx);
        }
        Ok(())
    }

    fn fill_data_types(&mut self) {
        let mut data_types = Vec::new();
        for i in 0..self.chunks[0].dimension() {
            data_types.push(self.chunks[0].column_at(i).unwrap().data_type().clone());
        }
        self.data_types = data_types;
    }
}

impl Executor for OrderByExecutor {
    fn init(&mut self) -> Result<()> {
        self.first_execution = false;
        self.child.init()?;
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.first_execution {
            self.collect_child_data()?;
            self.fill_data_types();
            self.first_execution = false;
        }
        let mut builders = self
            .data_types
            .iter()
            .map(|t| t.clone().create_array_builder(K_PROCESSING_WINDOW_SIZE))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;
        let mut chunk_size = 0usize;
        while !self.min_heap.is_empty() && chunk_size < K_PROCESSING_WINDOW_SIZE {
            let top = self.min_heap.pop().unwrap();
            for (idx, builder) in builders.iter_mut().enumerate() {
                let chunk_arr = self.chunks[top.chunk_idx].column_at(idx)?.array();
                let chunk_arr = chunk_arr.as_ref();
                macro_rules! gen_match {
            ($b: ident, $a: ident, [$( $tt: ident), *]) => {
                match ($b, $a) {
                    $((ArrayBuilderImpl::$tt($b), ArrayImpl::$tt($a)) => Ok($b.append($a.value_at(top.elem_idx))),)*
                        _ => Err(InternalError(String::from("Unmatched array and array builder types"))),
                }?
            }
        }
                let _ = gen_match!(
                    builder,
                    chunk_arr,
                    [Int16, Int32, Int64, Float32, Float64, UTF8, Bool]
                );
            }
            chunk_size += 1;
            self.push_heap_for_chunk(top.chunk_idx);
        }
        if chunk_size == 0 {
            return Ok(ExecutorResult::Done);
        }
        let columns = self
            .data_types
            .iter()
            .zip(builders)
            .map(|(d, b)| Ok(Column::new(Arc::new(b.finish()?), d.clone())))
            .collect::<Result<Vec<_>>>()?;
        let chunk = DataChunk::builder()
            .cardinality(chunk_size)
            .columns(columns)
            .build();
        Ok(ExecutorResult::Batch(Arc::new(chunk)))
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::column::Column;
    use crate::array2::{DataChunk, PrimitiveArray};
    use crate::executor::test_utils::MockExecutor;
    use crate::expr::InputRefExpression;
    use crate::types::Int32Type;

    use std::sync::Arc;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Arc::new(Int32Type::new(false));
        Ok(Column::new(array, data_type))
    }

    #[test]
    fn test_simple_order_by_executor() {
        let col0 = create_column(&[Some(1), Some(2), Some(3)]).unwrap();
        let col1 = create_column(&[Some(3), Some(2), Some(1)]).unwrap();
        let data_chunk = DataChunk::builder()
            .cardinality(3)
            .columns([col0, col1].to_vec())
            .build();
        let mut mock_executor = MockExecutor::new();
        mock_executor.add(data_chunk);
        let input_ref_1 = InputRefExpression::new(Arc::new(Int32Type::new(false)), 0usize);
        let input_ref_2 = InputRefExpression::new(Arc::new(Int32Type::new(false)), 1usize);
        let order_pairs = vec![
            OrderPair {
                order: Box::new(input_ref_2),
                order_type: OrderType::Ascending,
            },
            OrderPair {
                order: Box::new(input_ref_1),
                order_type: OrderType::Ascending,
            },
        ];
        let mut order_by_executor = OrderByExecutor {
            order_pairs: Arc::new(order_pairs),
            child: Box::new(mock_executor),
            first_execution: true,
            vis_indices: vec![],
            chunks: vec![],
            sorted_indices: vec![],
            min_heap: BinaryHeap::new(),
            data_types: vec![],
        };
        let res = order_by_executor.execute().unwrap();
        assert!(matches!(res, ExecutorResult::Batch(_)));
        if let ExecutorResult::Batch(res) = res {
            let col0 = res.as_ref().column_at(0).unwrap();
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
            assert_eq!(col0.array().as_int32().value_at(2), Some(1));
        }
    }
}
