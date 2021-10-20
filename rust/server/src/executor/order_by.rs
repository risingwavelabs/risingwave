use super::BoxedExecutor;
use crate::array::{
    column::Column, Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkRef,
};
use crate::error::{
    ErrorCode::{InternalError, ProtobufError},
    Result, RwError,
};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::types::DataTypeRef;
use crate::util::sort_util::{
    compare_two_row, fetch_orders_from_order_by_node, HeapElem, OrderPair, K_PROCESSING_WINDOW_SIZE,
};
use protobuf::Message;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::iter::Iterator;
use std::sync::Arc;
use std::vec::Vec;

use risingwave_proto::plan::{OrderByNode as OrderByProto, PlanNode_PlanNodeType};

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

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for OrderByExecutor {
    type Error = RwError;
    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::ORDER_BY);
        ensure!(source.plan_node().get_children().len() == 1);
        let order_by_node =
            OrderByProto::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(ProtobufError)?;
        let order_pairs = fetch_orders_from_order_by_node(&order_by_node).unwrap();
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
    fn get_order_index_from(&self, chunk: &DataChunk) -> Vec<usize> {
        let mut index: Vec<usize> = (0..chunk.cardinality()).collect();
        index.sort_by(|ia, ib| {
            compare_two_row(self.order_pairs.as_ref(), chunk, *ia, chunk, *ib)
                .unwrap_or(Ordering::Equal)
        });
        index
    }
    fn collect_child_data(&mut self) -> Result<()> {
        while let ExecutorResult::Batch(chunk) = self.child.execute()? {
            self.sorted_indices.push(self.get_order_index_from(&chunk));
            self.chunks.push(Arc::new(chunk));
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
        self.first_execution = true;
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
        let chunk = DataChunk::builder().columns(columns).build();
        Ok(ExecutorResult::Batch(chunk))
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::column::Column;
    use crate::array::{DataChunk, PrimitiveArray};
    use crate::executor::test_utils::MockExecutor;
    use crate::expr::InputRefExpression;
    use crate::types::Int32Type;
    use crate::util::sort_util::OrderType;

    use std::sync::Arc;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Int32Type::create(false);
        Ok(Column::new(array, data_type))
    }

    #[test]
    fn test_simple_order_by_executor() {
        let col0 = create_column(&[Some(1), Some(2), Some(3)]).unwrap();
        let col1 = create_column(&[Some(3), Some(2), Some(1)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col0, col1].to_vec()).build();
        let mut mock_executor = MockExecutor::new();
        mock_executor.add(data_chunk);
        let input_ref_1 = InputRefExpression::new(Int32Type::create(false), 0usize);
        let input_ref_2 = InputRefExpression::new(Int32Type::create(false), 1usize);
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
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
            assert_eq!(col0.array().as_int32().value_at(2), Some(1));
        }
    }
}
