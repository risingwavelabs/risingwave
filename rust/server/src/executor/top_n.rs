use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::vec::Vec;

use protobuf::Message;

use risingwave_proto::plan::{PlanNode_PlanNodeType, TopNNode};

use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::Schema;
use crate::error::{
    ErrorCode::{InternalError, ProtobufError},
    Result,
};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::util::sort_util::{fetch_orders_from_order_by_node, HeapElem, OrderPair};

use super::{BoxedExecutor, BoxedExecutorBuilder};

struct TopNHeap {
    order_pairs: Arc<Vec<OrderPair>>,
    min_heap: BinaryHeap<Reverse<HeapElem>>,
    limit: usize,
}

impl TopNHeap {
    fn insert(&mut self, elem: HeapElem) {
        if self.min_heap.len() < self.limit {
            self.min_heap.push(Reverse(elem));
        } else if elem > self.min_heap.peek().unwrap().0 {
            self.min_heap.push(Reverse(elem));
            self.min_heap.pop();
        }
    }

    pub fn fit(&mut self, chunk: DataChunkRef) {
        DataChunk::rechunk(&[chunk], 1)
            .unwrap()
            .into_iter()
            .for_each(|c| {
                let elem = HeapElem {
                    order_pairs: self.order_pairs.clone(),
                    chunk: Arc::new(c),
                    chunk_idx: 0usize, // useless
                    elem_idx: 0usize,
                };
                self.insert(elem);
            });
    }

    pub fn dump(&mut self) -> Option<DataChunk> {
        if self.min_heap.is_empty() {
            return None;
        }
        let mut chunks = self
            .min_heap
            .drain_sorted()
            .map(|e| e.0.chunk)
            .collect::<Vec<_>>();
        chunks.reverse();
        if let Ok(mut res) = DataChunk::rechunk(&chunks, self.limit) {
            assert_eq!(res.len(), 1);
            Some(res.remove(0))
        } else {
            None
        }
    }
}

pub(super) struct TopNExecutor {
    child: BoxedExecutor,
    top_n_heap: TopNHeap,
}

impl BoxedExecutorBuilder for TopNExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::TOP_N);
        ensure!(source.plan_node().get_children().len() == 1);
        let top_n_node = TopNNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let order_by_node = top_n_node.get_order_by();
        let order_pairs = fetch_orders_from_order_by_node(order_by_node).unwrap();
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child =
                ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
            return Ok(Box::new(Self::new(
                child,
                order_pairs,
                top_n_node.get_limit() as usize,
            )));
        }
        Err(InternalError("TopN must have one child".to_string()).into())
    }
}

impl TopNExecutor {
    fn new(child: BoxedExecutor, order_pairs: Vec<OrderPair>, limit: usize) -> Self {
        Self {
            top_n_heap: TopNHeap {
                min_heap: BinaryHeap::new(),
                limit,
                order_pairs: Arc::new(order_pairs),
            },
            child,
        }
    }
}

#[async_trait::async_trait]
impl Executor for TopNExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        while let ExecutorResult::Batch(chunk) = self.child.execute().await? {
            self.top_n_heap.fit(Arc::new(chunk));
        }
        if let Some(chunk) = self.top_n_heap.dump() {
            Ok(ExecutorResult::Batch(chunk))
        } else {
            Ok(ExecutorResult::Done)
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::column::Column;
    use crate::array::{Array, DataChunk, PrimitiveArray};
    use crate::catalog::{Field, Schema};
    use crate::executor::test_utils::MockExecutor;
    use crate::expr::InputRefExpression;
    use crate::types::{DataTypeKind, Int32Type};
    use crate::util::sort_util::OrderType;

    use super::*;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Int32Type::create(false);
        Ok(Column::new(array, data_type))
    }

    #[tokio::test]
    async fn test_simple_top_n_executor() {
        let col0 = create_column(&[Some(1), Some(2), Some(3)]).unwrap();
        let col1 = create_column(&[Some(3), Some(2), Some(1)]).unwrap();
        let data_chunk = DataChunk::builder().columns(vec![col0, col1]).build();
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: Int32Type::create(false),
                },
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
        let input_ref_0 = InputRefExpression::new(Int32Type::create(false), 0usize);
        let input_ref_1 = InputRefExpression::new(Int32Type::create(false), 1usize);
        let order_pairs = vec![
            OrderPair {
                order: Box::new(input_ref_1),
                order_type: OrderType::Ascending,
            },
            OrderPair {
                order: Box::new(input_ref_0),
                order_type: OrderType::Ascending,
            },
        ];
        let mut top_n_executor = TopNExecutor::new(Box::new(mock_executor), order_pairs, 2usize);
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        assert_eq!(fields[1].data_type.data_type_kind(), DataTypeKind::Int32);
        let res = top_n_executor.execute().await.unwrap();
        assert!(matches!(res, ExecutorResult::Batch(_)));
        if let ExecutorResult::Batch(res) = res {
            assert_eq!(res.cardinality(), 2);
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
        }
        let res = top_n_executor.execute().await.unwrap();
        assert!(matches!(res, ExecutorResult::Done));
    }
}
