use crate::array2::{column::Column, DataChunk};
use crate::error::ErrorCode::ProtobufError;
use crate::error::{ErrorCode, Result, RwError};
use crate::executor::{BoxedExecutor, Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{AggExpression, Expression as _};
use crate::types::DataType;
use crate::vector_op::agg::BoxedAggState;
use protobuf::Message as _;
use risingwave_proto::plan::{PlanNode_PlanNodeType, SimpleAggNode};
use std::convert::TryFrom;
use std::sync::Arc;

pub(super) struct SimpleAggExecutor {
    agg_exprs: Vec<AggExpression>,
    agg_states: Vec<BoxedAggState>,
    child: BoxedExecutor,
    child_done: bool,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for SimpleAggExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::SIMPLE_AGG);

        ensure!(source.plan_node().get_children().len() == 1);
        let proto_child = source
            .plan_node()
            .get_children()
            .get(0)
            .ok_or_else(|| ErrorCode::InternalError(String::from("")))?;
        let child = ExecutorBuilder::new(proto_child, source.global_task_env().clone()).build()?;

        let simple_agg_node =
            SimpleAggNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let agg_exprs = simple_agg_node
            .get_aggregations()
            .iter()
            .map(AggExpression::try_from)
            .collect::<Result<Vec<AggExpression>>>()?;

        let agg_states = agg_exprs
            .iter()
            .map(|expr| expr.create_agg_state())
            .collect::<Result<Vec<BoxedAggState>>>()?;

        Ok(Self {
            agg_exprs,
            agg_states,
            child,
            child_done: false,
        })
    }
}

impl Executor for SimpleAggExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.child_done {
            return Ok(ExecutorResult::Done);
        }

        let cardinality = 1;
        let mut array_builders = self
            .agg_exprs
            .iter()
            .map(|e| DataType::create_array_builder(e.return_type_ref(), cardinality))
            .collect::<Result<Vec<_>>>()?;

        while let ExecutorResult::Batch(child_chunk) = self.child.execute()? {
            self.agg_exprs
                .iter_mut()
                .zip(&mut self.agg_states)
                .try_for_each(|(expr, state)| {
                    state.update(expr.eval_child(&child_chunk)?.as_ref())
                })?;
        }
        self.child_done = true;

        self.agg_states
            .iter()
            .zip(&mut array_builders)
            .try_for_each(|(state, builder)| state.output(builder))?;

        let columns = self
            .agg_exprs
            .iter()
            .zip(array_builders)
            .map(|(e, b)| {
                Ok(Column {
                    array: Arc::new(b.finish()?),
                    data_type: e.return_type_ref(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let ret = DataChunk::builder()
            .cardinality(cardinality)
            .columns(columns)
            .build();

        Ok(ExecutorResult::Batch(Arc::new(ret)))
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::{Array as _, I32Array, I64Array};
    use crate::array_nonnull;
    use crate::executor::test_utils::MockExecutor;
    use crate::types::Int32Type;
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any;
    use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
    use risingwave_proto::expr::{ExprNode, ExprNode_ExprNodeType, FunctionCall, InputRefExpr};

    #[test]
    fn execute_sum_int32() -> Result<()> {
        let a = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let t32 = Arc::new(Int32Type::new(false));
        let chunk = DataChunk::builder()
            .cardinality(3)
            .columns(vec![Column {
                array: a,
                data_type: t32.clone(),
            }])
            .build();
        let mut child = MockExecutor::new();
        child.add(chunk);

        let proto = make_proto!(ExprNode, {
          expr_type: ExprNode_ExprNodeType::SUM,
          return_type: make_proto!(DataTypeProto, {
            type_name: DataType_TypeName::INT64
          }),
          body: Any::pack(&make_proto!(FunctionCall, {
            children: vec![make_proto!(ExprNode, {
              expr_type: ExprNode_ExprNodeType::INPUT_REF,
              return_type: make_proto!(DataTypeProto, {
                type_name: DataType_TypeName::INT32
              }),
              body: Any::pack(&make_proto!(InputRefExpr, {column_idx: 0})).unwrap()
            })].into()
          })).unwrap()
        });

        let e = AggExpression::try_from(&proto)?;
        let s = e.create_agg_state()?;

        let mut executor = SimpleAggExecutor {
            agg_exprs: vec![e],
            agg_states: vec![s],
            child: Box::new(child),
            child_done: false,
        };

        executor.init()?;
        let o = executor.execute()?.batch_or()?;
        if let ExecutorResult::Batch(_) = executor.execute()? {
            panic!("simple agg should have no more than 1 output.");
        }
        executor.clean()?;

        let actual = o.column_at(0)?.array;
        let actual: &I64Array = actual.as_ref().into();
        let v = actual.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(6)]);

        Ok(())
    }
}
