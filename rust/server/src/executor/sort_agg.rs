use crate::array::{column::Column, DataChunk};
use crate::error::ErrorCode::ProtobufError;
use crate::error::{ErrorCode, Result, RwError};
use crate::executor::{BoxedExecutor, Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression};
use crate::types::DataType;
use crate::vector_op::agg::{self, BoxedAggState, BoxedSortedGrouper, EqGroups};
use protobuf::Message as _;
use risingwave_proto::plan::{PlanNode_PlanNodeType, SortAggNode};
use std::sync::Arc;

use super::BoxedExecutorBuilder;

/// `SortAggExecutor` implements the sort aggregate algorithm, where tuples
/// belonging to the same group are continuous because they are sorted by the
/// group columns.
///
/// As a special case, simple aggregate without groups satisfies the requirement
/// automatically because all tuples should be aggregated together.
pub(super) struct SortAggExecutor {
    agg_states: Vec<BoxedAggState>,
    group_exprs: Vec<BoxedExpression>,
    sorted_groupers: Vec<BoxedSortedGrouper>,
    child: BoxedExecutor,
    child_done: bool,
}

impl BoxedExecutorBuilder for SortAggExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::SORT_AGG);

        ensure!(source.plan_node().get_children().len() == 1);
        let proto_child = source
            .plan_node()
            .get_children()
            .get(0)
            .ok_or_else(|| ErrorCode::InternalError(String::from("")))?;
        let child = ExecutorBuilder::new(proto_child, source.global_task_env().clone()).build()?;

        let sort_agg_node =
            SortAggNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let agg_states = sort_agg_node
            .get_agg_calls()
            .iter()
            .map(agg::create_agg_state)
            .collect::<Result<Vec<BoxedAggState>>>()?;

        let group_exprs = sort_agg_node
            .get_group_keys()
            .iter()
            .map(build_from_proto)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        let sorted_groupers = group_exprs
            .iter()
            .map(|e| agg::create_sorted_grouper(e.return_type()))
            .collect::<Result<Vec<BoxedSortedGrouper>>>()?;

        Ok(Box::new(Self {
            agg_states,
            group_exprs,
            sorted_groupers,
            child,
            child_done: false,
        }))
    }
}

impl Executor for SortAggExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.child_done {
            return Ok(ExecutorResult::Done);
        }

        let cardinality = 1;
        let mut group_builders = self
            .group_exprs
            .iter()
            .map(|e| DataType::create_array_builder(e.return_type_ref(), cardinality))
            .collect::<Result<Vec<_>>>()?;
        let mut array_builders = self
            .agg_states
            .iter()
            .map(|e| DataType::create_array_builder(e.return_type_ref(), cardinality))
            .collect::<Result<Vec<_>>>()?;

        while let ExecutorResult::Batch(child_chunk) = self.child.execute()? {
            let group_arrays = self
                .group_exprs
                .iter_mut()
                .map(|expr| expr.eval(&child_chunk))
                .collect::<Result<Vec<_>>>()?;

            let groups = self
                .sorted_groupers
                .iter()
                .zip(&group_arrays)
                .map(|(grouper, array)| grouper.split_groups(array))
                .collect::<Result<Vec<EqGroups>>>()?;
            let groups = EqGroups::intersect(&groups);

            self.sorted_groupers
                .iter_mut()
                .zip(&group_arrays)
                .zip(&mut group_builders)
                .try_for_each(|((grouper, array), builder)| {
                    grouper.update_and_output_with_sorted_groups(array, builder, &groups)
                })?;

            self.agg_states
                .iter_mut()
                .zip(&mut array_builders)
                .try_for_each(|(state, builder)| {
                    state.update_and_output_with_sorted_groups(&child_chunk, builder, &groups)
                })?;
        }
        self.child_done = true;

        self.sorted_groupers
            .iter()
            .zip(&mut group_builders)
            .try_for_each(|(grouper, builder)| grouper.output(builder))?;
        self.agg_states
            .iter()
            .zip(&mut array_builders)
            .try_for_each(|(state, builder)| state.output(builder))?;

        let columns = self
            .group_exprs
            .iter()
            .map(|e| e.return_type_ref())
            .chain(self.agg_states.iter().map(|e| e.return_type_ref()))
            .zip(group_builders.into_iter().chain(array_builders))
            .map(|(t, b)| Ok(Column::new(Arc::new(b.finish()?), t)))
            .collect::<Result<Vec<_>>>()?;

        let ret = DataChunk::builder().columns(columns).build();

        Ok(ExecutorResult::Batch(ret))
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array as _, I32Array, I64Array};
    use crate::array_nonnull;
    use crate::executor::test_utils::MockExecutor;
    use crate::types::Int32Type;
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any;
    use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
    use risingwave_proto::expr::{
        AggCall, AggCall_Arg, AggCall_Type, ExprNode, ExprNode_Type, InputRefExpr,
    };

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn execute_sum_int32() -> Result<()> {
        let a = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let t32 = Int32Type::create(false);
        let chunk = DataChunk::builder()
            .columns(vec![Column::new(a, t32)])
            .build();
        let mut child = MockExecutor::new();
        child.add(chunk);

        let proto = make_proto!(AggCall, {
          field_type: AggCall_Type::SUM,
          return_type: make_proto!(DataTypeProto, {
            type_name: DataType_TypeName::INT64
          }),
          args: vec![make_proto!(AggCall_Arg, {
            input: make_proto!(InputRefExpr, {column_idx: 0}),
            field_type: make_proto!(DataTypeProto, {
              type_name: DataType_TypeName::INT32
            })
          })].into()
        });

        let s = agg::create_agg_state(&proto)?;

        let mut executor = SortAggExecutor {
            agg_states: vec![s],
            group_exprs: vec![],
            sorted_groupers: vec![],
            child: Box::new(child),
            child_done: false,
        };

        executor.init()?;
        let o = executor.execute()?.batch_or()?;
        if let ExecutorResult::Batch(_) = executor.execute()? {
            panic!("simple agg should have no more than 1 output.");
        }
        executor.clean()?;

        let actual = o.column_at(0)?.array();
        let actual: &I64Array = actual.as_ref().into();
        let v = actual.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(6)]);

        Ok(())
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn execute_sum_int32_grouped() -> Result<()> {
        use crate::array::ArrayImpl;
        let a: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let t32 = Int32Type::create(false);
        let chunk = DataChunk::builder()
            .columns(vec![
                Column::new(a.clone(), t32.clone()),
                Column::new(
                    Arc::new(array_nonnull! { I32Array, [1, 1, 3] }.into()),
                    t32.clone(),
                ),
                Column::new(
                    Arc::new(array_nonnull! { I32Array, [7, 8, 8] }.into()),
                    t32.clone(),
                ),
            ])
            .build();
        let mut child = MockExecutor::new();
        child.add(chunk);
        let chunk = DataChunk::builder()
            .columns(vec![
                Column::new(a, t32.clone()),
                Column::new(
                    Arc::new(array_nonnull! { I32Array, [3, 4, 4] }.into()),
                    t32.clone(),
                ),
                Column::new(Arc::new(array_nonnull! { I32Array, [8, 8, 8] }.into()), t32),
            ])
            .build();
        child.add(chunk);

        let proto = make_proto!(AggCall, {
          field_type: AggCall_Type::SUM,
          return_type: make_proto!(DataTypeProto, {
            type_name: DataType_TypeName::INT64
          }),
          args: vec![make_proto!(AggCall_Arg, {
            input: make_proto!(InputRefExpr, {column_idx: 0}),
            field_type: make_proto!(DataTypeProto, {
              type_name: DataType_TypeName::INT32
            })
          })].into()
        });

        let s = agg::create_agg_state(&proto)?;

        let group_exprs = (1..=2)
            .map(|idx| {
                build_from_proto(&make_proto!(ExprNode, {
                  expr_type: ExprNode_Type::INPUT_REF,
                  return_type: make_proto!(DataTypeProto, {
                    type_name: DataType_TypeName::INT32
                  }),
                  body: Any::pack(&make_proto!(InputRefExpr, {column_idx: idx})).unwrap()
                }))
            })
            .collect::<Result<Vec<BoxedExpression>>>()?;
        let sorted_groupers = group_exprs
            .iter()
            .map(|e| agg::create_sorted_grouper(e.return_type()))
            .collect::<Result<Vec<BoxedSortedGrouper>>>()?;

        let mut executor = SortAggExecutor {
            agg_states: vec![s],
            group_exprs,
            sorted_groupers,
            child: Box::new(child),
            child_done: false,
        };

        executor.init()?;
        let o = executor.execute()?.batch_or()?;
        if let ExecutorResult::Batch(_) = executor.execute()? {
            panic!("simple agg should have no more than 1 output.");
        }
        executor.clean()?;

        let actual = o.column_at(2)?.array();
        let actual: &I64Array = actual.as_ref().into();
        let v = actual.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(1), Some(2), Some(4), Some(5)]);

        assert_eq!(
            o.column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(3), Some(4)]
        );
        assert_eq!(
            o.column_at(1)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(7), Some(8), Some(8), Some(8)]
        );

        Ok(())
    }
}
