use std::sync::Arc;

use prost::Message as _;

use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::SortAggNode;

use crate::executor::{BoxedExecutor, Executor, ExecutorBuilder, ExecutorResult};
use risingwave_common::array::{column::Column, DataChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost, BoxedExpression};
use risingwave_common::types::DataType;
use risingwave_common::vector_op::agg::{self, BoxedAggState, BoxedSortedGrouper, EqGroups};

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
    schema: Schema,
}

impl BoxedExecutorBuilder for SortAggExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::SortAgg);

        ensure!(source.plan_node().get_children().len() == 1);
        let proto_child = source
            .plan_node()
            .get_children()
            .get(0)
            .ok_or_else(|| ErrorCode::InternalError(String::from("")))?;
        let child = source.clone_for_plan(proto_child).build()?;

        let sort_agg_node = SortAggNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(|e| RwError::from(ProstError(e)))?;

        let agg_states = sort_agg_node
            .get_agg_calls()
            .iter()
            .map(agg::create_agg_state)
            .collect::<Result<Vec<BoxedAggState>>>()?;

        let group_exprs = sort_agg_node
            .get_group_keys()
            .iter()
            .map(build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        let sorted_groupers = group_exprs
            .iter()
            .map(|e| agg::create_sorted_grouper(e.return_type()))
            .collect::<Result<Vec<BoxedSortedGrouper>>>()?;

        let fields = group_exprs
            .iter()
            .map(|e| e.return_type_ref())
            .chain(agg_states.iter().map(|e| e.return_type_ref()))
            .map(|t| Field { data_type: t })
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            agg_states,
            group_exprs,
            sorted_groupers,
            child,
            child_done: false,
            schema: Schema { fields },
        }))
    }
}

#[async_trait::async_trait]
impl Executor for SortAggExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
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

        while let ExecutorResult::Batch(child_chunk) = self.child.execute().await? {
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

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::test_utils::MockExecutor;
    use prost_types::Any as ProstAny;
    use risingwave_common::array::{Array as _, I32Array, I64Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::expr::build_from_prost;
    use risingwave_common::types::{DataTypeKind, Int32Type};
    use risingwave_pb::data::{data_type::TypeName, DataType as DataTypeProst};
    use risingwave_pb::expr::expr_node::Type::InputRef;
    use risingwave_pb::expr::{agg_call::Arg, agg_call::Type, AggCall, ExprNode, InputRefExpr};

    use super::*;

    #[tokio::test]
    #[allow(clippy::many_single_char_names)]
    async fn execute_sum_int32() -> Result<()> {
        let a = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let t32 = Int32Type::create(false);
        let chunk = DataChunk::builder()
            .columns(vec![Column::new(a, t32)])
            .build();
        let schema = Schema {
            fields: vec![Field {
                data_type: Int32Type::create(false),
            }],
        };
        let mut child = MockExecutor::new(schema);
        child.add(chunk);

        let prost = AggCall {
            r#type: Type::Sum as i32,
            args: vec![Arg {
                input: Some(InputRefExpr { column_idx: 0 }),
                r#type: Some(DataTypeProst {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
        };

        let s = agg::create_agg_state(&prost)?;

        let group_exprs: Vec<BoxedExpression> = vec![];
        let agg_states = vec![s];
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type_ref())
            .chain(agg_states.iter().map(|e| e.return_type_ref()))
            .map(|t| Field { data_type: t })
            .collect::<Vec<Field>>();
        let mut executor = SortAggExecutor {
            agg_states,
            group_exprs: vec![],
            sorted_groupers: vec![],
            child: Box::new(child),
            child_done: false,
            schema: Schema { fields },
        };

        executor.init()?;
        let o = executor.execute().await?.batch_or()?;
        if let ExecutorResult::Batch(_) = executor.execute().await? {
            panic!("simple agg should have no more than 1 output.");
        }
        executor.clean()?;

        let actual = o.column_at(0)?.array();
        let actual: &I64Array = actual.as_ref().into();
        let v = actual.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(6)]);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::many_single_char_names)]
    async fn execute_sum_int32_grouped() -> Result<()> {
        use risingwave_common::array::ArrayImpl;
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
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: Int32Type::create(false),
                },
            ],
        };
        let mut child = MockExecutor::new(schema);
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

        let prost = AggCall {
            r#type: Type::Sum as i32,
            args: vec![Arg {
                input: Some(InputRefExpr { column_idx: 0 }),
                r#type: Some(DataTypeProst {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
        };

        let s = agg::create_agg_state(&prost)?;

        let group_exprs = (1..=2)
            .map(|idx| {
                build_from_prost(&ExprNode {
                    expr_type: InputRef as i32,
                    body: Some(ProstAny {
                        type_url: "/".to_string(),
                        value: InputRefExpr { column_idx: idx }.encode_to_vec(),
                    }),
                    return_type: Some(DataTypeProst {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    rex_node: None,
                })
            })
            .collect::<Result<Vec<BoxedExpression>>>()?;
        let sorted_groupers = group_exprs
            .iter()
            .map(|e| agg::create_sorted_grouper(e.return_type()))
            .collect::<Result<Vec<BoxedSortedGrouper>>>()?;

        let agg_states = vec![s];
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type_ref())
            .chain(agg_states.iter().map(|e| e.return_type_ref()))
            .map(|t| Field { data_type: t })
            .collect::<Vec<Field>>();

        let mut executor = SortAggExecutor {
            agg_states,
            group_exprs,
            sorted_groupers,
            child: Box::new(child),
            child_done: false,
            schema: Schema { fields },
        };

        executor.init()?;
        let fields = &executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        assert_eq!(fields[1].data_type.data_type_kind(), DataTypeKind::Int32);
        assert_eq!(fields[2].data_type.data_type_kind(), DataTypeKind::Int64);
        let o = executor.execute().await?.batch_or()?;
        if let ExecutorResult::Batch(_) = executor.execute().await? {
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
