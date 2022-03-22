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
//
use std::collections::HashMap;
use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;
use risingwave_pb::expr::AggCall as ProstAggCall;

use super::{
    BatchHashAgg, BatchSimpleAgg, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamHashAgg,
    StreamSimpleAgg, ToBatch, ToStream,
};
use crate::expr::{AggCall, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::LogicalProject;
use crate::optimizer::property::WithSchema;
use crate::utils::ColIndexMapping;

/// Aggregation Call
#[derive(Clone)]
pub struct PlanAggCall {
    /// Kind of aggregation function
    pub agg_kind: AggKind,

    /// Data type of the returned column
    pub return_type: DataType,

    /// Column indexes of input columns
    pub inputs: Vec<InputRef>,
}

impl fmt::Debug for PlanAggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_tuple(&format!("{}", self.agg_kind));
        self.inputs.iter().for_each(|child| {
            builder.field(child);
        });
        builder.finish()
    }
}

impl PlanAggCall {
    pub fn to_protobuf(&self) -> ProstAggCall {
        ProstAggCall {
            r#type: self.agg_kind.to_prost().into(),
            return_type: Some(self.return_type.to_protobuf()),
            args: self
                .inputs
                .iter()
                .map(InputRef::to_agg_arg_protobuf)
                .collect(),
            // TODO: support distinct
            distinct: false,
        }
    }
}

/// `LogicalAgg` groups input data by their group keys and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
#[derive(Clone, Debug)]
pub struct LogicalAgg {
    pub base: PlanBase,
    agg_calls: Vec<PlanAggCall>,
    agg_call_alias: Vec<Option<String>>,
    group_keys: Vec<usize>,
    input: PlanRef,
}

/// `ExprHandler` extracts agg calls and references to group columns from select list, in
/// preparation for generating a plan like `LogicalProject - LogicalAgg - LogicalProject`.
struct ExprHandler {
    // `project` contains all the ExprImpl inside GROUP BY clause (e.g. v1 for group by v1)
    // followed by those inside aggregates (e.g. v1 + v2 for min(v1 + v2)).
    pub project: Vec<ExprImpl>,
    group_column_index: HashMap<InputRef, usize>,
    pub agg_calls: Vec<PlanAggCall>,
    pub error: Option<ErrorCode>,
}

impl ExprHandler {
    fn new(group_exprs: Vec<ExprImpl>) -> Result<Self> {
        let mut group_column_index = HashMap::new();
        for (index, expr) in group_exprs.iter().enumerate() {
            // TODO: support more complicated expression in GROUP BY clause, because we currently
            // assume the only thing can appear in GROUP BY clause is an input column name.
            if let ExprImpl::InputRef(input_ref) = expr {
                group_column_index.insert(*input_ref.clone(), index);
            } else {
                return Err(ErrorCode::NotImplementedError(
                    "GROUP BY only supported on input column names!".into(),
                )
                .into());
            }
        }
        Ok(ExprHandler {
            project: group_exprs,
            group_column_index,
            agg_calls: vec![],
            error: None,
        })
    }
}

impl ExprRewriter for ExprHandler {
    // When there is an agg call, there are 3 things to do:
    // 1. eval its inputs via project;
    // 2. add a PlanAggCall to agg;
    // 3. rewrite it as an InputRef to the agg result in select list.
    //
    // Note that the rewriter does not traverse into inputs of agg calls.
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let return_type = agg_call.return_type();
        let (agg_kind, inputs) = agg_call.decompose();

        let begin = self.project.len();
        self.project.extend(inputs);
        let end = self.project.len();

        let inputs = (begin..end)
            .zip_eq(self.project.iter().skip(begin).take(end - begin))
            .map(|(idx, expr)| InputRef::new(idx, expr.return_type()))
            .collect_vec();

        if agg_kind == AggKind::Avg {
            // Rewrite avg to sum/count.
            self.agg_calls.push(PlanAggCall {
                agg_kind: AggKind::Sum,
                return_type: return_type.clone(),
                inputs: inputs.clone(),
            });
            let left = InputRef::new(
                self.group_column_index.len() + self.agg_calls.len() - 1,
                return_type.clone(),
            );

            self.agg_calls.push(PlanAggCall {
                agg_kind: AggKind::Count,
                return_type: DataType::Int64,
                inputs,
            });
            let right = InputRef::new(
                self.group_column_index.len() + self.agg_calls.len() - 1,
                return_type,
            );

            ExprImpl::from(
                FunctionCall::new(ExprType::Divide, vec![left.into(), right.into()]).unwrap(),
            )
        } else {
            self.agg_calls.push(PlanAggCall {
                agg_kind,
                return_type: return_type.clone(),
                inputs,
            });
            ExprImpl::from(InputRef::new(
                self.group_column_index.len() + self.agg_calls.len() - 1,
                return_type,
            ))
        }
    }
    // When there is an InputRef (outside of agg call), it must refers to a group column.
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if let Some(index) = self.group_column_index.get(&input_ref) {
            InputRef::new(*index, input_ref.data_type()).into()
        } else {
            self.error = Some(ErrorCode::InvalidInputSyntax(
                "column must appear in the GROUP BY clause or be used in an aggregate function"
                    .into(),
            ));
            input_ref.into()
        }
    }
}

impl LogicalAgg {
    pub fn new(
        agg_calls: Vec<PlanAggCall>,
        agg_call_alias: Vec<Option<String>>,
        group_keys: Vec<usize>,
        input: PlanRef,
    ) -> Self {
        let ctx = input.ctx();
        let schema = Self::derive_schema(
            input.schema(),
            &group_keys,
            agg_calls
                .iter()
                .map(|agg_call| agg_call.return_type.clone())
                .collect(),
            &agg_call_alias,
        );
        let pk_indices = match group_keys.is_empty() {
            // simple agg
            true => (0..schema.len()).collect(),
            // group agg
            false => group_keys.clone(),
        };
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        Self {
            agg_calls,
            group_keys,
            input,
            base,
            agg_call_alias,
        }
    }

    fn derive_schema(
        input: &Schema,
        group_keys: &[usize],
        agg_call_data_types: Vec<DataType>,
        agg_call_alias: &[Option<String>],
    ) -> Schema {
        let fields = group_keys
            .iter()
            .cloned()
            .map(|i| input.fields()[i].clone())
            .chain(
                agg_call_data_types
                    .into_iter()
                    .zip_eq(agg_call_alias.iter())
                    .enumerate()
                    .map(|(id, (data_type, alias))| {
                        let name = alias.clone().unwrap_or(format!("agg#{}", id));
                        Field { data_type, name }
                    }),
            )
            .collect();
        Schema { fields }
    }

    /// `create` will analyze the select exprs and group exprs, and construct a plan like
    ///
    /// ```text
    /// LogicalProject -> LogicalAgg -> LogicalProject -> input
    /// ```
    pub fn create(
        select_exprs: Vec<ExprImpl>,
        select_alias: Vec<Option<String>>,
        group_exprs: Vec<ExprImpl>,
        input: PlanRef,
    ) -> Result<PlanRef> {
        let group_keys = (0..group_exprs.len()).collect();
        let mut expr_handler = ExprHandler::new(group_exprs)?;

        let rewritten_select_exprs = select_exprs
            .into_iter()
            .map(|expr| {
                let rewritten_expr = expr_handler.rewrite_expr(expr);
                if let Some(error) = expr_handler.error.take() {
                    return Err(error.into());
                }
                Ok(rewritten_expr)
            })
            .collect::<Result<_>>()?;

        // This LogicalProject focuses on the exprs in aggregates and GROUP BY clause.
        let expr_alias = vec![None; expr_handler.project.len()];
        let logical_project = LogicalProject::create(input, expr_handler.project, expr_alias);

        // This LogicalAgg foucuses on calculating the aggregates and grouping.
        let agg_call_alias = vec![None; expr_handler.agg_calls.len()];
        let logical_agg = LogicalAgg::new(
            expr_handler.agg_calls,
            agg_call_alias,
            group_keys,
            logical_project,
        );

        // This LogicalProject focuse on tranforming the aggregates and grouping columns to
        // InputRef.
        Ok(LogicalProject::create(
            logical_agg.into(),
            rewritten_select_exprs,
            select_alias,
        ))
    }

    /// Get a reference to the logical agg's agg call alias.
    pub fn agg_call_alias(&self) -> &[Option<String>] {
        self.agg_call_alias.as_ref()
    }

    /// Get a reference to the logical agg's agg calls.
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.agg_calls.as_ref()
    }

    /// Get a reference to the logical agg's group keys.
    pub fn group_keys(&self) -> &[usize] {
        self.group_keys.as_ref()
    }
}

impl PlanTreeNodeUnary for LogicalAgg {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            self.agg_calls().to_vec(),
            self.agg_call_alias().to_vec(),
            self.group_keys().to_vec(),
            input,
        )
    }
}
impl_plan_tree_node_for_unary! {LogicalAgg}

impl fmt::Display for LogicalAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalAgg")
            .field("group_keys", &self.group_keys)
            .field("agg_calls", &self.agg_calls)
            .finish()
    }
}

impl ColPrunable for LogicalAgg {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let mut child_required_cols =
            FixedBitSet::with_capacity(self.input.schema().fields().len());
        child_required_cols.extend(self.group_keys.iter().cloned());

        // Do not prune the group keys.
        let mut group_keys = self.group_keys.clone();
        let (mut agg_calls, agg_call_alias): (Vec<_>, Vec<_>) = required_cols
            .ones()
            .filter(|&index| index >= self.group_keys.len())
            .map(|index| {
                let index = index - self.group_keys.len();
                let agg_call = self.agg_calls[index].clone();
                child_required_cols.extend(agg_call.inputs.iter().map(|x| x.index()));
                (agg_call, self.agg_call_alias[index].clone())
            })
            .multiunzip();

        let mapping = ColIndexMapping::with_remaining_columns(&child_required_cols);
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call
                .inputs
                .iter_mut()
                .for_each(|i| *i = InputRef::new(mapping.map(i.index()), i.return_type()));
        });
        group_keys.iter_mut().for_each(|i| *i = mapping.map(*i));

        let agg = LogicalAgg::new(
            agg_calls,
            agg_call_alias,
            group_keys,
            self.input.prune_col(&child_required_cols),
        );

        if FixedBitSet::from_iter(0..self.group_keys.len()).is_subset(required_cols) {
            agg.into()
        } else {
            // Some group key columns are not needed
            let mut required_cols_new = FixedBitSet::with_capacity(agg.schema().fields().len());
            required_cols
                .ones()
                .filter(|&i| i < agg.group_keys.len())
                .for_each(|i| required_cols_new.insert(i));
            required_cols_new.extend(agg.group_keys.len()..agg.schema().fields().len());
            LogicalProject::with_mapping(
                agg.into(),
                ColIndexMapping::with_remaining_columns(&required_cols_new),
            )
            .into()
        }
    }
}

impl ToBatch for LogicalAgg {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        if self.group_keys().is_empty() {
            BatchSimpleAgg::new(new_logical).into()
        } else {
            BatchHashAgg::new(new_logical).into()
        }
    }
}

impl ToStream for LogicalAgg {
    fn to_stream(&self) -> PlanRef {
        let new_input = self.input().to_stream();
        let new_logical = self.clone_with_input(new_input);
        if self.group_keys().is_empty() {
            StreamSimpleAgg::new(new_logical).into()
        } else {
            StreamHashAgg::new(new_logical).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{
        assert_eq_input_ref, input_ref_to_column_indices, AggCall, ExprType, FunctionCall,
    };
    use crate::optimizer::plan_node::LogicalScan;
    use crate::optimizer::property::ctx::WithId;
    use crate::session::QueryContext;

    #[tokio::test]
    async fn test_create() {
        let ty = DataType::Int32;
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema { fields },
            ctx,
        );
        let input = Rc::new(table_scan);
        let input_ref_1 = InputRef::new(0, ty.clone());
        let input_ref_2 = InputRef::new(1, ty.clone());
        let input_ref_3 = InputRef::new(2, ty.clone());

        let gen_internal_value = |select_exprs: Vec<ExprImpl>,
                                  group_exprs|
         -> (Vec<ExprImpl>, Vec<PlanAggCall>, Vec<usize>) {
            let select_alias = vec![None; select_exprs.len()];
            let plan =
                LogicalAgg::create(select_exprs, select_alias, group_exprs, input.clone()).unwrap();
            let logical_project = plan.as_logical_project().unwrap();
            let exprs = logical_project.exprs();

            let plan = logical_project.input();
            let logical_agg = plan.as_logical_agg().unwrap();
            let agg_calls = logical_agg.agg_calls().to_vec();
            let group_keys = logical_agg.group_keys().to_vec();

            (exprs.clone(), agg_calls, group_keys)
        };

        // Test case: select v1 from test group by v1;
        {
            let select_exprs = vec![input_ref_1.clone().into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_keys) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 1);
            assert_eq_input_ref!(&exprs[0], 0);

            assert_eq!(agg_calls.len(), 0);
            assert_eq!(group_keys, vec![0]);
        }

        // Test case: select v1, min(v2) from test group by v1;
        {
            let min_v2 = AggCall::new(AggKind::Min, vec![input_ref_2.clone().into()]).unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), min_v2.into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_keys) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 2);
            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_keys, vec![0]);
        }

        // Test case: select v1, min(v2) + max(v3) from t group by v1;
        {
            let min_v2 = AggCall::new(AggKind::Min, vec![input_ref_2.clone().into()]).unwrap();
            let max_v3 = AggCall::new(AggKind::Max, vec![input_ref_3.clone().into()]).unwrap();
            let func_call =
                FunctionCall::new(ExprType::Add, vec![min_v2.into(), max_v3.into()]).unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), ExprImpl::from(func_call)];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_keys) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            if let ExprImpl::FunctionCall(func_call) = &exprs[1] {
                assert_eq!(func_call.get_expr_type(), ExprType::Add);
                let inputs = func_call.inputs();
                assert_eq_input_ref!(&inputs[0], 1);
                assert_eq_input_ref!(&inputs[1], 2);
            } else {
                panic!("Wrong expression type!");
            }

            assert_eq!(agg_calls.len(), 2);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(agg_calls[1].agg_kind, AggKind::Max);
            assert_eq!(input_ref_to_column_indices(&agg_calls[1].inputs), vec![2]);
            assert_eq!(group_keys, vec![0]);
        }

        // Test case: select v2, min(v1 * v3) from test group by v2;
        {
            let v1_mult_v3 = FunctionCall::new(
                ExprType::Multiply,
                vec![input_ref_1.into(), input_ref_3.into()],
            )
            .unwrap();
            let agg_call = AggCall::new(AggKind::Min, vec![v1_mult_v3.into()]).unwrap();
            let select_exprs = vec![input_ref_2.clone().into(), agg_call.into()];
            let group_exprs = vec![input_ref_2.into()];

            let (exprs, agg_calls, group_keys) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_keys, vec![0]);
        }
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,1] (all columns) will result in
    /// ```text
    /// Agg(max(input_ref(1))) group by (input_ref(0))
    ///  TableScan(v2, v3)
    async fn test_prune_all() {
        let ty = DataType::Int32;
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let agg_call = PlanAggCall {
            agg_kind: AggKind::Min,
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty.clone())],
        };
        let agg = LogicalAgg::new(
            vec![agg_call],
            vec![Some("min".to_string())],
            vec![1],
            table_scan.into(),
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(2);
        required_cols.extend(vec![0, 1]);
        let plan = agg.prune_col(&required_cols);

        // Check the result
        let agg_new = plan.as_logical_agg().unwrap();
        assert_eq!(agg_new.agg_call_alias(), vec![Some("min".to_string())]);
        assert_eq!(agg_new.group_keys(), vec![0]);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Min);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let scan = agg_new.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1] (group key removed) will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Agg(max(input_ref(1))) group by (input_ref(0))
    ///     TableScan(v2, v3)
    async fn test_prune_group_key() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let agg_call = PlanAggCall {
            agg_kind: AggKind::Min,
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty.clone())],
        };
        let agg = LogicalAgg::new(
            vec![agg_call],
            vec![Some("min".to_string())],
            vec![1],
            table_scan.into(),
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(2);
        required_cols.extend(vec![1]);
        let plan = agg.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq!(project.id().0, 4);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.agg_call_alias(), vec![Some("min".to_string())]);
        assert_eq!(agg_new.group_keys(), vec![0]);
        assert_eq!(agg_new.id().0, 3);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Min);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let scan = agg_new.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields(), &fields[1..]);
        assert_eq!(scan.id().0, 2);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2)), max(input_ref(1))) group by (input_ref(1), input_ref(2))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,3] will result in
    /// ```text
    /// Project(input_ref(0), input_ref(2))
    ///   Agg(max(input_ref(0))) group by (input_ref(0), input_ref(1))
    ///     TableScan(v2, v3)
    async fn test_prune_agg() {
        let ty = DataType::Int32;
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let agg_calls = vec![
            PlanAggCall {
                agg_kind: AggKind::Min,
                return_type: ty.clone(),
                inputs: vec![InputRef::new(2, ty.clone())],
            },
            PlanAggCall {
                agg_kind: AggKind::Max,
                return_type: ty.clone(),
                inputs: vec![InputRef::new(1, ty.clone())],
            },
        ];
        let agg = LogicalAgg::new(
            agg_calls,
            vec![Some("min".to_string()), Some("max".to_string())],
            vec![1, 2],
            table_scan.into(),
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(4);
        required_cols.extend(vec![0, 3]);
        let plan = agg.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 0);
        assert_eq_input_ref!(&project.exprs()[1], 2);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.agg_call_alias(), vec![Some("max".to_string())]);
        assert_eq!(agg_new.group_keys(), vec![0, 1]);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Max);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![0]);
        assert_eq!(agg_call_new.return_type, ty);

        let scan = agg_new.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields(), &fields[1..]);
    }
}
