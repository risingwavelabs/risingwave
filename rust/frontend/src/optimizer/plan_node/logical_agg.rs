use std::fmt;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;

use super::{ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::expr::{AggCall, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::LogicalProject;
use crate::optimizer::property::WithSchema;
use crate::utils::ColIndexMapping;

/// Aggregation Call
#[derive(Clone, Debug)]
pub struct PlanAggCall {
    /// Kind of aggregation function
    pub agg_kind: AggKind,

    /// Data type of the returned column
    pub return_type: DataType,

    /// Column indexes of input columns
    pub inputs: Vec<usize>,
}

/// `LogicalAgg` groups input data by their group keys and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
#[derive(Clone, Debug)]
pub struct LogicalAgg {
    pub base: LogicalBase,
    agg_calls: Vec<PlanAggCall>,
    agg_call_alias: Vec<Option<String>>,
    group_keys: Vec<usize>,
    input: PlanRef,
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
        let base = LogicalBase {
            schema,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
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
    #[allow(unused_variables)]
    pub fn create(
        select_exprs: Vec<ExprImpl>,
        select_alias: Vec<Option<String>>,
        group_exprs: Vec<ExprImpl>,
        input: PlanRef,
    ) -> PlanRef {
        // Notice: we now assume the only thing can appear in GROUP BY clause is an input column
        // name.
        let mut project: Vec<ExprImpl> = vec![];
        let mut group_keys = vec![];
        let input_refs =
            LogicalAgg::traverse_group_expr(group_exprs, &mut project, &mut group_keys);

        let mut agg_calls = vec![];
        LogicalAgg::traverse_select_expr(&select_exprs, &mut project, &mut agg_calls, &input_refs);

        // This LogicalProject focuses on the exprs in aggregates and GROUP BY clause.
        let expr_alias = vec![None; project.len()];
        let logical_project = LogicalProject::create(input, project.clone(), expr_alias.clone());

        // Assume the output order of LogicalAgg will be [group column, aggregate result], same as
        // the order of `project`.
        // This LogicalAgg foucuses on calculating the aggregates and grouping.
        let logical_agg = LogicalAgg::new(agg_calls, expr_alias, group_keys, logical_project);

        let mut rewrite_agg = RewriteAgg {
            index: input_refs.len(),
            input_refs,
        };
        // We should consider the cases like `select 1, v1, min(v2) from t group by v1`, in wthich
        // `1` wouldn't get into `project`.
        let exprs = select_exprs
            .into_iter()
            .map(|expr| rewrite_agg.rewrite_expr(expr))
            .collect();
        // This LogicalProject focuse on tranforming the aggregates and grouping columns to
        // InputRef.
        LogicalProject::create(Rc::new(logical_agg), exprs, select_alias)
    }

    fn traverse_group_expr(
        group_exprs: Vec<ExprImpl>,
        project: &mut Vec<ExprImpl>,
        group_keys: &mut Vec<usize>,
    ) -> Vec<InputRef> {
        let mut input_refs = vec![];
        for (index, group_expr) in group_exprs.into_iter().enumerate() {
            match group_expr {
                ExprImpl::InputRef(input_ref) => {
                    project.push((*input_ref).clone().into());
                    group_keys.push(index);
                    input_refs.push(*input_ref);
                }
                _ => {
                    panic!("GROUP BY clause cannot contain aggregates!");
                }
            }
        }
        input_refs
    }

    fn traverse_select_expr(
        select_exprs: &[ExprImpl],
        project: &mut Vec<ExprImpl>,
        agg_calls: &mut Vec<PlanAggCall>,
        input_refs: &Vec<InputRef>,
    ) {
        for select_expr in select_exprs {
            match select_expr {
                ExprImpl::InputRef(input_ref) => {
                    if !input_refs.iter().any(|input| *input == **input_ref) {
                        panic!("column must appear in the GROUP BY clause or be used in an aggregate function");
                    }
                }
                ExprImpl::Literal(_) => {}
                ExprImpl::FunctionCall(func_call) => {
                    LogicalAgg::traverse_select_expr(
                        func_call.inputs(),
                        project,
                        agg_calls,
                        input_refs,
                    );
                }
                ExprImpl::AggCall(agg_call) => {
                    let begin = project.len();
                    let end = begin + agg_call.inputs.len();
                    project.extend(agg_call.inputs.clone().into_iter());
                    agg_calls.push(PlanAggCall {
                        agg_kind: agg_call.agg_kind.clone(),
                        return_type: agg_call.return_type.clone(),
                        inputs: (begin..end).collect(),
                    });
                }
            }
        }
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

struct RewriteAgg {
    index: usize,
    input_refs: Vec<InputRef>,
}
// May or may not: the order between match arms affects the final result in current implementation.
impl ExprRewriter for RewriteAgg {
    // Agg -> InputRef (select min(v1))
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let input_ref = InputRef::new(self.index, agg_call.return_type).into();
        self.index += 1;
        input_ref
    }
    // InputRef -> InputRef(select v1 from t group by v1)
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if let Some(index) = self.input_refs.iter().position(|input| *input == input_ref) {
            InputRef::new(index, input_ref.data_type()).into()
        } else {
            panic!("unreachable");
        }
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
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
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
                child_required_cols.extend(agg_call.inputs.iter().copied());
                (agg_call, self.agg_call_alias[index].clone())
            })
            .multiunzip();

        let mapping = ColIndexMapping::with_remaining_columns(&child_required_cols);
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call
                .inputs
                .iter_mut()
                .for_each(|i| *i = mapping.map(*i));
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
        todo!()
    }
}

impl ToStream for LogicalAgg {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::assert_eq_input_ref;
    use crate::optimizer::plan_node::LogicalScan;
    use crate::optimizer::property::ctx::WithId;
    use crate::session::QueryContext;

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
            inputs: vec![2],
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
        assert_eq!(agg_call_new.inputs, vec![1]);
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
            inputs: vec![2],
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
        assert_eq!(agg_call_new.inputs, vec![1]);
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
                inputs: vec![2],
            },
            PlanAggCall {
                agg_kind: AggKind::Max,
                return_type: ty.clone(),
                inputs: vec![1],
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
        assert_eq!(agg_call_new.inputs, vec![0]);
        assert_eq!(agg_call_new.return_type, ty);

        let scan = agg_new.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields(), &fields[1..]);
    }
}
