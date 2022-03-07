// NOTICE: `Substitute` is adapted from the [RisingLight](https://github.com/risinglightdb/risinglight/blob/53ccdf96f36267c4703386577eeeff1da18bdf13/src/optimizer/plan_nodes/logical_projection.rs) project.

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use log::debug;
use risingwave_common::catalog::{Field, Schema};

use super::{
    BatchProject, ColPrunable, PlanRef, PlanTreeNodeUnary, StreamProject, ToBatch, ToStream,
};
use crate::expr::{assert_input_ref, Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::CollectRequiredCols;
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};
use crate::utils::ColIndexMapping;

#[derive(Debug, Clone)]
pub struct LogicalProject {
    exprs: Vec<ExprImpl>,
    expr_alias: Vec<Option<String>>,
    input: PlanRef,
    schema: Schema,
}

impl LogicalProject {
    fn new(input: PlanRef, exprs: Vec<ExprImpl>, expr_alias: Vec<Option<String>>) -> Self {
        // Merge contiguous Project nodes.
        if let Some(input) = input.as_logical_project() {
            let mut subst = Substitute {
                mapping: input.exprs.clone(),
            };
            let exprs = exprs
                .iter()
                .cloned()
                .map(|expr| subst.rewrite_expr(expr))
                .collect();
            return LogicalProject::new(input.input(), exprs, expr_alias);
        }

        let schema = Self::derive_schema(&exprs, &expr_alias);
        for expr in &exprs {
            assert_input_ref(expr, input.schema().fields().len());
        }
        LogicalProject {
            input,
            schema,
            exprs,
            expr_alias,
        }
    }

    pub fn create(
        input: PlanRef,
        exprs: Vec<ExprImpl>,
        expr_alias: Vec<Option<String>>,
    ) -> PlanRef {
        Self::new(input, exprs, expr_alias).into()
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        debug!("with_mapping {:?}", mapping);
        assert_eq!(
            input.schema().fields().len(),
            mapping.source_upper() + 1,
            "invalid mapping given"
        );
        let mut input_refs = vec![None; mapping.target_upper() + 1];
        for (src, tar) in mapping.mapping_pairs() {
            assert_eq!(input_refs[tar], None);
            input_refs[tar] = Some(src);
        }
        let input_schema = input.schema();
        let exprs: Vec<ExprImpl> = input_refs
            .into_iter()
            .map(|i| i.unwrap())
            .map(|i| InputRef::new(i, input_schema.fields()[i].data_type()).into())
            .collect();

        let alias = vec![None; exprs.len()];
        LogicalProject::new(input, exprs, alias)
    }

    fn derive_schema(exprs: &[ExprImpl], expr_alias: &[Option<String>]) -> Schema {
        let fields = exprs
            .iter()
            .zip_eq(expr_alias.iter())
            .enumerate()
            .map(|(id, (expr, alias))| {
                let name = alias.clone().unwrap_or(format!("expr#{}", id));
                Field {
                    name,
                    data_type: expr.return_type(),
                }
            })
            .collect();
        Schema { fields }
    }
    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.exprs
    }

    /// Get a reference to the logical project's expr alias.
    pub fn expr_alias(&self) -> &[Option<String>] {
        self.expr_alias.as_ref()
    }
}

impl PlanTreeNodeUnary for LogicalProject {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.exprs.clone(), self.expr_alias().to_vec())
    }
}

impl_plan_tree_node_for_unary! {LogicalProject}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalProject")
            .field("exprs", self.exprs())
            .field("expr_alias", &format_args!("{:?}", self.expr_alias()))
            .finish()
    }
}

impl WithOrder for LogicalProject {}

impl WithDistribution for LogicalProject {}

impl WithSchema for LogicalProject {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ColPrunable for LogicalProject {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );

        let mut visitor = CollectRequiredCols {
            required_cols: FixedBitSet::with_capacity(self.input.schema().fields().len()),
        };
        required_cols.ones().for_each(|id| {
            visitor.visit_expr(&self.exprs[id]);
        });

        let child_required_cols = visitor.required_cols;
        let mut mapping = ColIndexMapping::with_remaining_columns(&child_required_cols);

        let (exprs, expr_alias) = required_cols
            .ones()
            .map(|id| {
                (
                    mapping.rewrite_expr(self.exprs[id].clone()),
                    self.expr_alias[id].clone(),
                )
            })
            .unzip();
        LogicalProject::new(
            self.input.prune_col(&child_required_cols),
            exprs,
            expr_alias,
        )
        .into()
    }
}

impl ToBatch for LogicalProject {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        BatchProject::new(new_logical).into()
    }
}

impl ToStream for LogicalProject {
    fn to_stream_with_dist_required(&self, required_dist: &Distribution) -> PlanRef {
        let new_input = self.input().to_stream_with_dist_required(required_dist);
        let new_logical = self.clone_with_input(new_input);
        StreamProject::new(new_logical).into()
    }
    fn to_stream(&self) -> PlanRef {
        self.to_stream_with_dist_required(Distribution::any())
    }
}

/// Substitute `InputRef` with corresponding `ExprImpl`.
struct Substitute {
    mapping: Vec<ExprImpl>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        assert_eq!(
            self.mapping[input_ref.index()].return_type(),
            input_ref.return_type(),
            "Type mismatch when substituting {:?} with {:?}",
            input_ref,
            self.mapping[input_ref.index()],
        );
        self.mapping[input_ref.index()].clone()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::LogicalScan;

    #[test]
    fn test_contiguous_project() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = (1..4)
            .map(|i| Field {
                data_type: ty.clone(),
                name: format!("v{}", i),
            })
            .collect();
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
        );
        let inner = LogicalProject::new(
            table_scan.into(),
            vec![
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(1, ty.clone()).into(),
                        InputRef::new(2, ty.clone()).into(),
                    ],
                )
                .unwrap()
                .into(),
                InputRef::new(0, ty.clone()).into(),
            ],
            vec![Some("aa".to_string()), Some("bb".to_string())],
        );

        let outer = LogicalProject::new(
            inner.into(),
            vec![
                InputRef::new(1, ty.clone()).into(),
                Literal::new(None, ty.clone()).into(),
                InputRef::new(0, DataType::Boolean).into(),
            ],
            vec![None;3],
        );

        assert!(outer.input().as_logical_scan().is_some());
        assert_eq!(outer.exprs().len(), 3);
        assert_eq_input_ref!(&outer.exprs()[0], 0);
        match outer.exprs()[2].clone() {
            ExprImpl::FunctionCall(call) => {
                assert_eq_input_ref!(&call.inputs()[0], 1);
                assert_eq_input_ref!(&call.inputs()[1], 2);
            }
            _ => panic!("Expected function call"),
        }

        let outermost = LogicalProject::new(
            outer.into(),
            vec![InputRef::new(0, ty.clone()).into()],
            vec![None],
        );

        assert!(outermost.input().as_logical_scan().is_some());
        assert_eq!(outermost.exprs().len(), 1);
        assert_eq_input_ref!(&outermost.exprs()[0], 0);
    }

    #[test]
    /// Pruning
    /// ```text
    /// Project(1, input_ref(2), input_ref(0)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns `[1, 2]` will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0)<5)
    ///   TableScan(v1, v3)
    /// ```
    fn test_prune_project() {
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
        );
        let project = LogicalProject::new(
            table_scan.into(),
            vec![
                ExprImpl::Literal(Box::new(Literal::new(None, ty.clone()))),
                InputRef::new(2, ty.clone()).into(),
                ExprImpl::FunctionCall(Box::new(
                    FunctionCall::new(
                        Type::LessThan,
                        vec![
                            ExprImpl::InputRef(Box::new(InputRef::new(0, ty.clone()))),
                            ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                        ],
                    )
                    .unwrap(),
                )),
            ],
            vec![None; 3],
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(3);
        required_cols.insert(1);
        required_cols.insert(2);
        let plan = project.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        match project.exprs()[1].clone() {
            ExprImpl::FunctionCall(call) => assert_eq_input_ref!(&call.inputs()[0], 0),
            _ => panic!("Expected function call"),
        }

        let scan = project.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields().len(), 2);
        assert_eq!(scan.schema().fields()[0], fields[0]);
        assert_eq!(scan.schema().fields()[1], fields[2]);
    }
}
