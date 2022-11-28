
use std::fmt;


use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};









use super::{GenericPlanNode, GenericPlanRef};


use crate::expr::{Expr, ExprDisplay, ExprImpl};

use crate::session::OptimizerContextRef;

use crate::utils::{ColIndexMapping};


/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Project<PlanRef> {
    fn schema(&self) -> Schema {
        let o2i = self.o2i_col_mapping();
        let exprs = &self.exprs;
        let input_schema = self.input.schema();
        let fields = exprs
            .iter()
            .enumerate()
            .map(|(id, expr)| {
                // Get field info from o2i.
                let (name, sub_fields, type_name) = match o2i.try_map(id) {
                    Some(input_idx) => {
                        let field = input_schema.fields()[input_idx].clone();
                        (field.name, field.sub_fields, field.type_name)
                    }
                    None => (
                        format!("{:?}", ExprDisplay { expr, input_schema }),
                        vec![],
                        String::new(),
                    ),
                };
                Field::with_struct(expr.return_type(), name, sub_fields, type_name)
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        self.input
            .logical_pk()
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> Project<PlanRef> {
    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        Project { exprs, input }
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        builder.field(
            "exprs",
            &self
                .exprs
                .iter()
                .map(|expr| ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                })
                .collect_vec(),
        );
        builder.finish()
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let exprs = &self.exprs;
        let input_len = self.input.schema().len();
        let mut map = vec![None; exprs.len()];
        for (i, expr) in exprs.iter().enumerate() {
            map[i] = match expr {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }
}
