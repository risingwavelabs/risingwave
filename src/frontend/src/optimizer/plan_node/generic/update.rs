// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::Hash;

use educe::Educe;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema, TableVersionId};
use risingwave_common::types::DataType;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Update<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub old_exprs: Vec<ExprImpl>,
    pub new_exprs: Vec<ExprImpl>,
    pub returning: bool,
}

impl<PlanRef: GenericPlanRef> Update<PlanRef> {
    pub fn output_len(&self) -> usize {
        if self.returning {
            self.new_exprs.len()
        } else {
            1
        }
    }
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for Update<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.output_len())
    }

    fn schema(&self) -> Schema {
        if self.returning {
            Schema::new(
                self.new_exprs
                    .iter()
                    .map(|e| Field::unnamed(e.return_type()))
                    .collect(),
            )
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: Eq + Hash> Update<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        old_exprs: Vec<ExprImpl>,
        new_exprs: Vec<ExprImpl>,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            old_exprs,
            new_exprs,
            returning,
        }
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        for exprs in [&mut self.old_exprs, &mut self.new_exprs] {
            *exprs = exprs.iter().map(|e| r.rewrite_expr(e.clone())).collect();
        }
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        for exprs in [&self.old_exprs, &self.new_exprs] {
            exprs.iter().for_each(|e| v.visit_expr(e));
        }
    }
}

impl<PlanRef: Eq + Hash> DistillUnit for Update<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let mut vec = Vec::with_capacity(if self.returning { 3 } else { 2 });
        vec.push(("table", Pretty::from(self.table_name.clone())));
        vec.push(("exprs", Pretty::debug(&self.new_exprs)));
        if self.returning {
            vec.push(("returning", Pretty::display(&true)));
        }
        childless_record(name, vec)
    }
}
