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

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::binder::BoundFillStrategy;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::ColIndexMapping;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GapFill<PlanRef> {
    pub input: PlanRef,
    pub time_col: InputRef,
    pub interval: ExprImpl,
    pub fill_strategies: Vec<BoundFillStrategy>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for GapFill<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.input.stream_key().map(|key| key.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> crate::optimizer::plan_node::expr_visitable::ExprVisitable
    for GapFill<PlanRef>
{
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        v.visit_expr(&self.time_col.clone().into());
        v.visit_expr(&self.interval);
    }
}

impl<PlanRef> GapFill<PlanRef> {
    pub fn rewrite_with_col_index_mapping(&mut self, mapping: &mut ColIndexMapping) {
        let expr: ExprImpl = self.time_col.clone().into();
        self.time_col = mapping
            .rewrite_expr(expr)
            .as_input_ref()
            .expect("time_col must be an InputRef after rewrite")
            .as_ref()
            .clone();

        self.fill_strategies.iter_mut().for_each(|s| {
            let expr: ExprImpl = s.target_col.clone().into();
            s.target_col = mapping
                .rewrite_expr(expr)
                .as_input_ref()
                .expect("target_col must be an InputRef after rewrite")
                .as_ref()
                .clone();
        });
        self.interval = mapping.rewrite_expr(self.interval.clone());
    }

    pub fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.interval = r.rewrite_expr(self.interval.clone());

        let expr: ExprImpl = self.time_col.clone().into();
        let rewritten = r.rewrite_expr(expr);
        self.time_col = rewritten
            .as_input_ref()
            .expect("time_col must be an InputRef after rewrite")
            .as_ref()
            .clone();

        self.fill_strategies.iter_mut().for_each(|s| {
            let expr: ExprImpl = s.target_col.clone().into();
            let rewritten = r.rewrite_expr(expr);
            s.target_col = rewritten
                .as_input_ref()
                .expect("target_col must be an InputRef after rewrite")
                .as_ref()
                .clone();
        });
    }
}

impl<PlanRef: GenericPlanRef> DistillUnit for GapFill<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let mut fields = Vec::new();
        fields.push(("time_col", Pretty::debug(&self.time_col)));
        fields.push(("interval", Pretty::debug(&self.interval)));
        fields.push(("fill_strategies", Pretty::debug(&self.fill_strategies)));
        childless_record(name, fields)
    }
}
