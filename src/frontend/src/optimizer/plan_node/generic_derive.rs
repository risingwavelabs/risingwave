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

use risingwave_common::catalog::{Field, Schema};

use super::generic::*;
use crate::expr::{Expr, ExprDisplay};
use crate::session::OptimizerContextRef;

impl<PlanRef: GenericPlanRef> GenericBase for Project<PlanRef> {
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

    fn logical_pk(&self) -> Vec<usize> {
        let i2o = self.i2o_col_mapping();
        self.input
            .logical_pk()
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Agg<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for HopWindow<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Join<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl GenericBase for Scan {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for TopN<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl GenericBase for Source {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for ProjectSet<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Expand<PlanRef> {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn logical_pk(&self) -> Vec<usize> {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}
