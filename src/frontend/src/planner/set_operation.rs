use itertools::Itertools;
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
use risingwave_common::error::{ErrorCode, Result};

use crate::binder::{BoundSetExpr, BoundSetOperation};
use crate::optimizer::plan_node::LogicalUnion;
use crate::planner::Planner;
use crate::PlanRef;

impl Planner {
    pub(super) fn plan_set_operation(
        &mut self,
        op: BoundSetOperation,
        all: bool,
        left: BoundSetExpr,
        right: BoundSetExpr,
    ) -> Result<PlanRef> {
        match op {
            BoundSetOperation::UNION => {
                let left = self.plan_set_expr(left, vec![])?;
                let right = self.plan_set_expr(right, vec![])?;

                if left.schema().fields.len() != right.schema().fields.len() {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "each UNION query must have the same number of columns".to_string(),
                    )
                    .into());
                }

                for (a, b) in left
                    .schema()
                    .fields
                    .iter()
                    .zip_eq(right.schema().fields.iter())
                {
                    if a.data_type != b.data_type {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "UNION types {} of column {} is different from types {} of column {}",
                            a.data_type.prost_type_name().as_str_name(),
                            a.name,
                            b.data_type.prost_type_name().as_str_name(),
                            b.name,
                        ))
                        .into());
                    }
                }

                Ok(LogicalUnion::create(all, vec![left, right]))
            }
            BoundSetOperation::Except | BoundSetOperation::Intersect => {
                Err(ErrorCode::NotImplemented(format!("set expr: {:?}", op), None.into()).into())
            }
        }
    }
}
