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

use std::rc::Rc;

use anyhow::{anyhow, bail};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, TableFunctionType};
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::{Logical, LogicalGetFragmentVnodes, LogicalTableFunction};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

fn expr_impl_to_u32(arg: &ExprImpl) -> anyhow::Result<u32> {
    match arg
        .clone()
        .cast_implicit(&DataType::Int32)?
        .try_fold_const()
    {
        Some(Ok(Some(scalar))) => match scalar {
            ScalarImpl::Int32(value) => {
                if value < 0 {
                    bail!("fragment_id cannot be negative: {value}");
                }
                Ok(value as u32)
            }
            other => Err(anyhow!(
                "expected fragment_id to be int32 constant, got {other:?}"
            )),
        },
        Some(Ok(None)) => bail!("fragment_id cannot be NULL"),
        Some(Err(err)) => Err(anyhow!(err).context("failed to fold constant")),
        None => bail!("fragment_id must be a constant expression"),
    }
}

/// Transform the `internal_get_fragment_vnodes()` table function into a plan node that fetches
/// fragment vnode assignments from meta.
pub struct TableFunctionToInternalGetFragmentVnodesRule {}

impl FallibleRule<Logical> for TableFunctionToInternalGetFragmentVnodesRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalGetFragmentVnodes
        {
            return ApplyResult::NotApplicable;
        }

        let plan = Self::build_plan(plan.ctx(), &logical_table_function.table_function)?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalGetFragmentVnodesRule {
    fn build_plan(
        ctx: Rc<OptimizerContext>,
        table_function: &crate::expr::TableFunction,
    ) -> anyhow::Result<PlanRef> {
        if table_function.args.len() != 1 {
            bail!("internal_get_fragment_vnodes expects exactly 1 argument");
        }
        let fragment_id = expr_impl_to_u32(&table_function.args[0])?;

        let DataType::Struct(struct_type) = &table_function.return_type else {
            bail!("internal_get_fragment_vnodes must return a struct type");
        };

        let fields = struct_type
            .iter()
            .map(|(name, ty)| Field::new(name.to_owned(), ty.clone()))
            .collect();
        let schema = Schema::new(fields);

        let plan = LogicalGetFragmentVnodes::new(ctx, schema, fragment_id);
        Ok(plan.into())
    }

    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalGetFragmentVnodesRule {})
    }
}
