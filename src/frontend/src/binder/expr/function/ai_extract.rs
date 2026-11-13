// Copyright 2026 RisingWave Labs
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

use risingwave_common::types::{DataType, Datum, JsonbVal, MapType, ScalarImpl};
use risingwave_expr::scalar::ai_extract::ExtractionSchema;

use crate::error::{ErrorCode, Result};
use crate::expr::{ExprImpl, ExprType, FunctionCall, Literal};

fn invalid(reason: impl Into<String>) -> crate::error::RwError {
    ErrorCode::InvalidInputSyntax(reason.into()).into()
}

fn constant(expr: &ExprImpl, argument: &str) -> Result<Datum> {
    expr.try_fold_const()
        .ok_or_else(|| invalid(format!("{argument} must be constant")))?
}

fn json_constant(expr: &ExprImpl, argument: &str) -> Result<JsonbVal> {
    match constant(expr, argument)? {
        Some(ScalarImpl::Jsonb(value)) => Ok(value),
        _ => Err(invalid(format!("{argument} must be non-null JSONB"))),
    }
}

pub(super) fn bind(mut inputs: Vec<ExprImpl>) -> Result<ExprImpl> {
    if !matches!(inputs.len(), 5 | 6) {
        return Err(invalid("ai_extract expects five or six arguments"));
    }
    for (index, ty) in [
        DataType::Varchar,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Jsonb,
        DataType::Varchar,
    ]
    .into_iter()
    .enumerate()
    {
        inputs[index].cast_implicit_mut(&ty)?;
    }
    if let Some(headers) = inputs.get_mut(5) {
        headers
            .cast_implicit_mut(&MapType::from_kv(DataType::Varchar, DataType::Varchar).into())?;
    }
    let specification = json_constant(&inputs[3], "ai_extract specification")?;
    let schema = ExtractionSchema::parse(&specification).map_err(|e| invalid(e.to_string()))?;
    // Ship the validated constant to the executor, not an unevaluated expression.
    inputs[3] = Literal::new(Some(specification.into()), DataType::Jsonb).into();
    Ok(FunctionCall::new_unchecked(ExprType::AiExtract, inputs, schema.return_type()).into())
}
