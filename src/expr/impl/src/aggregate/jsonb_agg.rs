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

use risingwave_common::types::{Datum, JsonbVal};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, aggregate};

use crate::scalar::ToJsonb;

/// Collects all the input values, including nulls, into a JSON array.
/// Values are converted to JSON as per `to_jsonb`.
#[aggregate("jsonb_agg(*) -> jsonb")]
fn jsonb_agg(
    state: &mut JsonbArrayState,
    input: Option<impl ToJsonb>,
    ctx: &Context,
) -> Result<()> {
    input.add_to(&ctx.arg_types[0], &mut state.0)?;
    Ok(())
}

/// Collects all the key/value pairs into a JSON object.
/// // Key arguments are coerced to text;
/// value arguments are converted as per `to_jsonb`.
/// Values can be null, but keys cannot.
#[aggregate("jsonb_object_agg(varchar, *) -> jsonb")]
fn jsonb_object_agg(
    state: &mut JsonbObjectState,
    key: Option<&str>,
    value: Option<impl ToJsonb>,
    ctx: &Context,
) -> Result<()> {
    let key = key.ok_or(ExprError::FieldNameNull)?;
    state.0.add_string(key);
    value.add_to(&ctx.arg_types[1], &mut state.0)?;
    Ok(())
}

#[derive(Debug)]
struct JsonbArrayState(jsonbb::Builder);

impl EstimateSize for JsonbArrayState {
    fn estimated_heap_size(&self) -> usize {
        self.0.capacity()
    }
}

impl AggStateDyn for JsonbArrayState {}

/// Creates an initial state.
impl Default for JsonbArrayState {
    fn default() -> Self {
        let mut builder = jsonbb::Builder::default();
        builder.begin_array();
        Self(builder)
    }
}

/// Finishes aggregation and returns the result.
impl From<&JsonbArrayState> for Datum {
    fn from(builder: &JsonbArrayState) -> Self {
        // TODO: avoid clone
        let mut builder = builder.0.clone();
        builder.end_array();
        let jsonb: JsonbVal = builder.finish().into();
        Some(jsonb.into())
    }
}

#[derive(Debug)]
struct JsonbObjectState(jsonbb::Builder);

impl EstimateSize for JsonbObjectState {
    fn estimated_heap_size(&self) -> usize {
        self.0.capacity()
    }
}

impl AggStateDyn for JsonbObjectState {}

/// Creates an initial state.
impl Default for JsonbObjectState {
    fn default() -> Self {
        let mut builder = jsonbb::Builder::default();
        builder.begin_object();
        Self(builder)
    }
}

/// Finishes aggregation and returns the result.
impl From<&JsonbObjectState> for Datum {
    fn from(builder: &JsonbObjectState) -> Self {
        // TODO: avoid clone
        let mut builder = builder.0.clone();
        builder.end_object();
        let jsonb: JsonbVal = builder.finish().into();
        Some(jsonb.into())
    }
}
