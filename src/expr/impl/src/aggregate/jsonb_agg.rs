// Copyright 2023 RisingWave Labs
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

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::{DataType, JsonbVal, ScalarImpl};
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::{aggregate, ExprError, Result};

use crate::scalar::ToJsonb;

/// Collects all the input values, including nulls, into a JSON array.
/// Values are converted to JSON as per `to_jsonb`.
#[aggregate("jsonb_agg(boolean) -> jsonb")]
#[aggregate("jsonb_agg(*int) -> jsonb")]
#[aggregate("jsonb_agg(*float) -> jsonb")]
#[aggregate("jsonb_agg(decimal) -> jsonb")]
#[aggregate("jsonb_agg(serial) -> jsonb")]
#[aggregate("jsonb_agg(int256) -> jsonb")]
#[aggregate("jsonb_agg(date) -> jsonb")]
#[aggregate("jsonb_agg(time) -> jsonb")]
#[aggregate("jsonb_agg(timestamp) -> jsonb")]
#[aggregate("jsonb_agg(timestamptz) -> jsonb")]
#[aggregate("jsonb_agg(interval) -> jsonb")]
#[aggregate("jsonb_agg(varchar) -> jsonb")]
#[aggregate("jsonb_agg(bytea) -> jsonb")]
#[aggregate("jsonb_agg(jsonb) -> jsonb")]
fn jsonb_agg(state: &mut JsonbArrayState, input: Option<impl ToJsonb>) -> Result<()> {
    // FIXME(runji):
    // None of the input types we currently support depend on `data_type` in `add_to`.
    // So we just use a dummy type here.
    // To get the correct type, we need to support `ctx: &Context` argument in `#[aggregate]`.
    let data_type = &DataType::Int32;

    input.add_to(data_type, &mut state.0)?;
    Ok(())
}

/// Collects all the key/value pairs into a JSON object.
/// // TODO: support "any" type key
/// // Key arguments are coerced to text;
/// value arguments are converted as per `to_jsonb`.
/// Values can be null, but keys cannot.
#[aggregate("jsonb_object_agg(varchar, boolean) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *int) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *float) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, decimal) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, serial) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, int256) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, date) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, time) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, timestamp) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, timestamptz) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, interval) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, varchar) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, bytea) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, jsonb) -> jsonb")]
fn jsonb_object_agg(
    state: &mut JsonbObjectState,
    key: Option<&str>,
    value: Option<impl ToJsonb>,
) -> Result<()> {
    // FIXME(runji):
    // None of the input types we currently support depend on `data_type` in `add_to`.
    // So we just use a dummy type here.
    // To get the correct type, we need to support `ctx: &Context` argument in `#[aggregate]`.
    let data_type = &DataType::Int32;

    let key = key.ok_or(ExprError::FieldNameNull)?;
    state.0.add_string(key);
    value.add_to(data_type, &mut state.0)?;
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
impl From<&JsonbArrayState> for ScalarImpl {
    fn from(builder: &JsonbArrayState) -> Self {
        // TODO: avoid clone
        let mut builder = builder.0.clone();
        builder.end_array();
        let jsonb: JsonbVal = builder.finish().into();
        jsonb.into()
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
impl From<&JsonbObjectState> for ScalarImpl {
    fn from(builder: &JsonbObjectState) -> Self {
        // TODO: avoid clone
        let mut builder = builder.0.clone();
        builder.end_object();
        let jsonb: JsonbVal = builder.finish().into();
        jsonb.into()
    }
}
