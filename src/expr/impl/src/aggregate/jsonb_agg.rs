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
use risingwave_common::types::{JsonbRef, JsonbVal, ScalarImpl, F32, F64};
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::{aggregate, ExprError, Result};

#[aggregate("jsonb_agg(boolean) -> jsonb")]
#[aggregate("jsonb_agg(*int) -> jsonb")]
#[aggregate("jsonb_agg(*float) -> jsonb")]
#[aggregate("jsonb_agg(varchar) -> jsonb")]
#[aggregate("jsonb_agg(jsonb) -> jsonb")]
fn jsonb_agg(state: &mut JsonbArrayState, input: Option<impl ToJson>) {
    match input {
        Some(input) => input.add_to(&mut state.0),
        None => state.0.add_null(),
    }
}

#[aggregate("jsonb_object_agg(varchar, boolean) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *int) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *float) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, varchar) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, jsonb) -> jsonb")]
fn jsonb_object_agg(
    state: &mut JsonbObjectState,
    key: Option<&str>,
    value: Option<impl ToJson>,
) -> Result<()> {
    let key = key.ok_or(ExprError::FieldNameNull)?;
    state.0.add_string(key);
    match value {
        Some(value) => value.add_to(&mut state.0),
        None => state.0.add_null(),
    }
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

/// Values that can be converted to JSON.
trait ToJson {
    fn add_to(self, builder: &mut jsonbb::Builder);
}

impl ToJson for bool {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_bool(self);
    }
}

impl ToJson for i16 {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_i64(self as _);
    }
}

impl ToJson for i32 {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_i64(self as _);
    }
}

impl ToJson for i64 {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_i64(self);
    }
}

impl ToJson for F32 {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_f64(self.0 as f64);
    }
}

impl ToJson for F64 {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_f64(self.0);
    }
}

impl ToJson for &str {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_string(self);
    }
}

impl ToJson for JsonbRef<'_> {
    fn add_to(self, builder: &mut jsonbb::Builder) {
        builder.add_value(self.into());
    }
}
