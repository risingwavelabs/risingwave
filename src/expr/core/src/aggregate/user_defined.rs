// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use arrow_array::ArrayRef;
use arrow_schema::{Field, Fields, Schema, SchemaRef};
use risingwave_common::array::arrow::{FromArrow, ToArrow, UdfArrowConvert};
use risingwave_common::array::Op;

use super::*;
use crate::sig::{UdfImpl, UdfOptions};

#[derive(Debug)]
pub struct UserDefinedAggregateFunction {
    arg_schema: SchemaRef,
    return_type: DataType,
    state_type: DataType,
    return_field: Field,
    state_field: Field,
    runtime: Box<dyn UdfImpl>,
}

#[async_trait::async_trait]
impl AggregateFunction for UserDefinedAggregateFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    /// Creates an initial state of the aggregate function.
    fn create_state(&self) -> Result<AggregateState> {
        let state = self.runtime.create_state()?;
        Ok(AggregateState::Any(Box::new(State(state))))
    }

    /// Update the state with multiple rows.
    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = &mut state.downcast_mut::<State>().0;
        let ops = input
            .visibility()
            .iter_ones()
            .map(|i| Some(matches!(input.ops()[i], Op::Delete | Op::UpdateDelete)))
            .collect();
        // this will drop invisible rows
        let arrow_input = UdfArrowConvert::default()
            .to_record_batch(self.arg_schema.clone(), input.data_chunk())?;
        let new_state = self
            .runtime
            .accumulate_or_retract(state, &ops, &arrow_input)?;
        *state = new_state;
        Ok(())
    }

    /// Update the state with a range of rows.
    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        todo!()
    }

    /// Get aggregate result from the state.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        let arrow_output = self.runtime.finish(state)?;
        let output = UdfArrowConvert::default().from_array(&self.return_field, &arrow_output)?;
        Ok(output.datum_at(0))
    }

    /// Encode the state into a datum that can be stored in state table.
    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        let state = UdfArrowConvert::default().from_array(&self.state_field, state)?;
        Ok(state.datum_at(0))
    }

    /// Decode the state from a datum in state table.
    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let array = {
            let mut builder = self.state_type.create_array_builder(1);
            builder.append(datum);
            builder.finish()
        };
        let state = UdfArrowConvert::default().to_array(self.state_field.data_type(), &array)?;
        Ok(AggregateState::Any(Box::new(State(state))))
    }
}

// In arrow-udf, aggregate state is represented as an `ArrayRef`.
// To avoid unnecessary conversion between `ArrayRef` and `Datum`,
// we store `ArrayRef` directly in our `AggregateState`.
#[derive(Debug)]
struct State(ArrayRef);

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

impl AggStateDyn for State {}

/// Create a new user-defined aggregate function.
pub fn new_user_defined(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    let udf = agg
        .user_defined
        .as_ref()
        .context("missing UDF definition")?;

    let identifier = udf.get_identifier()?;
    let language = udf.language.as_str();
    let runtime = udf.runtime.as_deref();
    let link = udf.link.as_deref();
    let state_type = DataType::from(udf.state_type.as_ref().context("missing state type")?);

    let build_fn = crate::sig::find_udf_impl(language, runtime, link)?.build_fn;
    let runtime = build_fn(UdfOptions {
        table_function: true,
        body: udf.body.as_deref(),
        compressed_binary: udf.compressed_binary.as_deref(),
        link: udf.link.as_deref(),
        identifier,
        arg_names: &udf.arg_names,
        return_type: &agg.return_type,
        state_type: Some(&state_type),
        always_retry_on_network_error: false,
        function_type: udf.function_type.as_deref(),
    })
    .context("failed to build UDF runtime")?;

    // legacy UDF runtimes do not support aggregate functions,
    // so we can assume that the runtime is not legacy
    let arrow_convert = UdfArrowConvert::default();
    let arg_schema = Arc::new(Schema::new(
        udf.arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", &DataType::from(t)))
            .try_collect::<_, Fields, _>()?,
    ));

    Ok(Box::new(UserDefinedAggregateFunction {
        return_field: arrow_convert.to_arrow_field("", &agg.return_type)?,
        state_field: arrow_convert.to_arrow_field("", &state_type)?,
        return_type: agg.return_type.clone(),
        state_type,
        arg_schema,
        runtime,
    }))
}
