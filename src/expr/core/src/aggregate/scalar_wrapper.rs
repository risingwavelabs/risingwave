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

use risingwave_common::array::{Array, ArrayBuilderImpl, DataChunk, ListArray};
use risingwave_common::types::ListValue;

use super::*;
use crate::expr::{BoxedExpression, Expression};

/// Wraps a scalar function that takes a list as input as an aggregate function.
#[derive(Debug)]
pub struct ScalarWrapper {
    arg_type: DataType,
    scalar: BoxedExpression,
}

impl ScalarWrapper {
    /// Creates a new scalar wrapper.
    pub fn new(arg_type: DataType, scalar: BoxedExpression) -> Self {
        Self { arg_type, scalar }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for ScalarWrapper {
    fn return_type(&self) -> DataType {
        self.scalar.return_type()
    }

    /// Creates an initial state of the aggregate function.
    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::new(State(
            self.arg_type.create_array_builder(0),
        ))))
    }

    /// Update the state with multiple rows.
    ///
    /// All rows in the input chunk must be Insert.
    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = &mut state.downcast_mut::<State>().0;
        let column = input.column_at(0);
        for i in input.visibility().iter_ones() {
            state.append(column.datum_at(i));
        }
        Ok(())
    }

    /// Update the state with a range of rows.
    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = &mut state.downcast_mut::<State>().0;
        let column = input.column_at(0);
        for i in input.visibility().iter_ones() {
            if i < range.start {
                continue;
            } else if i >= range.end {
                break;
            }
            state.append(column.datum_at(i));
        }
        Ok(())
    }

    /// Get aggregate result from the state.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        // XXX: can we avoid cloning here?
        let list = ListValue::new(state.clone().finish());
        let chunk = DataChunk::new(
            vec![ListArray::from_single_value(self.arg_type.clone(), list).into_ref()],
            1,
        );
        let output = self.scalar.eval(&chunk).await?;
        Ok(output.to_datum())
    }

    /// Encode the state into a datum that can be stored in state table.
    fn encode_state(&self, _state: &AggregateState) -> Result<Datum> {
        panic!("should not store state in state table")
    }

    /// Decode the state from a datum in state table.
    fn decode_state(&self, _datum: Datum) -> Result<AggregateState> {
        panic!("should not store state in state table")
    }
}

/// The state is an array builder
#[derive(Debug)]
struct State(ArrayBuilderImpl);

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl AggStateDyn for State {}
