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

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType as DFDataType, Field, FieldRef};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator as AccumulatorTrait, AggregateUDFImpl, Signature};
use datafusion_common::{Result as DFResult, ScalarValue, exec_datafusion_err, exec_err};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::types::{DataType as RwDataType, ListValue, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::{deserialize_datum, serialize_datum};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggregateState, BoxedAggregateFunction};

use crate::datafusion::{
    CastExecutor, convert_scalar_value, create_data_chunk, to_datafusion_error,
};

#[derive(educe::Educe)]
#[educe(Debug, PartialEq, Eq, Hash)]
pub struct AggregateFunction {
    name: String,
    input_types: Vec<RwDataType>,
    #[educe(Debug(ignore), PartialEq(ignore), Hash(ignore))]
    agg_impl: Arc<AggregateImpl>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    signature: Signature,
}

struct AggregateImpl {
    cast: CastExecutor,
    func: BoxedAggregateFunction,
}

#[derive(Debug)]
enum State {
    Uninit,
    Phase1(Vec<ArrayBuilderImpl>),
    Phase2(AggregateState),
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct Accumulator {
    #[educe(Debug(ignore))]
    agg_impl: Arc<AggregateImpl>,
    input_types: Vec<RwDataType>,
    state: State,
}

impl AggregateFunction {
    pub fn new(
        name: String,
        input_types: Vec<RwDataType>,
        cast: CastExecutor,
        func: BoxedAggregateFunction,
        signature: Signature,
    ) -> Self {
        Self {
            name,
            input_types,
            agg_impl: Arc::new(AggregateImpl { cast, func }),
            signature,
        }
    }
}

impl AggregateUDFImpl for AggregateFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DFDataType]) -> DFResult<DFDataType> {
        let data_type = self.agg_impl.func.return_type();
        Ok(IcebergArrowConvert
            .to_arrow_field("", &data_type)
            .map_err(to_datafusion_error)?
            .data_type()
            .clone())
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DFResult<Vec<FieldRef>> {
        let mut res = Vec::with_capacity(args.input_fields.len());
        for i in 0..args.input_fields.len() {
            let name = format!("{}[{}]", args.name, i);
            let field = Arc::new(Field::new(name, DFDataType::Binary, true));
            res.push(field);
        }
        for i in 0..args.ordering_fields.len() {
            let name = format!("{}[order_{}]", args.name, i);
            let field = Arc::new(Field::new(name, DFDataType::Binary, true));
            res.push(field);
        }
        Ok(res)
    }

    fn accumulator(&self, _: AccumulatorArgs<'_>) -> DFResult<Box<dyn AccumulatorTrait>> {
        Ok(Box::new(Accumulator {
            agg_impl: self.agg_impl.clone(),
            input_types: self.input_types.clone(),
            state: State::Uninit,
        }))
    }
}

impl AccumulatorTrait for Accumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.init_phase1()?;

        if values[0].is_empty() {
            return Ok(());
        }
        let num_rows = values[0].len();
        let chunk =
            create_data_chunk(values.iter().cloned(), num_rows).map_err(to_datafusion_error)?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.update_phase1(chunk))
        })?;
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.get_result())
        })
        .map_err(to_datafusion_error)?;
        let data_type = self.agg_impl.func.return_type();
        let res = convert_scalar_value(&value, data_type).map_err(to_datafusion_error)?;
        Ok(res)
    }

    fn size(&self) -> usize {
        match &self.state {
            State::Uninit => 0,
            State::Phase1(builders) => builders.iter().map(|b| b.estimated_size()).sum(),
            State::Phase2(state) => state.estimated_size(),
        }
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let arrays = match &mut self.state {
            State::Uninit => {
                return Ok(vec![ScalarValue::Binary(None); self.input_types.len()]);
            }
            State::Phase1(arrays) => std::mem::take(arrays),
            State::Phase2(_) => {
                return exec_err!("Accumulator state called after merge_batch");
            }
        };

        let res = arrays
            .into_iter()
            .map(|arr| {
                let list_value = ListValue::new(arr.finish());
                let binary = serialize_datum(Some(ScalarImpl::List(list_value)));
                ScalarValue::Binary(Some(binary))
            })
            .collect();
        Ok(res)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.init_phase2()?;
        if states[0].is_empty() {
            return Ok(());
        }

        let data_chunk = self.extract_state(states)?;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.update_phase2(data_chunk))
        })?;
        Ok(())
    }
}

impl Accumulator {
    fn init_phase1(&mut self) -> DFResult<()> {
        match self.state {
            State::Uninit => {}
            State::Phase1(_) => return Ok(()),
            State::Phase2(_) => {
                return exec_err!("Call to update_batch after merge_batch is not allowed");
            }
        }

        let inner = self
            .input_types
            .iter()
            .map(|ty| ty.create_array_builder(0))
            .collect();
        self.state = State::Phase1(inner);
        Ok(())
    }

    fn init_phase2(&mut self) -> DFResult<()> {
        match self.state {
            State::Uninit => {}
            State::Phase1(_) => {
                return exec_err!("Call to merge_batch after update_batch is not allowed");
            }
            State::Phase2(_) => return Ok(()),
        }

        let state = self
            .agg_impl
            .func
            .create_state()
            .map_err(to_datafusion_error)?;
        self.state = State::Phase2(state);
        Ok(())
    }

    async fn update_phase1(&mut self, chunk: DataChunk) -> DFResult<()> {
        let chunk = self
            .agg_impl
            .cast
            .execute(chunk)
            .await
            .map_err(to_datafusion_error)?;
        if !chunk.is_vis_compacted() {
            return exec_err!("Expected a visible-compacted chunk in aggregate update");
        }

        let state = match &mut self.state {
            State::Phase1(state) => state,
            _ => {
                return exec_err!("Accumulator has not been initialized to phase1 state");
            }
        };

        for (array, builder) in chunk.columns().iter().zip_eq_fast(state) {
            builder.append_array(array.as_ref());
        }
        Ok(())
    }

    fn extract_state(&self, states: &[ArrayRef]) -> DFResult<DataChunk> {
        let mut columns = Vec::with_capacity(states.len());
        for (state, input_type) in states.iter().zip_eq_fast(&self.input_types) {
            let state = state
                .as_binary_opt::<i32>()
                .ok_or_else(|| exec_datafusion_err!("Failed to get binary array from state"))?;
            let ty = input_type.clone().list();

            let mut array_builder = input_type.create_array_builder(0);
            for binary in state.iter().flatten() {
                let scalar = deserialize_datum(binary, &ty).map_err(to_datafusion_error)?;
                let Some(ScalarImpl::List(list_value)) = scalar else {
                    return exec_err!("Expected non-null ListValue in deserialized state");
                };
                array_builder.append_array(&list_value.into_array());
            }
            columns.push(Arc::new(array_builder.finish()));
        }
        let size = columns[0].len();
        let data_chunk = DataChunk::new(columns, Bitmap::ones(size));
        Ok(data_chunk)
    }

    async fn update_phase2(&mut self, chunk: DataChunk) -> DFResult<()> {
        let state = match &mut self.state {
            State::Phase2(state) => state,
            _ => {
                return exec_err!("Accumulator has not been initialized to phase2 state");
            }
        };

        self.agg_impl
            .func
            .update(state, &chunk.into())
            .await
            .map_err(to_datafusion_error)
    }

    async fn get_result(&mut self) -> DFResult<Option<ScalarImpl>> {
        match &mut self.state {
            State::Uninit => {
                let state = self
                    .agg_impl
                    .func
                    .create_state()
                    .map_err(to_datafusion_error)?;
                let result = self
                    .agg_impl
                    .func
                    .get_result(&state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase1(arrays) => {
                let columns = std::mem::take(arrays)
                    .into_iter()
                    .map(|b| Arc::new(b.finish()))
                    .collect::<Vec<_>>();
                let size = columns[0].len();
                let data_chunk = DataChunk::new(columns, Bitmap::ones(size));

                let func = self.agg_impl.func.as_ref();
                let mut agg_state = func.create_state().map_err(to_datafusion_error)?;
                func.update(&mut agg_state, &data_chunk.into())
                    .await
                    .map_err(to_datafusion_error)?;
                let result = func
                    .get_result(&agg_state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase2(state) => {
                let result = self
                    .agg_impl
                    .func
                    .get_result(state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
        }
    }
}

impl Debug for AggregateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinglePhaseAggregateImpl").finish()
    }
}
