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

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType as DFDataType, FieldRef};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator as AccumulatorTrait, AggregateUDFImpl, Signature};
use datafusion_common::{Result as DFResult, ScalarValue, exec_err};
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::types::ScalarImpl;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggregateState, BoxedAggregateFunction};

use crate::datafusion::{
    CastExecutor, convert_scalar_value, create_data_chunk, to_datafusion_error,
};
use crate::error::Result as RwResult;

#[derive(educe::Educe)]
#[educe(Debug, PartialEq, Eq, Hash)]
pub struct AggregateFunction {
    name: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    agg_impl: Arc<AggregateImpl>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    signature: Signature,
}

struct AggregateImpl {
    phase1_cast: CastExecutor,
    phase2_cast: CastExecutor,
    phase1: BoxedAggregateFunction,
    phase2: BoxedAggregateFunction,
}

#[derive(Debug)]
enum State {
    Uninit,
    Phase1(AggregateState),
    Phase2(AggregateState),
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct Accumulator {
    #[educe(Debug(ignore))]
    agg_impl: Arc<AggregateImpl>,
    state: State,
}

impl AggregateFunction {
    pub fn new(
        name: String,
        input_cast: CastExecutor,
        phase1: BoxedAggregateFunction,
        phase2: BoxedAggregateFunction,
        signature: Signature,
    ) -> RwResult<Self> {
        let phase2_source = IcebergArrowConvert
            .to_arrow_field("", &phase1.return_type())?
            .data_type()
            .clone();
        let phase2_cast = CastExecutor::from_iter(
            std::iter::once(phase2_source),
            std::iter::once(phase1.return_type()),
        )?;
        Ok(Self {
            name,
            agg_impl: Arc::new(AggregateImpl {
                phase1_cast: input_cast,
                phase2_cast,
                phase1,
                phase2,
            }),
            signature,
        })
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
        let data_type = self.agg_impl.phase2.return_type();
        Ok(IcebergArrowConvert
            .to_arrow_field("", &data_type)
            .map_err(to_datafusion_error)?
            .data_type()
            .clone())
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DFResult<Vec<FieldRef>> {
        let data_type = self.agg_impl.phase1.return_type();
        let name = format!("{}_state", args.name);
        let field = IcebergArrowConvert
            .to_arrow_field(&name, &data_type)
            .map_err(to_datafusion_error)?;
        Ok(vec![Arc::new(field)])
    }

    fn accumulator(&self, _: AccumulatorArgs<'_>) -> DFResult<Box<dyn AccumulatorTrait>> {
        Ok(Box::new(Accumulator {
            agg_impl: self.agg_impl.clone(),
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
        let data_type = self.agg_impl.phase2.return_type();
        let res = convert_scalar_value(&value, data_type).map_err(to_datafusion_error)?;
        Ok(res)
    }

    fn size(&self) -> usize {
        match &self.state {
            State::Uninit => 0,
            State::Phase1(state) => state.estimated_size(),
            State::Phase2(state) => state.estimated_size(),
        }
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.get_state())
        })
        .map_err(to_datafusion_error)?;
        let data_type = self.agg_impl.phase1.return_type();
        let res = convert_scalar_value(&value, data_type).map_err(to_datafusion_error)?;
        Ok(vec![res])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.init_phase2()?;
        if states[0].is_empty() {
            return Ok(());
        }
        let num_rows = states[0].len();
        let chunk = create_data_chunk(std::iter::once(states[0].clone()), num_rows)
            .map_err(to_datafusion_error)?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.update_phase2(chunk))
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

        let state = self
            .agg_impl
            .phase1
            .create_state()
            .map_err(to_datafusion_error)?;
        self.state = State::Phase1(state);
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
            .phase2
            .create_state()
            .map_err(to_datafusion_error)?;
        self.state = State::Phase2(state);
        Ok(())
    }

    async fn update_phase1(&mut self, chunk: DataChunk) -> DFResult<()> {
        let state = match &mut self.state {
            State::Phase1(state) => state,
            _ => {
                return exec_err!("Accumulator has not been initialized to phase1 state");
            }
        };

        let chunk = self
            .agg_impl
            .phase1_cast
            .execute(chunk)
            .await
            .map_err(to_datafusion_error)?;
        self.agg_impl
            .phase1
            .update(state, &chunk.into())
            .await
            .map_err(to_datafusion_error)
    }

    async fn update_phase2(&mut self, chunk: DataChunk) -> DFResult<()> {
        let state = match &mut self.state {
            State::Phase2(state) => state,
            _ => {
                return exec_err!("Accumulator has not been initialized to phase2 state");
            }
        };

        let chunk = self
            .agg_impl
            .phase2_cast
            .execute(chunk)
            .await
            .map_err(to_datafusion_error)?;
        self.agg_impl
            .phase2
            .update(state, &chunk.into())
            .await
            .map_err(to_datafusion_error)
    }

    async fn get_result(&self) -> DFResult<Option<ScalarImpl>> {
        match &self.state {
            State::Uninit => {
                let func = self.agg_impl.phase2.as_ref();
                let state = func.create_state().map_err(to_datafusion_error)?;
                let result = func.get_result(&state).await.map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase1(state) => {
                let phase1 = self.agg_impl.phase1.as_ref();
                let phase2 = self.agg_impl.phase2.as_ref();
                let mut agg_state = phase2.create_state().map_err(to_datafusion_error)?;

                let value = phase1
                    .get_result(state)
                    .await
                    .map_err(to_datafusion_error)?;
                if let Some(v) = value {
                    let mut builder = phase1.return_type().create_array_builder(1);
                    builder.append(Some(v));
                    let columns = vec![Arc::new(builder.finish())];
                    let chunk = DataChunk::new(columns, Bitmap::ones(1));

                    phase2
                        .update(&mut agg_state, &chunk.into())
                        .await
                        .map_err(to_datafusion_error)?;
                }

                let result = phase2
                    .get_result(&agg_state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase2(state) => {
                let result = self
                    .agg_impl
                    .phase2
                    .get_result(state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
        }
    }

    async fn get_state(&self) -> DFResult<Option<ScalarImpl>> {
        match &self.state {
            State::Uninit => {
                let phase1 = self.agg_impl.phase1.as_ref();
                let state = phase1.create_state().map_err(to_datafusion_error)?;
                let result = phase1
                    .get_result(&state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase1(state) => {
                let result = self
                    .agg_impl
                    .phase1
                    .get_result(state)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(result)
            }
            State::Phase2(_) => {
                exec_err!("Cannot get intermediate state from phase2 accumulator")
            }
        }
    }
}

impl Debug for AggregateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoPhaseAggregateImpl").finish()
    }
}
