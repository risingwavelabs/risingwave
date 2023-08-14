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

use std::any::Any;
use std::ops::Range;

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, DataTypeName, Datum};

use crate::sig::FuncSigDebug;
use crate::{ExprError, Result};

// aggregate definition
mod def;

// concrete AggregateFunctions
mod approx_count_distinct;
mod array_agg;
mod general;
mod jsonb_agg;
mod mode;
mod percentile_cont;
mod percentile_disc;
mod string_agg;

pub use self::def::*;

/// A trait over all aggregate functions.
#[async_trait::async_trait]
pub trait AggregateFunction: Send + Sync + 'static {
    /// Returns the return type of the aggregate function.
    fn return_type(&self) -> DataType;

    /// Returns the initial state of the aggregate function.
    fn init_state(&self) -> AggregateState {
        AggregateState::Datum(None)
    }

    /// Update the state with multiple rows.
    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()>;

    /// Update the state with a range of rows.
    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()>;

    /// Get aggregate result from the state.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum>;
}

/// Intermediate state of an aggregate function.
pub enum AggregateState {
    /// A scalar value.
    Datum(Datum),
    /// A state of any type.
    Any(Box<dyn Any + Send + Sync>),
}

impl AggregateState {
    fn as_datum(&self) -> &Datum {
        match self {
            Self::Datum(d) => d,
            Self::Any(_) => panic!("not datum"),
        }
    }

    fn as_datum_mut(&mut self) -> &mut Datum {
        match self {
            Self::Datum(d) => d,
            Self::Any(_) => panic!("not datum"),
        }
    }

    fn downcast_ref<T: Any>(&self) -> &T {
        match self {
            Self::Datum(_) => panic!("cannot downcast scalar"),
            Self::Any(a) => a.downcast_ref::<T>().expect("cannot downcast"),
        }
    }

    fn downcast_mut<T: Any>(&mut self) -> &mut T {
        match self {
            Self::Datum(_) => panic!("cannot downcast scalar"),
            Self::Any(a) => a.downcast_mut::<T>().expect("cannot downcast"),
        }
    }
}

pub type BoxedAggregateFunction = Box<dyn AggregateFunction>;

/// Build an `AggregateFunction` from `AggCall`.
///
/// NOTE: This function ignores argument indices, `column_orders`, `filter` and `distinct` in
/// `AggCall`. Such operations should be done in batch or streaming executors.
pub fn build(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    // NOTE: The function signature is checked by `AggCall::infer_return_type` in the frontend.

    let args = (agg.args.arg_types().iter())
        .map(|t| t.into())
        .collect::<Vec<DataTypeName>>();
    let ret_type = (&agg.return_type).into();
    let desc = crate::sig::agg::AGG_FUNC_SIG_MAP
        .get(agg.kind, &args, ret_type)
        .ok_or_else(|| {
            ExprError::UnsupportedFunction(format!(
                "{:?}",
                FuncSigDebug {
                    func: agg.kind,
                    inputs_type: &args,
                    ret_type,
                    set_returning: false,
                    deprecated: false,
                }
            ))
        })?;

    (desc.build)(agg)
}
