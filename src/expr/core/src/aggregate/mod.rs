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

use std::fmt::Debug;
use std::ops::Range;

use anyhow::anyhow;
use downcast_rs::{Downcast, impl_downcast};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_common_estimate_size::EstimateSize;

use crate::expr::build_from_prost;
use crate::sig::FuncBuilder;
use crate::{ExprError, Result};

// aggregate definition
mod def;
mod scalar_wrapper;
// user defined aggregate function
mod user_defined;

pub use self::def::*;

/// A trait over all aggregate functions.
#[async_trait::async_trait]
pub trait AggregateFunction: Send + Sync + 'static {
    /// Returns the return type of the aggregate function.
    fn return_type(&self) -> DataType;

    /// Creates an initial state of the aggregate function.
    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Datum(None))
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

    /// Encode the state into a datum that can be stored in state table.
    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        match state {
            AggregateState::Datum(d) => Ok(d.clone()),
            AggregateState::Any(_) => Err(ExprError::Internal(anyhow!("cannot encode state"))),
        }
    }

    /// Decode the state from a datum in state table.
    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        Ok(AggregateState::Datum(datum))
    }
}

/// Intermediate state of an aggregate function.
#[derive(Debug)]
pub enum AggregateState {
    /// A scalar value.
    Datum(Datum),
    /// A state of any type.
    Any(Box<dyn AggStateDyn>),
}

impl EstimateSize for AggregateState {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Self::Datum(d) => d.estimated_heap_size(),
            Self::Any(a) => std::mem::size_of_val(&**a) + a.estimated_heap_size(),
        }
    }
}

pub trait AggStateDyn: Send + Sync + Debug + EstimateSize + Downcast {}

impl_downcast!(AggStateDyn);

impl AggregateState {
    pub fn as_datum(&self) -> &Datum {
        match self {
            Self::Datum(d) => d,
            Self::Any(_) => panic!("not datum"),
        }
    }

    pub fn as_datum_mut(&mut self) -> &mut Datum {
        match self {
            Self::Datum(d) => d,
            Self::Any(_) => panic!("not datum"),
        }
    }

    pub fn downcast_ref<T: AggStateDyn>(&self) -> &T {
        match self {
            Self::Datum(_) => panic!("cannot downcast scalar"),
            Self::Any(a) => a.downcast_ref::<T>().expect("cannot downcast"),
        }
    }

    pub fn downcast_mut<T: AggStateDyn>(&mut self) -> &mut T {
        match self {
            Self::Datum(_) => panic!("cannot downcast scalar"),
            Self::Any(a) => a.downcast_mut::<T>().expect("cannot downcast"),
        }
    }
}

pub type BoxedAggregateFunction = Box<dyn AggregateFunction>;

/// Build an append-only `Aggregator` from `AggCall`.
pub fn build_append_only(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    build(agg, true)
}

/// Build a retractable `Aggregator` from `AggCall`.
pub fn build_retractable(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    build(agg, false)
}

/// Build an aggregate function.
///
/// If `prefer_append_only` is true, and both append-only and retractable implementations exist,
/// the append-only version will be used.
///
/// NOTE: This function ignores argument indices, `column_orders`, `filter` and `distinct` in
/// `AggCall`. Such operations should be done in batch or streaming executors.
pub fn build(agg: &AggCall, prefer_append_only: bool) -> Result<BoxedAggregateFunction> {
    // handle special kinds
    let kind = match &agg.agg_type {
        AggType::UserDefined(udf) => {
            return user_defined::new_user_defined(&agg.return_type, udf);
        }
        AggType::WrapScalar(scalar) => {
            return Ok(Box::new(scalar_wrapper::ScalarWrapper::new(
                agg.args.arg_types()[0].clone(),
                build_from_prost(scalar)?,
            )));
        }
        AggType::Builtin(kind) => kind,
    };

    // find the signature for builtin aggregation
    let sig = crate::sig::FUNCTION_REGISTRY.get(*kind, agg.args.arg_types(), &agg.return_type)?;

    if let FuncBuilder::Aggregate {
        append_only: Some(f),
        ..
    } = sig.build
        && prefer_append_only
    {
        return f(agg);
    }
    sig.build_aggregate(agg)
}
