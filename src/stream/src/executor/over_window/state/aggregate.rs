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

use std::collections::BTreeSet;

use futures::FutureExt;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::{bail, must_match};
use risingwave_expr::agg::{build as builg_agg, AggArgs, AggCall, BoxedAggState};
use risingwave_expr::function::window::{WindowFuncCall, WindowFuncKind};
use smallvec::SmallVec;

use super::buffer::StreamWindowBuffer;
use super::{StateEvictHint, StateKey, StateOutput, StatePos, WindowState};
use crate::executor::StreamExecutorResult;

pub(super) struct AggregateState {
    factory: BoxedAggState,
    arg_data_types: Vec<DataType>,
    buffer: StreamWindowBuffer<StateKey, SmallVec<[Datum; 2]>>,
}

impl AggregateState {
    pub fn new(call: &WindowFuncCall) -> StreamExecutorResult<Self> {
        if !call.frame.bounds.is_valid() || call.frame.bounds.end_is_unbounded() {
            bail!("the window frame must be valid and end-bounded");
        }
        let agg_kind = must_match!(call.kind, WindowFuncKind::Aggregate(agg_kind) => agg_kind);
        let arg_data_types = call.args.arg_types().to_vec();
        let agg_call = AggCall {
            kind: agg_kind,
            args: match &call.args {
                // convert args to [0] or [0, 1]
                AggArgs::None => AggArgs::None,
                AggArgs::Unary(data_type, _) => AggArgs::Unary(data_type.to_owned(), 0),
                AggArgs::Binary(data_types, _) => AggArgs::Binary(data_types.to_owned(), [0, 1]),
            },
            return_type: call.return_type.clone(),
            column_orders: Vec::new(), // the input is already sorted
            // TODO(rc): support filter on window function call
            filter: None,
            // TODO(rc): support distinct on window function call? PG doesn't support it either.
            distinct: false,
        };
        Ok(Self {
            factory: builg_agg(agg_call)?,
            arg_data_types,
            buffer: StreamWindowBuffer::new(call.frame.clone()),
        })
    }
}

impl WindowState for AggregateState {
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>) {
        self.buffer.append(key, args);
    }

    fn curr_window(&self) -> StatePos<'_> {
        let window = self.buffer.curr_window();
        StatePos {
            key: window.key,
            is_ready: window.following_saturated,
        }
    }

    fn output(&mut self) -> StreamExecutorResult<StateOutput> {
        assert!(self.curr_window().is_ready);
        let wrapper = BatchAggregatorWrapper {
            factory: &self.factory,
            arg_data_types: &self.arg_data_types,
        };
        let return_value =
            wrapper.aggregate(self.buffer.curr_window_values().map(SmallVec::as_slice))?;
        let removed_keys: BTreeSet<_> = self.buffer.slide().collect();
        Ok(StateOutput {
            return_value,
            evict_hint: if removed_keys.is_empty() {
                StateEvictHint::CannotEvict(
                    self.buffer
                        .smallest_key()
                        .expect("sliding without removing, must have some entry in the buffer")
                        .clone(),
                )
            } else {
                StateEvictHint::CanEvict(removed_keys)
            },
        })
    }
}

struct BatchAggregatorWrapper<'a> {
    factory: &'a BoxedAggState,
    arg_data_types: &'a [DataType],
}

impl BatchAggregatorWrapper<'_> {
    fn aggregate<'a>(
        &'a self,
        values: impl Iterator<Item = &'a [Datum]>,
    ) -> StreamExecutorResult<Datum> {
        // TODO(rc): switch to a better general version of aggregator implementation

        let mut args_builders = self
            .arg_data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(0 /* bad! */))
            .collect::<Vec<_>>();
        let mut n_values = 0;
        for value in values {
            n_values += 1;
            for (builder, datum) in args_builders.iter_mut().zip_eq_fast(value.iter()) {
                builder.append_datum(datum);
            }
        }

        let columns = args_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect::<Vec<_>>();
        let chunk = DataChunk::new(columns, Vis::Compact(n_values));

        let mut aggregator = self.factory.clone();
        aggregator
            .update_multi(&chunk, 0, n_values)
            .now_or_never()
            .expect("we don't support UDAF currently, so the function should return immediately")?;

        let mut ret_value_builder = aggregator.return_type().create_array_builder(1);
        aggregator.output(&mut ret_value_builder)?;
        Ok(ret_value_builder.finish().to_datum())
    }
}

#[cfg(test)]
mod tests {
    // TODO(rc): need to add some unit tests
}
