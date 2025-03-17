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

use std::collections::BTreeSet;

use educe::Educe;
use futures_util::FutureExt;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::{bail, must_match};
use risingwave_common_estimate_size::{EstimateSize, KvSize};
use risingwave_expr::Result;
use risingwave_expr::aggregate::{
    AggCall, AggType, AggregateFunction, AggregateState as AggImplState, BoxedAggregateFunction,
    PbAggKind, build_append_only,
};
use risingwave_expr::sig::FUNCTION_REGISTRY;
use risingwave_expr::window_function::{
    BoxedWindowState, FrameBounds, StateEvictHint, StateKey, StatePos, WindowFuncCall,
    WindowFuncKind, WindowState,
};
use smallvec::SmallVec;

use super::buffer::{RangeWindow, RowsWindow, SessionWindow, WindowBuffer, WindowImpl};

type StateValue = SmallVec<[Datum; 2]>;

struct AggregateState<W>
where
    W: WindowImpl<Key = StateKey, Value = StateValue>,
{
    agg_impl: AggImpl,
    arg_data_types: Vec<DataType>,
    ignore_nulls: bool,
    buffer: WindowBuffer<W>,
    buffer_heap_size: KvSize,
}

pub(super) fn new(call: &WindowFuncCall) -> Result<BoxedWindowState> {
    if call.frame.bounds.validate().is_err() {
        bail!("the window frame must be valid");
    }
    let agg_type = must_match!(&call.kind, WindowFuncKind::Aggregate(agg_type) => agg_type);
    let arg_data_types = call.args.arg_types().to_vec();
    let agg_call = AggCall {
        agg_type: agg_type.clone(),
        args: call.args.clone(),
        return_type: call.return_type.clone(),
        column_orders: Vec::new(), // the input is already sorted
        // TODO(rc): support filter on window function call
        filter: None,
        // TODO(rc): support distinct on window function call? PG doesn't support it either.
        distinct: false,
        direct_args: vec![],
    };

    let (agg_impl, enable_delta) = match agg_type {
        AggType::Builtin(PbAggKind::FirstValue) => (AggImpl::Shortcut(Shortcut::FirstValue), false),
        AggType::Builtin(PbAggKind::LastValue) => (AggImpl::Shortcut(Shortcut::LastValue), false),
        AggType::Builtin(kind) => {
            let agg_func_sig = FUNCTION_REGISTRY
                .get(*kind, &arg_data_types, &call.return_type)
                .expect("the agg func must exist");
            let agg_func = agg_func_sig.build_aggregate(&agg_call)?;
            let (agg_impl, enable_delta) =
                if agg_func_sig.is_retractable() && call.frame.exclusion.is_no_others() {
                    let init_state = agg_func.create_state()?;
                    (AggImpl::Incremental(agg_func, init_state), true)
                } else {
                    (AggImpl::Full(agg_func), false)
                };
            (agg_impl, enable_delta)
        }
        AggType::UserDefined(_) => {
            // TODO(rc): utilize `retract` method of embedded UDAF to do incremental aggregation
            (AggImpl::Full(build_append_only(&agg_call)?), false)
        }
        AggType::WrapScalar(_) => {
            // we have to feed the wrapped scalar function with all the rows in the window,
            // instead of doing incremental aggregation
            (AggImpl::Full(build_append_only(&agg_call)?), false)
        }
    };

    let this = match &call.frame.bounds {
        FrameBounds::Rows(frame_bounds) => Box::new(AggregateState {
            agg_impl,
            arg_data_types,
            ignore_nulls: call.ignore_nulls,
            buffer: WindowBuffer::<RowsWindow<StateKey, StateValue>>::new(
                RowsWindow::new(frame_bounds.clone()),
                call.frame.exclusion,
                enable_delta,
            ),
            buffer_heap_size: KvSize::new(),
        }) as BoxedWindowState,
        FrameBounds::Range(frame_bounds) => Box::new(AggregateState {
            agg_impl,
            arg_data_types,
            ignore_nulls: call.ignore_nulls,
            buffer: WindowBuffer::<RangeWindow<StateValue>>::new(
                RangeWindow::new(frame_bounds.clone()),
                call.frame.exclusion,
                enable_delta,
            ),
            buffer_heap_size: KvSize::new(),
        }) as BoxedWindowState,
        FrameBounds::Session(frame_bounds) => Box::new(AggregateState {
            agg_impl,
            arg_data_types,
            ignore_nulls: call.ignore_nulls,
            buffer: WindowBuffer::<SessionWindow<StateValue>>::new(
                SessionWindow::new(frame_bounds.clone()),
                call.frame.exclusion,
                enable_delta,
            ),
            buffer_heap_size: KvSize::new(),
        }) as BoxedWindowState,
    };
    Ok(this)
}

impl<W> AggregateState<W>
where
    W: WindowImpl<Key = StateKey, Value = StateValue>,
{
    fn slide_inner(&mut self) -> StateEvictHint {
        let removed_keys: BTreeSet<_> = self
            .buffer
            .slide()
            .map(|(k, v)| {
                v.iter().for_each(|arg| {
                    self.buffer_heap_size.sub_val(arg);
                });
                self.buffer_heap_size.sub_val(&k);
                k
            })
            .collect();
        if removed_keys.is_empty() {
            StateEvictHint::CannotEvict(
                self.buffer
                    .smallest_key()
                    .expect("sliding without removing, must have some entry in the buffer")
                    .clone(),
            )
        } else {
            StateEvictHint::CanEvict(removed_keys)
        }
    }
}

impl<W> WindowState for AggregateState<W>
where
    W: WindowImpl<Key = StateKey, Value = StateValue>,
{
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>) {
        args.iter().for_each(|arg| {
            self.buffer_heap_size.add_val(arg);
        });
        self.buffer_heap_size.add_val(&key);
        self.buffer.append(key, args);
    }

    fn curr_window(&self) -> StatePos<'_> {
        let window = self.buffer.curr_window();
        StatePos {
            key: window.key,
            is_ready: window.following_saturated,
        }
    }

    fn slide(&mut self) -> Result<(Datum, StateEvictHint)> {
        let output = match self.agg_impl {
            AggImpl::Full(ref agg_func) => {
                let wrapper = AggregatorWrapper {
                    agg_func: agg_func.as_ref(),
                    arg_data_types: &self.arg_data_types,
                };
                wrapper.aggregate(self.buffer.curr_window_values())
            }
            AggImpl::Incremental(ref agg_func, ref mut state) => {
                let wrapper = AggregatorWrapper {
                    agg_func: agg_func.as_ref(),
                    arg_data_types: &self.arg_data_types,
                };
                wrapper.update(state, self.buffer.consume_curr_window_values_delta())
            }
            AggImpl::Shortcut(shortcut) => match shortcut {
                Shortcut::FirstValue => Ok(if !self.ignore_nulls {
                    // no `IGNORE NULLS`
                    self.buffer
                        .curr_window_values()
                        .next()
                        .and_then(|args| args[0].clone())
                } else {
                    // filter out NULLs
                    self.buffer
                        .curr_window_values()
                        .find(|args| args[0].is_some())
                        .and_then(|args| args[0].clone())
                }),
                Shortcut::LastValue => Ok(if !self.ignore_nulls {
                    self.buffer
                        .curr_window_values()
                        .next_back()
                        .and_then(|args| args[0].clone())
                } else {
                    self.buffer
                        .curr_window_values()
                        .rev()
                        .find(|args| args[0].is_some())
                        .and_then(|args| args[0].clone())
                }),
            },
        }?;
        let evict_hint = self.slide_inner();
        Ok((output, evict_hint))
    }

    fn slide_no_output(&mut self) -> Result<StateEvictHint> {
        match self.agg_impl {
            AggImpl::Full(..) => {}
            AggImpl::Incremental(ref agg_func, ref mut state) => {
                // for incremental agg, we need to update the state even if the caller doesn't need
                // the output
                let wrapper = AggregatorWrapper {
                    agg_func: agg_func.as_ref(),
                    arg_data_types: &self.arg_data_types,
                };
                wrapper.update(state, self.buffer.consume_curr_window_values_delta())?;
            }
            AggImpl::Shortcut(..) => {}
        };
        Ok(self.slide_inner())
    }
}

impl<W> EstimateSize for AggregateState<W>
where
    W: WindowImpl<Key = StateKey, Value = StateValue>,
{
    fn estimated_heap_size(&self) -> usize {
        // estimate `VecDeque` of `StreamWindowBuffer` internal size
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.arg_data_types.estimated_heap_size() + self.buffer_heap_size.size()
    }
}

#[derive(Educe)]
#[educe(Debug)]
enum AggImpl {
    Incremental(#[educe(Debug(ignore))] BoxedAggregateFunction, AggImplState),
    Full(#[educe(Debug(ignore))] BoxedAggregateFunction),
    Shortcut(Shortcut),
}

#[derive(Debug, Clone, Copy)]
enum Shortcut {
    FirstValue,
    LastValue,
}

struct AggregatorWrapper<'a> {
    agg_func: &'a dyn AggregateFunction,
    arg_data_types: &'a [DataType],
}

impl AggregatorWrapper<'_> {
    fn aggregate<V>(&self, values: impl IntoIterator<Item = V>) -> Result<Datum>
    where
        V: AsRef<[Datum]>,
    {
        let mut state = self.agg_func.create_state()?;
        self.update(
            &mut state,
            values.into_iter().map(|args| (Op::Insert, args)),
        )
    }

    fn update<V>(
        &self,
        state: &mut AggImplState,
        delta: impl IntoIterator<Item = (Op, V)>,
    ) -> Result<Datum>
    where
        V: AsRef<[Datum]>,
    {
        let mut args_builders = self
            .arg_data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(0 /* bad! */))
            .collect::<Vec<_>>();
        let mut ops = Vec::new();
        let mut n_rows = 0;
        for (op, value) in delta {
            n_rows += 1;
            ops.push(op);
            for (builder, datum) in args_builders.iter_mut().zip_eq_fast(value.as_ref()) {
                builder.append(datum);
            }
        }
        let columns = args_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect::<Vec<_>>();
        let chunk = StreamChunk::from_parts(ops, DataChunk::new(columns, n_rows));

        self.agg_func
            .update(state, &chunk)
            .now_or_never()
            .expect("we don't support UDAF currently, so the function should return immediately")?;
        self.agg_func
            .get_result(state)
            .now_or_never()
            .expect("we don't support UDAF currently, so the function should return immediately")
    }
}

#[cfg(test)]
mod tests {
    // TODO(rc): need to add some unit tests
}
