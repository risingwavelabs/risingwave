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
                    let non_null_count = needs_non_null_count(*kind).then_some(0);
                    (
                        AggImpl::Incremental(agg_func, init_state, non_null_count),
                        true,
                    )
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
            AggImpl::Incremental(ref agg_func, ref mut state, ref mut non_null_count) => {
                let wrapper = AggregatorWrapper {
                    agg_func: agg_func.as_ref(),
                    arg_data_types: &self.arg_data_types,
                };
                wrapper.update(
                    state,
                    non_null_count.as_mut(),
                    self.buffer.consume_curr_window_values_delta(),
                )
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
            AggImpl::Incremental(ref agg_func, ref mut state, ref mut non_null_count) => {
                // for incremental agg, we need to update the state even if the caller doesn't need
                // the output
                let wrapper = AggregatorWrapper {
                    agg_func: agg_func.as_ref(),
                    arg_data_types: &self.arg_data_types,
                };
                wrapper.update(
                    state,
                    non_null_count.as_mut(),
                    self.buffer.consume_curr_window_values_delta(),
                )?;
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
    /// The optional `usize` is the number of non-NULL inputs in the current window, maintained only
    /// for aggregates that need it to output NULL correctly, see [`needs_non_null_count`].
    Incremental(
        #[educe(Debug(ignore))] BoxedAggregateFunction,
        AggImplState,
        Option<usize>,
    ),
    Full(#[educe(Debug(ignore))] BoxedAggregateFunction),
    Shortcut(Shortcut),
}

/// Aggregates like `sum` should output NULL once no non-NULL input is left in the window. Their
/// retractable state can't tell that apart from inputs cancelling out (e.g. `sum` goes back to `0`),
/// so we count the non-NULL inputs alongside the state and output NULL when the count is `0`.
///
/// Keep this in sync with `LogicalAgg::guard_empty_input_with_count` in the frontend.
fn needs_non_null_count(kind: PbAggKind) -> bool {
    matches!(
        kind,
        PbAggKind::Sum
            | PbAggKind::BitAnd
            | PbAggKind::BitOr
            | PbAggKind::BitXor
            | PbAggKind::BoolAnd
            | PbAggKind::BoolOr
    )
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
            None,
            values.into_iter().map(|args| (Op::Insert, args)),
        )
    }

    fn update<V>(
        &self,
        state: &mut AggImplState,
        mut non_null_count: Option<&mut usize>,
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
            if let Some(count) = non_null_count.as_deref_mut()
                && value.as_ref().iter().all(|datum| datum.is_some())
            {
                match op {
                    Op::Insert | Op::UpdateInsert => *count += 1,
                    Op::Delete | Op::UpdateDelete => *count -= 1,
                }
            }
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
        if non_null_count.is_some_and(|count| *count == 0) {
            return Ok(None);
        }
        self.agg_func
            .get_result(state)
            .now_or_never()
            .expect("we don't support UDAF currently, so the function should return immediately")
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::memcmp_encoding;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::aggregate::AggArgs;
    use risingwave_expr::window_function::{Frame, FrameBound};
    use smallvec::smallvec;

    use super::*;

    fn create_state_key(order: i64) -> StateKey {
        StateKey {
            order_key: memcmp_encoding::encode_value(
                Some(ScalarImpl::from(order)),
                OrderType::ascending(),
            )
            .unwrap(),
            pk: OwnedRow::new(vec![Some(order.into())]).into(),
        }
    }

    /// Feeds `values` into a `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` window and returns the
    /// output for each row.
    fn run_rows_window(
        kind: PbAggKind,
        arg_type: DataType,
        return_type: DataType,
        values: Vec<Datum>,
    ) -> Vec<Datum> {
        let call = WindowFuncCall {
            kind: WindowFuncKind::Aggregate(AggType::Builtin(kind)),
            return_type,
            args: AggArgs::from_iter([(arg_type, 0)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::CurrentRow),
        };
        let mut state = new(&call).unwrap();
        let mut outputs = vec![];
        for (i, value) in values.into_iter().enumerate() {
            state.append(create_state_key(i as i64), smallvec![value]);
            assert!(state.curr_window().is_ready);
            outputs.push(state.slide().unwrap().0);
        }
        outputs
    }

    #[test]
    fn test_null_after_non_null_inputs_leave_frame() {
        let int = |v: i32| Some(ScalarImpl::Int32(v));
        let bigint = |v: i64| Some(ScalarImpl::Int64(v));
        let boolean = |v: bool| Some(ScalarImpl::Bool(v));

        let ints = vec![int(5), None, None, int(3)];
        assert_eq!(
            run_rows_window(
                PbAggKind::Sum,
                DataType::Int32,
                DataType::Int64,
                ints.clone()
            ),
            vec![bigint(5), bigint(5), None, bigint(3)]
        );
        for kind in [PbAggKind::BitAnd, PbAggKind::BitOr, PbAggKind::BitXor] {
            assert_eq!(
                run_rows_window(kind, DataType::Int32, DataType::Int32, ints.clone()),
                vec![int(5), int(5), None, int(3)]
            );
        }
        // `count` should still output `0` rather than NULL.
        assert_eq!(
            run_rows_window(PbAggKind::Count, DataType::Int32, DataType::Int64, ints),
            vec![bigint(1), bigint(1), bigint(0), bigint(1)]
        );

        let bools = vec![boolean(true), None, None, boolean(false)];
        for kind in [PbAggKind::BoolAnd, PbAggKind::BoolOr] {
            assert_eq!(
                run_rows_window(kind, DataType::Boolean, DataType::Boolean, bools.clone()),
                vec![boolean(true), boolean(true), None, boolean(false)]
            );
        }
    }
}
