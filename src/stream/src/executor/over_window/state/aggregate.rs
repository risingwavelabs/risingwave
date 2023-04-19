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

use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_expr::function::aggregate::AggCall;
use risingwave_expr::function::window::{WindowFuncCall, WindowFuncKind};
use risingwave_expr::vector_op::agg::AggStateFactory;
use smallvec::SmallVec;

use super::buffer::WindowBuffer;
use super::{StateEvictHint, StateKey, StateOutput, StatePos, WindowState};
use crate::executor::StreamExecutorResult;

pub(super) struct AggregateState {
    factory: AggStateFactory,
    buffer: WindowBuffer<StateKey, SmallVec<[Datum; 2]>>,
}

impl AggregateState {
    pub fn new(call: &WindowFuncCall) -> StreamExecutorResult<Self> {
        let agg_kind = must_match!(call.kind, WindowFuncKind::Aggregate(agg_kind) => agg_kind);
        let agg_call = AggCall {
            kind: agg_kind,
            args: call.args.clone(),
            return_type: call.return_type.clone(),
            column_orders: Vec::new(), // the input is already sorted
            // TODO(rc): support filter on window function call
            filter: None,
            // TODO(rc): support distinct on window function call? PG doesn't support it either.
            distinct: false,
        };
        Ok(Self {
            factory: AggStateFactory::new(agg_call)?,
            buffer: WindowBuffer::new(call.frame.clone()),
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
            is_ready: window.is_ready(),
        }
    }

    fn output(&mut self) -> StateOutput {
        debug_assert!(self.curr_window().is_ready);
        let aggregator = self.factory.create_agg_state();
        let return_value = None; // TODO(): do aggregation
        let removed_keys: BTreeSet<_> = self.buffer.slide().collect();
        StateOutput {
            return_value,
            evict_hint: if removed_keys.is_empty() {
                StateEvictHint::CannotEvict(
                    self.buffer
                        .curr_window_left()
                        .expect("sliding without removing, must have window left")
                        .0
                        .clone(),
                )
            } else {
                StateEvictHint::CanEvict(removed_keys)
            },
        }
    }
}
