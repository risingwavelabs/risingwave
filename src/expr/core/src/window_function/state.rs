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

use itertools::Itertools;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{Datum, DefaultOrdered};
use risingwave_common::util::memcmp_encoding::MemcmpEncoded;
use risingwave_common_estimate_size::EstimateSize;
use smallvec::SmallVec;

use super::WindowFuncCall;
use crate::{ExprError, Result};

/// Unique and ordered identifier for a row in internal states.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, EstimateSize)]
pub struct StateKey {
    pub order_key: MemcmpEncoded,
    pub pk: DefaultOrdered<OwnedRow>,
}

#[derive(Debug)]
pub struct StatePos<'a> {
    /// Only 2 cases in which the `key` is `None`:
    /// 1. The state is empty.
    /// 2. It's a pure preceding window, and all ready outputs are consumed.
    pub key: Option<&'a StateKey>,
    pub is_ready: bool,
}

#[derive(Debug)]
pub enum StateEvictHint {
    /// Use a set instead of a single key to avoid state table iter or too many range delete.
    /// Shouldn't be empty set.
    CanEvict(BTreeSet<StateKey>),
    /// State keys from the specified key are still required, so must be kept in the state table.
    CannotEvict(StateKey),
}

impl StateEvictHint {
    pub fn merge(self, other: StateEvictHint) -> StateEvictHint {
        use StateEvictHint::*;
        match (self, other) {
            (CanEvict(a), CanEvict(b)) => {
                // Example:
                // a = CanEvict({1, 2, 3})
                // b = CanEvict({2, 3, 4})
                // a.merge(b) = CanEvict({1, 2, 3})
                let a_last = a.last().unwrap();
                let b_last = b.last().unwrap();
                let last = std::cmp::min(a_last, b_last).clone();
                CanEvict(
                    a.into_iter()
                        .take_while(|k| k <= &last)
                        .chain(b.into_iter().take_while(|k| k <= &last))
                        .collect(),
                )
            }
            (CannotEvict(a), CannotEvict(b)) => {
                // Example:
                // a = CannotEvict(2), meaning keys < 2 can be evicted
                // b = CannotEvict(3), meaning keys < 3 can be evicted
                // a.merge(b) = CannotEvict(2)
                CannotEvict(std::cmp::min(a, b))
            }
            (CanEvict(mut keys), CannotEvict(still_required))
            | (CannotEvict(still_required), CanEvict(mut keys)) => {
                // Example:
                // a = CanEvict({1, 2, 3})
                // b = CannotEvict(3)
                // a.merge(b) = CanEvict({1, 2})
                keys.split_off(&still_required);
                CanEvict(keys)
            }
        }
    }
}

pub trait WindowState: EstimateSize {
    // TODO(rc): may append rows in batch like in `hash_agg`.
    /// Append a new input row to the state. The `key` is expected to be increasing.
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>);

    /// Get the current window frame position.
    fn curr_window(&self) -> StatePos<'_>;

    /// Slide the window frame forward and collect the output and evict hint. Similar to `Iterator::next`.
    fn slide(&mut self) -> Result<(Datum, StateEvictHint)>;

    /// Slide the window frame forward and collect the evict hint. Don't calculate the output if possible.
    fn slide_no_output(&mut self) -> Result<StateEvictHint>;
}

pub type BoxedWindowState = Box<dyn WindowState + Send + Sync>;

#[linkme::distributed_slice]
pub static WINDOW_STATE_BUILDERS: [fn(&WindowFuncCall) -> Result<BoxedWindowState>];

pub fn create_window_state(call: &WindowFuncCall) -> Result<BoxedWindowState> {
    // we expect only one builder function in `expr_impl/window_function/mod.rs`
    let builder = WINDOW_STATE_BUILDERS.iter().next();
    builder.map_or_else(
        || {
            Err(ExprError::UnsupportedFunction(format!(
                "{}({}) -> {}",
                call.kind,
                call.args.arg_types().iter().format(", "),
                &call.return_type,
            )))
        },
        |f| f(call),
    )
}
