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

use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_expr::expr::WindowFuncKind;
use smallvec::SmallVec;

use super::call::WindowFuncCall;
use super::MemcmpEncoded;

mod lag;
mod lead;

/// Unique and ordered identifier for a row in internal states.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StateKey {
    pub order_key: ScalarImpl,
    pub encoded_pk: MemcmpEncoded,
}

pub(super) struct StatePos<'a> {
    /// Only 2 cases in which the `key` is `None`:
    /// 1. The state is empty.
    /// 2. It's a pure preceding window, and all ready outputs are consumed.
    pub key: Option<&'a StateKey>,
    pub is_ready: bool,
}

pub(super) struct StateOutput {
    pub return_value: Datum,
    pub last_evicted_key: Option<StateKey>,
}

pub(super) trait WindowState {
    // TODO(rc): may append rows in batch like in `hash_agg`.
    /// Append a new input row to the state. The `key` is expected to be increasing.
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>);

    /// Get the current window frame position.
    fn curr_window(&self) -> StatePos<'_>;

    /// Return the output for the current ready window frame and push the window forward.
    fn slide(&mut self) -> StateOutput;
}

pub(super) fn create_window_state(call: &WindowFuncCall) -> Box<dyn WindowState> {
    use WindowFuncKind::*;
    match call.kind {
        Lag => Box::new(lag::LagState::new(&call.frame)),
        Lead => Box::new(lead::LeadState::new(&call.frame)),
        FirstValue => todo!(),
        LastValue => todo!(),
        NthValue => todo!(),
        Aggregate(_) => todo!(),
    }
}
