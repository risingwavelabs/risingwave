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

use risingwave_common::estimate_size::collections::VecDeque;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::Datum;
use smallvec::SmallVec;

use super::{StateEvictHint, StateKey, StatePos, WindowState};
use crate::function::window::WindowFuncCall;
use crate::Result;

#[derive(EstimateSize)]
pub struct RowNumberState {
    first_key: Option<StateKey>,
    buffer: VecDeque<StateKey>,
    curr_row_number: i64,
}

impl RowNumberState {
    pub fn new(_call: &WindowFuncCall) -> Self {
        Self {
            first_key: None,
            buffer: Default::default(),
            curr_row_number: 1,
        }
    }
}

impl WindowState for RowNumberState {
    fn append(&mut self, key: StateKey, _args: SmallVec<[Datum; 2]>) {
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.buffer.push_back(key);
    }

    fn curr_window(&self) -> StatePos<'_> {
        let curr_key = self.buffer.front();
        StatePos {
            key: curr_key,
            is_ready: curr_key.is_some(),
        }
    }

    fn curr_output(&self) -> Result<Datum> {
        if self.curr_window().is_ready {
            Ok(Some(self.curr_row_number.into()))
        } else {
            Ok(None)
        }
    }

    fn slide_forward(&mut self) -> StateEvictHint {
        self.curr_row_number += 1;
        self.buffer
            .pop_front()
            .expect("should not slide forward when the current window is not ready");
        // can't evict any state key in EOWC mode, because we can't recover from previous output now
        StateEvictHint::CannotEvict(
            self.first_key
                .clone()
                .expect("should have appended some rows"),
        )
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::agg::AggArgs;
    use crate::function::window::{Frame, FrameBound, WindowFuncKind};

    fn create_state_key(pk: i64) -> StateKey {
        StateKey {
            order_key: vec![].into(), // doesn't matter here
            pk: OwnedRow::new(vec![Some(pk.into())]).into(),
        }
    }

    #[test]
    fn test_row_number_state() {
        let call = WindowFuncCall {
            kind: WindowFuncKind::RowNumber,
            args: AggArgs::None,
            return_type: DataType::Int64,
            frame: Frame::rows(
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
            ),
        };
        let mut state = RowNumberState::new(&call);
        assert!(state.curr_window().key.is_none());
        assert!(!state.curr_window().is_ready);
        assert!(state.curr_output().unwrap().is_none());
        state.append(create_state_key(100), SmallVec::new());
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(100));
        assert!(state.curr_window().is_ready);
        assert_eq!(state.curr_output().unwrap().unwrap(), 1i64.into());
        state.append(create_state_key(103), SmallVec::new());
        state.append(create_state_key(102), SmallVec::new());
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(100));
        let evict_hint = state.slide_forward();
        match evict_hint {
            StateEvictHint::CannotEvict(state_key) => {
                assert_eq!(state_key, create_state_key(100));
            }
            _ => unreachable!(),
        }
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(103));
        assert_eq!(state.curr_output().unwrap().unwrap(), 2i64.into());
        let evict_hint = state.slide_forward();
        match evict_hint {
            StateEvictHint::CannotEvict(state_key) => {
                assert_eq!(state_key, create_state_key(100));
            }
            _ => unreachable!(),
        }
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(102));
        assert_eq!(state.curr_output().unwrap().unwrap(), 3i64.into());
    }
}
