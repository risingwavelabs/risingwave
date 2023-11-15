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

use std::marker::PhantomData;

use risingwave_common::estimate_size::collections::VecDeque;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::Datum;
use risingwave_common::util::memcmp_encoding::MemcmpEncoded;
use smallvec::SmallVec;

use self::private::RankFuncCount;
use super::{StateEvictHint, StateKey, StatePos, WindowState};
use crate::window_function::WindowFuncCall;
use crate::Result;

mod private {
    use super::*;

    pub trait RankFuncCount: Default + EstimateSize {
        fn count(&mut self, curr_key: StateKey) -> i64;
    }
}

#[derive(Default, EstimateSize)]
pub struct RowNumber {
    prev_rank: i64,
}

impl RankFuncCount for RowNumber {
    fn count(&mut self, _curr_key: StateKey) -> i64 {
        let curr_rank = self.prev_rank + 1;
        self.prev_rank = curr_rank;
        curr_rank
    }
}

#[derive(EstimateSize)]
pub struct Rank {
    prev_order_key: Option<MemcmpEncoded>,
    prev_rank: i64,
    prev_pos_in_peer_group: i64,
}

impl Default for Rank {
    fn default() -> Self {
        Self {
            prev_order_key: None,
            prev_rank: 0,
            prev_pos_in_peer_group: 1, // first position in the fake starting peer group
        }
    }
}

impl RankFuncCount for Rank {
    fn count(&mut self, curr_key: StateKey) -> i64 {
        let (curr_rank, curr_pos_in_group) = if let Some(prev_order_key) = self.prev_order_key.as_ref() && prev_order_key == &curr_key.order_key {
            // current key is in the same peer group as the previous one
            (self.prev_rank, self.prev_pos_in_peer_group + 1)
        } else {
            // starting a new peer group
            (self.prev_rank + self.prev_pos_in_peer_group, 1)
        };
        self.prev_order_key = Some(curr_key.order_key);
        self.prev_rank = curr_rank;
        self.prev_pos_in_peer_group = curr_pos_in_group;
        curr_rank
    }
}

#[derive(Default, EstimateSize)]
pub struct DenseRank {
    prev_order_key: Option<MemcmpEncoded>,
    prev_rank: i64,
}

impl RankFuncCount for DenseRank {
    fn count(&mut self, curr_key: StateKey) -> i64 {
        let curr_rank = if let Some(prev_order_key) = self.prev_order_key.as_ref() && prev_order_key == &curr_key.order_key {
             // current key is in the same peer group as the previous one
            self.prev_rank
        } else {
            // starting a new peer group
            self.prev_rank + 1
        };
        self.prev_order_key = Some(curr_key.order_key);
        self.prev_rank = curr_rank;
        curr_rank
    }
}

/// Generic state for rank window functions including `row_number`, `rank` and `dense_rank`.
#[derive(EstimateSize)]
pub struct RankState<RF: RankFuncCount> {
    /// First state key of the partition.
    first_key: Option<StateKey>,
    /// State keys that are waiting to be outputted.
    buffer: VecDeque<StateKey>,
    /// Function-specific state.
    func_state: RF,
    _phantom: PhantomData<RF>,
}

impl<RF: RankFuncCount> RankState<RF> {
    pub fn new(_call: &WindowFuncCall) -> Self {
        Self {
            first_key: None,
            buffer: Default::default(),
            func_state: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn slide_inner(&mut self) -> (i64, StateEvictHint) {
        let curr_key = self
            .buffer
            .pop_front()
            .expect("should not slide forward when the current window is not ready");
        let rank = self.func_state.count(curr_key);
        // can't evict any state key in EOWC mode, because we can't recover from previous output now
        let evict_hint = StateEvictHint::CannotEvict(
            self.first_key
                .clone()
                .expect("should have appended some rows"),
        );
        (rank, evict_hint)
    }
}

impl<RF: RankFuncCount> WindowState for RankState<RF> {
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

    fn slide(&mut self) -> Result<(Datum, StateEvictHint)> {
        let (rank, evict_hint) = self.slide_inner();
        Ok((Some(rank.into()), evict_hint))
    }

    fn slide_no_output(&mut self) -> Result<StateEvictHint> {
        let (_rank, evict_hint) = self.slide_inner();
        Ok(evict_hint)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::memcmp_encoding;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::aggregate::AggArgs;
    use crate::window_function::{Frame, FrameBound, WindowFuncKind};

    fn create_state_key(order: i64, pk: i64) -> StateKey {
        StateKey {
            order_key: memcmp_encoding::encode_value(
                Some(ScalarImpl::from(order)),
                OrderType::ascending(),
            )
            .unwrap(),
            pk: OwnedRow::new(vec![Some(pk.into())]).into(),
        }
    }

    #[test]
    #[should_panic(expected = "should not slide forward when the current window is not ready")]
    fn test_rank_state_bad_use() {
        let call = WindowFuncCall {
            kind: WindowFuncKind::RowNumber,
            args: AggArgs::None,
            return_type: DataType::Int64,
            frame: Frame::rows(
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
            ),
        };
        let mut state = RankState::<RowNumber>::new(&call);
        assert!(state.curr_window().key.is_none());
        assert!(!state.curr_window().is_ready);
        _ = state.slide()
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
        let mut state = RankState::<RowNumber>::new(&call);
        assert!(state.curr_window().key.is_none());
        assert!(!state.curr_window().is_ready);
        state.append(create_state_key(1, 100), SmallVec::new());
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(1, 100));
        assert!(state.curr_window().is_ready);
        let (output, evict_hint) = state.slide().unwrap();
        assert_eq!(output.unwrap(), 1i64.into());
        match evict_hint {
            StateEvictHint::CannotEvict(state_key) => {
                assert_eq!(state_key, create_state_key(1, 100));
            }
            _ => unreachable!(),
        }
        assert!(!state.curr_window().is_ready);
        state.append(create_state_key(2, 103), SmallVec::new());
        state.append(create_state_key(2, 102), SmallVec::new());
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(2, 103));
        let (output, evict_hint) = state.slide().unwrap();
        assert_eq!(output.unwrap(), 2i64.into());
        match evict_hint {
            StateEvictHint::CannotEvict(state_key) => {
                assert_eq!(state_key, create_state_key(1, 100));
            }
            _ => unreachable!(),
        }
        assert_eq!(state.curr_window().key.unwrap(), &create_state_key(2, 102));
        let (output, _) = state.slide().unwrap();
        assert_eq!(output.unwrap(), 3i64.into());
    }

    #[test]
    fn test_rank_state() {
        let call = WindowFuncCall {
            kind: WindowFuncKind::Rank,
            args: AggArgs::None,
            return_type: DataType::Int64,
            frame: Frame::rows(
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
            ),
        };
        let mut state = RankState::<Rank>::new(&call);
        assert!(state.curr_window().key.is_none());
        assert!(!state.curr_window().is_ready);
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 103), SmallVec::new());
        state.append(create_state_key(2, 102), SmallVec::new());
        state.append(create_state_key(3, 106), SmallVec::new());
        state.append(create_state_key(3, 105), SmallVec::new());
        state.append(create_state_key(3, 104), SmallVec::new());
        state.append(create_state_key(8, 108), SmallVec::new());

        let mut outputs = vec![];
        while state.curr_window().is_ready {
            outputs.push(state.slide().unwrap().0)
        }

        assert_eq!(
            outputs,
            vec![
                Some(1i64.into()),
                Some(2i64.into()),
                Some(2i64.into()),
                Some(4i64.into()),
                Some(4i64.into()),
                Some(4i64.into()),
                Some(7i64.into())
            ]
        );
    }

    #[test]
    fn test_dense_rank_state() {
        let call = WindowFuncCall {
            kind: WindowFuncKind::DenseRank,
            args: AggArgs::None,
            return_type: DataType::Int64,
            frame: Frame::rows(
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
            ),
        };
        let mut state = RankState::<DenseRank>::new(&call);
        assert!(state.curr_window().key.is_none());
        assert!(!state.curr_window().is_ready);
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 103), SmallVec::new());
        state.append(create_state_key(2, 102), SmallVec::new());
        state.append(create_state_key(3, 106), SmallVec::new());
        state.append(create_state_key(3, 105), SmallVec::new());
        state.append(create_state_key(3, 104), SmallVec::new());
        state.append(create_state_key(8, 108), SmallVec::new());

        let mut outputs = vec![];
        while state.curr_window().is_ready {
            outputs.push(state.slide().unwrap().0)
        }

        assert_eq!(
            outputs,
            vec![
                Some(1i64.into()),
                Some(2i64.into()),
                Some(2i64.into()),
                Some(3i64.into()),
                Some(3i64.into()),
                Some(3i64.into()),
                Some(4i64.into())
            ]
        );
    }
}
