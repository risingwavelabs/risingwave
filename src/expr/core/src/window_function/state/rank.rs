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

#[derive(EstimateSize)]
pub struct RowNumber {
    prev_rank: i64,
}

impl Default for RowNumber {
    fn default() -> Self {
        Self { prev_rank: 0 }
    }
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

#[derive(Debug, EstimateSize)]
pub struct DenseRank {
    prev_order_key: Option<MemcmpEncoded>,
    prev_rank: i64,
}

impl Default for DenseRank {
    fn default() -> Self {
        Self {
            prev_order_key: None,
            prev_rank: 0,
        }
    }
}

impl RankFuncCount for DenseRank {
    fn count(&mut self, curr_key: StateKey) -> i64 {
        println!(
            "[rc] DenseRank: {:?}, curr order key: {:?}",
            self, curr_key.order_key
        );

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
        assert!(self.curr_window().is_ready);
        let (rank, evict_hint) = self.slide_inner();
        Ok((Some(rank.into()), evict_hint))
    }

    fn slide_no_output(&mut self) -> Result<StateEvictHint> {
        assert!(self.curr_window().is_ready);
        let (_rank, evict_hint) = self.slide_inner();
        Ok(evict_hint)
    }
}
