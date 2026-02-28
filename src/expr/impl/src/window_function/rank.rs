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
use std::marker::PhantomData;

use risingwave_common::types::Datum;
use risingwave_common::util::memcmp_encoding::MemcmpEncoded;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVecDeque;
use risingwave_expr::window_function::{
    StateEvictHint, StateKey, StatePos, WindowFuncCall, WindowState, WindowStateSnapshot,
};
use risingwave_expr::{ExprError, Result};
use risingwave_pb::window_function::window_state_snapshot::FunctionState;
use risingwave_pb::window_function::{DenseRankState, RankState as RankStateProto, RowNumberState};
use smallvec::SmallVec;

use self::private::RankFuncCount;

mod private {
    use super::*;

    pub trait RankFuncCount: Default + EstimateSize {
        /// Count and return the rank for the given key, updating internal state.
        fn count(&mut self, curr_key: StateKey) -> i64;

        /// Convert the function state to the proto oneof variant for persistence.
        fn to_proto_state(&self) -> FunctionState;

        /// Restore the function state from the proto oneof variant.
        /// Returns an error if the variant does not match this function type.
        fn from_proto_state(state: FunctionState) -> Result<Self>
        where
            Self: Sized;
    }
}

#[derive(Default, EstimateSize)]
pub(super) struct RowNumber {
    prev_rank: i64,
}

impl RankFuncCount for RowNumber {
    fn count(&mut self, _curr_key: StateKey) -> i64 {
        let curr_rank = self.prev_rank + 1;
        self.prev_rank = curr_rank;
        curr_rank
    }

    fn to_proto_state(&self) -> FunctionState {
        FunctionState::RowNumberState(RowNumberState {
            prev_rank: self.prev_rank,
        })
    }

    fn from_proto_state(state: FunctionState) -> Result<Self> {
        match state {
            FunctionState::RowNumberState(s) => Ok(Self {
                prev_rank: s.prev_rank,
            }),
            other => Err(ExprError::Internal(anyhow::anyhow!(
                "expected RowNumberState, got {other:?}"
            ))),
        }
    }
}

#[derive(EstimateSize)]
pub(super) struct Rank {
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
        let (curr_rank, curr_pos_in_group) = if let Some(prev_order_key) =
            self.prev_order_key.as_ref()
            && prev_order_key == &curr_key.order_key
        {
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

    fn to_proto_state(&self) -> FunctionState {
        FunctionState::RankState(RankStateProto {
            prev_order_key: self.prev_order_key.as_ref().map(|k| k.to_vec()),
            prev_rank: self.prev_rank,
            prev_pos_in_peer_group: self.prev_pos_in_peer_group,
        })
    }

    fn from_proto_state(state: FunctionState) -> Result<Self> {
        match state {
            FunctionState::RankState(s) => Ok(Self {
                prev_order_key: s.prev_order_key.map(Into::into),
                prev_rank: s.prev_rank,
                prev_pos_in_peer_group: s.prev_pos_in_peer_group,
            }),
            other => Err(ExprError::Internal(anyhow::anyhow!(
                "expected RankState, got {other:?}"
            ))),
        }
    }
}

#[derive(Default, EstimateSize)]
pub(super) struct DenseRank {
    prev_order_key: Option<MemcmpEncoded>,
    prev_rank: i64,
}

impl RankFuncCount for DenseRank {
    fn count(&mut self, curr_key: StateKey) -> i64 {
        let curr_rank = if let Some(prev_order_key) = self.prev_order_key.as_ref()
            && prev_order_key == &curr_key.order_key
        {
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

    fn to_proto_state(&self) -> FunctionState {
        FunctionState::DenseRankState(DenseRankState {
            prev_order_key: self.prev_order_key.as_ref().map(|k| k.to_vec()),
            prev_rank: self.prev_rank,
        })
    }

    fn from_proto_state(state: FunctionState) -> Result<Self> {
        match state {
            FunctionState::DenseRankState(s) => Ok(Self {
                prev_order_key: s.prev_order_key.map(Into::into),
                prev_rank: s.prev_rank,
            }),
            other => Err(ExprError::Internal(anyhow::anyhow!(
                "expected DenseRankState, got {other:?}"
            ))),
        }
    }
}

/// Generic state for rank window functions including `row_number`, `rank` and `dense_rank`.
#[derive(EstimateSize)]
pub(super) struct RankState<RF: RankFuncCount> {
    /// First state key of the partition.
    first_key: Option<StateKey>,
    /// State keys that are waiting to be outputted.
    buffer: EstimatedVecDeque<StateKey>,
    /// Function-specific state.
    func_state: RF,
    /// Whether persistence is enabled for this state.
    persistence_enabled: bool,
    /// The key of the last output row (for snapshot).
    last_output_key: Option<StateKey>,
    /// During recovery, skip updating `func_state` for rows up to and including this key.
    recover_skip_until: Option<StateKey>,
    _phantom: PhantomData<RF>,
}

impl<RF: RankFuncCount> RankState<RF> {
    pub fn new(_call: &WindowFuncCall) -> Self {
        Self {
            first_key: None,
            buffer: Default::default(),
            func_state: Default::default(),
            persistence_enabled: false,
            last_output_key: None,
            recover_skip_until: None,
            _phantom: PhantomData,
        }
    }

    fn slide_inner(&mut self) -> (i64, StateEvictHint) {
        let curr_key = self
            .buffer
            .pop_front()
            .expect("should not slide forward when the current window is not ready");
        let rank = self.func_state.count(curr_key.clone());

        // Track last output key for persistence
        self.last_output_key = Some(curr_key.clone());

        let evict_hint = if self.persistence_enabled {
            // When persistence is enabled, we can evict the just-output key
            let mut evict_set = BTreeSet::new();
            evict_set.insert(curr_key);
            StateEvictHint::CanEvict(evict_set)
        } else {
            // Can't evict any state key in EOWC mode without persistence,
            // because we can't recover from previous output
            StateEvictHint::CannotEvict(
                self.first_key
                    .clone()
                    .expect("should have appended some rows"),
            )
        };
        (rank, evict_hint)
    }

    fn slide_no_output_inner(&mut self) -> StateEvictHint {
        let curr_key = self
            .buffer
            .pop_front()
            .expect("should not slide forward when the current window is not ready");

        // Check if we should skip counting (during recovery)
        let should_skip = self
            .recover_skip_until
            .as_ref()
            .is_some_and(|skip_key| &curr_key <= skip_key);

        if !should_skip {
            // Normal counting
            self.func_state.count(curr_key.clone());
        }

        // Clear recover_skip_until when we've processed the target key
        if self
            .recover_skip_until
            .as_ref()
            .is_some_and(|skip_key| &curr_key == skip_key)
        {
            self.recover_skip_until = None;
        }

        // Track last output key for persistence
        self.last_output_key = Some(curr_key.clone());

        if self.persistence_enabled {
            let mut evict_set = BTreeSet::new();
            evict_set.insert(curr_key);
            StateEvictHint::CanEvict(evict_set)
        } else {
            StateEvictHint::CannotEvict(
                self.first_key
                    .clone()
                    .expect("should have appended some rows"),
            )
        }
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
        let evict_hint = self.slide_no_output_inner();
        Ok(evict_hint)
    }

    fn enable_persistence(&mut self) {
        self.persistence_enabled = true;
    }

    fn snapshot(&self) -> Option<WindowStateSnapshot> {
        if !self.persistence_enabled {
            return None;
        }
        Some(WindowStateSnapshot {
            last_output_key: self.last_output_key.clone(),
            function_state: self.func_state.to_proto_state(),
        })
    }

    fn restore(&mut self, snapshot: WindowStateSnapshot) -> Result<()> {
        self.func_state = RF::from_proto_state(snapshot.function_state)?;
        // Set recover_skip_until so that slide_no_output skips counting for rows
        // that have already been counted before the snapshot was taken.
        self.recover_skip_until = snapshot.last_output_key;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::memcmp_encoding;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::aggregate::AggArgs;
    use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

    use super::*;

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
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
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
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
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
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
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
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
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

    fn create_call(kind: WindowFuncKind) -> WindowFuncCall {
        WindowFuncCall {
            kind,
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
            frame: Frame::rows(
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
            ),
        }
    }

    #[test]
    fn test_row_number_snapshot_restore_roundtrip() {
        let call = create_call(WindowFuncKind::RowNumber);
        let mut state = RankState::<RowNumber>::new(&call);
        state.enable_persistence();

        // Process some rows
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 101), SmallVec::new());
        state.append(create_state_key(3, 102), SmallVec::new());

        // Output first two rows
        let (output1, _) = state.slide().unwrap();
        assert_eq!(output1.unwrap(), 1i64.into());
        let (output2, _) = state.slide().unwrap();
        assert_eq!(output2.unwrap(), 2i64.into());

        // Take snapshot
        let snapshot = state.snapshot().unwrap();
        assert!(snapshot.last_output_key.is_some());
        assert_eq!(
            snapshot.last_output_key.as_ref().unwrap(),
            &create_state_key(2, 101)
        );

        // Create new state and restore
        let mut new_state = RankState::<RowNumber>::new(&call);
        new_state.enable_persistence();
        new_state.restore(snapshot).unwrap();

        // Continue from where we left off (row 3)
        new_state.append(create_state_key(3, 102), SmallVec::new());
        new_state.append(create_state_key(4, 103), SmallVec::new());

        // Output should continue from rank 3
        let (output3, _) = new_state.slide().unwrap();
        assert_eq!(output3.unwrap(), 3i64.into());
        let (output4, _) = new_state.slide().unwrap();
        assert_eq!(output4.unwrap(), 4i64.into());
    }

    #[test]
    fn test_rank_snapshot_restore_roundtrip() {
        let call = create_call(WindowFuncKind::Rank);
        let mut state = RankState::<Rank>::new(&call);
        state.enable_persistence();

        // Add rows with ties
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 101), SmallVec::new());
        state.append(create_state_key(2, 102), SmallVec::new()); // tie

        // Output rows
        let (output1, _) = state.slide().unwrap();
        assert_eq!(output1.unwrap(), 1i64.into());
        let (output2, _) = state.slide().unwrap();
        assert_eq!(output2.unwrap(), 2i64.into()); // first in tie group
        let (output3, _) = state.slide().unwrap();
        assert_eq!(output3.unwrap(), 2i64.into()); // second in tie group

        // Take snapshot
        let snapshot = state.snapshot().unwrap();

        // Create new state and restore
        let mut new_state = RankState::<Rank>::new(&call);
        new_state.enable_persistence();
        new_state.restore(snapshot).unwrap();

        // Add more rows
        new_state.append(create_state_key(3, 103), SmallVec::new());

        // Output should be rank 4 (since 2 items tied at rank 2)
        let (output4, _) = new_state.slide().unwrap();
        assert_eq!(output4.unwrap(), 4i64.into());
    }

    #[test]
    fn test_dense_rank_snapshot_restore_roundtrip() {
        let call = create_call(WindowFuncKind::DenseRank);
        let mut state = RankState::<DenseRank>::new(&call);
        state.enable_persistence();

        // Add rows with ties
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 101), SmallVec::new());
        state.append(create_state_key(2, 102), SmallVec::new()); // tie

        // Output rows
        let (output1, _) = state.slide().unwrap();
        assert_eq!(output1.unwrap(), 1i64.into());
        let (output2, _) = state.slide().unwrap();
        assert_eq!(output2.unwrap(), 2i64.into());
        let (output3, _) = state.slide().unwrap();
        assert_eq!(output3.unwrap(), 2i64.into()); // same rank due to tie

        // Take snapshot
        let snapshot = state.snapshot().unwrap();

        // Create new state and restore
        let mut new_state = RankState::<DenseRank>::new(&call);
        new_state.enable_persistence();
        new_state.restore(snapshot).unwrap();

        // Add more rows
        new_state.append(create_state_key(3, 103), SmallVec::new());

        // Output should be rank 3 (dense rank increments by 1)
        let (output4, _) = new_state.slide().unwrap();
        assert_eq!(output4.unwrap(), 3i64.into());
    }

    #[test]
    fn test_recovery_skip_logic() {
        // Test that slide_no_output correctly skips counting for recovered rows
        let call = create_call(WindowFuncKind::RowNumber);
        let mut state = RankState::<RowNumber>::new(&call);
        state.enable_persistence();

        // Process initial rows
        state.append(create_state_key(1, 100), SmallVec::new());
        state.append(create_state_key(2, 101), SmallVec::new());
        state.append(create_state_key(3, 102), SmallVec::new());

        let (_, _) = state.slide().unwrap();
        let (_, _) = state.slide().unwrap();

        // Take snapshot after row 2
        let snapshot = state.snapshot().unwrap();
        assert_eq!(
            snapshot.last_output_key.as_ref().unwrap(),
            &create_state_key(2, 101)
        );

        // Create new state and restore
        let mut new_state = RankState::<RowNumber>::new(&call);
        new_state.enable_persistence();
        new_state.restore(snapshot).unwrap();

        // Simulate recovery: replay rows from state table
        // First two rows should be skipped in slide_no_output
        new_state.append(create_state_key(1, 100), SmallVec::new());
        new_state.append(create_state_key(2, 101), SmallVec::new());
        new_state.append(create_state_key(3, 102), SmallVec::new());
        new_state.append(create_state_key(4, 103), SmallVec::new());

        // Use slide_no_output for recovery (skips counting for rows <= snapshot key)
        let _ = new_state.slide_no_output().unwrap();
        let _ = new_state.slide_no_output().unwrap();

        // Now output the remaining rows - should continue correctly
        let (output3, _) = new_state.slide().unwrap();
        assert_eq!(output3.unwrap(), 3i64.into());
        let (output4, _) = new_state.slide().unwrap();
        assert_eq!(output4.unwrap(), 4i64.into());
    }

    #[test]
    fn test_eviction_hint_with_persistence() {
        let call = create_call(WindowFuncKind::RowNumber);

        // Test without persistence - should return CannotEvict
        let mut state_no_persist = RankState::<RowNumber>::new(&call);
        state_no_persist.append(create_state_key(1, 100), SmallVec::new());
        let (_, evict_hint) = state_no_persist.slide().unwrap();
        match evict_hint {
            StateEvictHint::CannotEvict(_) => {}
            StateEvictHint::CanEvict(_) => panic!("expected CannotEvict without persistence"),
        }

        // Test with persistence - should return CanEvict
        let mut state_persist = RankState::<RowNumber>::new(&call);
        state_persist.enable_persistence();
        state_persist.append(create_state_key(1, 100), SmallVec::new());
        let (_, evict_hint) = state_persist.slide().unwrap();
        match evict_hint {
            StateEvictHint::CanEvict(keys) => {
                assert_eq!(keys.len(), 1);
                assert!(keys.contains(&create_state_key(1, 100)));
            }
            StateEvictHint::CannotEvict(_) => panic!("expected CanEvict with persistence"),
        }
    }

    #[test]
    fn test_snapshot_returns_none_without_persistence() {
        let call = create_call(WindowFuncKind::RowNumber);
        let mut state = RankState::<RowNumber>::new(&call);
        // Don't enable persistence
        state.append(create_state_key(1, 100), SmallVec::new());
        let (_, _) = state.slide().unwrap();

        // Snapshot should return None
        assert!(state.snapshot().is_none());
    }

    #[test]
    fn test_restore_from_empty_starts_fresh() {
        // Test that restoring with default state works correctly
        let call = create_call(WindowFuncKind::RowNumber);
        let mut state = RankState::<RowNumber>::new(&call);
        state.enable_persistence();

        // Create a snapshot with default func_state (prev_rank = 0)
        let snapshot = WindowStateSnapshot {
            last_output_key: None,
            function_state: RowNumber::default().to_proto_state(),
        };

        state.restore(snapshot).unwrap();

        // Add rows and verify output starts from 1
        state.append(create_state_key(1, 100), SmallVec::new());
        let (output, _) = state.slide().unwrap();
        assert_eq!(output.unwrap(), 1i64.into());
    }

    #[test]
    fn test_wrong_function_state_type_is_rejected() {
        // Restoring a RowNumber state with a RankState payload should fail fast.
        let call = create_call(WindowFuncKind::RowNumber);
        let mut state = RankState::<RowNumber>::new(&call);
        state.enable_persistence();

        // Snapshot contains a RankState variant instead of RowNumberState.
        let snapshot = WindowStateSnapshot {
            last_output_key: None,
            function_state: Rank::default().to_proto_state(),
        };

        assert!(state.restore(snapshot).is_err());
    }
}
