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

/// The watermark of the order key that closes a window, i.e., no future row can fall into it.
/// A watermark `w` promises that no row with order value `< w` arrives afterwards.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, EstimateSize)]
pub struct WatermarkBound {
    /// The order value, memcmp-encoded in the same way as [`StateKey::order_key`].
    pub value: MemcmpEncoded,
    /// Whether the watermark must be greater than `value`, rather than greater than or equal to.
    pub exclusive: bool,
}

impl WatermarkBound {
    pub fn is_satisfied_by(&self, watermark: &MemcmpEncoded) -> bool {
        if self.exclusive {
            watermark > &self.value
        } else {
            watermark >= &self.value
        }
    }
}

/// Whether the current window is closed, i.e., no future row can fall into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    /// The window is closed once the watermark of the order key satisfies the bound.
    OnWatermark(WatermarkBound),
    /// Only future rows can close the window.
    WaitForInput,
}

impl Readiness {
    /// The readiness of windows that must all be closed.
    pub fn and(self, other: Self) -> Self {
        use Readiness::*;
        match (self, other) {
            (WaitForInput, _) | (_, WaitForInput) => WaitForInput,
            (Ready, r) | (r, Ready) => r,
            // At the same value, the exclusive bound is the stricter one.
            (OnWatermark(a), OnWatermark(b)) => OnWatermark(std::cmp::max(a, b)),
        }
    }

    pub fn is_ready(&self, watermark: Option<&MemcmpEncoded>) -> bool {
        match self {
            Readiness::Ready => true,
            Readiness::OnWatermark(bound) => watermark.is_some_and(|w| bound.is_satisfied_by(w)),
            Readiness::WaitForInput => false,
        }
    }
}

#[derive(Debug)]
pub struct StatePos<'a> {
    /// Only 2 cases in which the `key` is `None`:
    /// 1. The state is empty.
    /// 2. All rows in the state are output, and some are retained for the windows of future rows.
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
                if keys.is_empty() {
                    CannotEvict(still_required)
                } else {
                    CanEvict(keys)
                }
            }
        }
    }
}

/// Snapshot of a window state for persistence and recovery.
#[derive(Debug, Clone)]
pub struct WindowStateSnapshot {
    /// The key of the last output row.
    pub last_output_key: Option<StateKey>,
    /// Function-specific state. The variant must match the window function type;
    /// a mismatch is detected at restore time and returned as an error.
    pub function_state: risingwave_pb::window_function::window_state_snapshot::FunctionState,
}

pub trait WindowState: EstimateSize {
    // TODO(rc): may append rows in batch like in `hash_agg`.
    /// Append a new input row to the state. The `key` is expected to be increasing.
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>);

    /// Get the current window frame position.
    fn curr_window(&self) -> StatePos<'_>;

    /// Get the readiness of the current window, given that the rows are appended in the order of
    /// the order key, so no future row is less than the last one. Unlike [`StatePos::is_ready`],
    /// which is conservative and must not change for the recovery of existing jobs, this is exact.
    fn readiness(&self) -> Readiness {
        if self.curr_window().is_ready {
            Readiness::Ready
        } else {
            Readiness::WaitForInput
        }
    }

    /// Whether no row is retained in the state.
    fn is_empty(&self) -> bool;

    /// Slide the window frame forward and collect the output and evict hint. Similar to `Iterator::next`.
    fn slide(&mut self) -> Result<(Datum, StateEvictHint)>;

    /// Slide the window frame forward and collect the evict hint. Don't calculate the output if possible.
    fn slide_no_output(&mut self) -> Result<StateEvictHint>;

    /// Enable persistence for this window state. When enabled, the state may return
    /// `CanEvict` hints and should support snapshot/restore.
    fn enable_persistence(&mut self) {}

    /// Create a snapshot of the current state for persistence.
    /// Returns `None` if the state doesn't support persistence.
    fn snapshot(&self) -> Option<WindowStateSnapshot> {
        None
    }

    /// Restore the state from a snapshot. Called during recovery before replaying rows.
    fn restore(&mut self, _snapshot: WindowStateSnapshot) -> Result<()> {
        Ok(())
    }
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
                call.return_type,
            )))
        },
        |f| f(call),
    )
}

/// Test that merging evict hints from mixed persistent-numbering and non-numbering window
/// functions does not panic. This reproduces the scenario where e.g. `row_number()`, `lag()`,
/// `rank()` are used together in an EOWC query:
///
///   1. `row_number` (persistent) -> `CanEvict({key})`
///   2. `lag` (non-persistent)    -> `CannotEvict(key)`
///      merge(1, 2) = `CanEvict({})` -- empty set, violates the "shouldn't be empty" invariant
///   3. `rank` (persistent)       -> `CanEvict({key})`
///      merge(empty, 3) panics on `.last().unwrap()`
#[cfg(test)]
mod tests {
    use super::*;

    fn make_state_key(order: i64, pk: i64) -> StateKey {
        use risingwave_common::row::OwnedRow;
        use risingwave_common::types::ScalarImpl;
        StateKey {
            order_key: MemcmpEncoded::from(order.to_be_bytes().to_vec()),
            pk: OwnedRow::new(vec![Some(ScalarImpl::Int64(pk))]).into(),
        }
    }

    /// Simulates `row_number()` -> `CanEvict({K})`, `lag()` -> `CannotEvict(K)`,
    /// `rank()` -> `CanEvict({K})`.
    /// The two-way merge must fall back to `CannotEvict` (not an empty `CanEvict`),
    /// so the three-way merge doesn't panic on `.last().unwrap()`.
    #[test]
    fn test_merge_mixed_persistent_and_non_persistent() {
        let key = make_state_key(1, 100);

        // Two-way: CanEvict({K}) + CannotEvict(K) -> CannotEvict(K)
        let merged = StateEvictHint::CanEvict(BTreeSet::from([key.clone()]))
            .merge(StateEvictHint::CannotEvict(key.clone()));
        assert!(matches!(&merged, StateEvictHint::CannotEvict(k) if k == &key));

        // Three-way: CannotEvict(K) + CanEvict({K}) -> CannotEvict(K)
        let merged = merged.merge(StateEvictHint::CanEvict(BTreeSet::from([key.clone()])));
        assert!(matches!(&merged, StateEvictHint::CannotEvict(k) if k == &key));
    }

    #[test]
    fn test_readiness_and() {
        use Readiness::*;
        let encode = |value: i64| MemcmpEncoded::from(value.to_be_bytes().to_vec());
        let bound = |value: i64, exclusive: bool| WatermarkBound {
            value: encode(value),
            exclusive,
        };

        assert_eq!(
            Ready.and(OnWatermark(bound(1, false))),
            OnWatermark(bound(1, false))
        );
        assert_eq!(OnWatermark(bound(1, false)).and(WaitForInput), WaitForInput);
        assert_eq!(
            OnWatermark(bound(2, false)).and(OnWatermark(bound(1, true))),
            OnWatermark(bound(2, false))
        );
        assert_eq!(
            OnWatermark(bound(1, false)).and(OnWatermark(bound(1, true))),
            OnWatermark(bound(1, true))
        );

        assert!(bound(1, false).is_satisfied_by(&encode(1)));
        assert!(!bound(1, true).is_satisfied_by(&encode(1)));
        assert!(bound(1, true).is_satisfied_by(&encode(2)));
        assert!(!OnWatermark(bound(1, false)).is_ready(None));
    }
}
