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

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};
use std::hash::Hash;

use super::Watermark;

const MAX_STAGED_WATERMARKS_PER_INPUT: usize = 1024;

#[derive(Default, Debug)]
pub(super) struct StagedWatermarks {
    in_heap: bool,
    staged: VecDeque<Watermark>,
}

impl StagedWatermarks {
    fn push_with_cap(&mut self, watermark: Watermark) {
        if self.staged.len() >= MAX_STAGED_WATERMARKS_PER_INPUT {
            let _ = self.staged.pop_front();
        }
        self.staged.push_back(watermark);
    }
}

pub(super) struct BufferedWatermarks<Id> {
    /// We store the smallest watermark of each upstream, because the next watermark to emit is
    /// among them.
    pub first_buffered_watermarks: BinaryHeap<Reverse<(Watermark, Id)>>,
    /// We buffer other watermarks of each upstream. The next-to-smallest one will become the
    /// smallest when the smallest is emitted and be moved into heap.
    pub other_buffered_watermarks: BTreeMap<Id, StagedWatermarks>,
}

impl<Id: Ord + Hash + std::fmt::Debug> BufferedWatermarks<Id> {
    pub fn with_ids(buffer_ids: impl IntoIterator<Item = Id>) -> Self {
        let other_buffered_watermarks: BTreeMap<_, _> = buffer_ids
            .into_iter()
            .map(|id| (id, Default::default()))
            .collect();
        let first_buffered_watermarks = BinaryHeap::with_capacity(other_buffered_watermarks.len());

        BufferedWatermarks {
            first_buffered_watermarks,
            other_buffered_watermarks,
        }
    }

    pub fn add_buffers(&mut self, buffer_ids: impl IntoIterator<Item = Id>) {
        buffer_ids.into_iter().for_each(|id| {
            self.other_buffered_watermarks
                .try_insert(id, Default::default())
                .unwrap();
        });
    }

    pub fn clear(&mut self) {
        self.first_buffered_watermarks.clear();
        self.other_buffered_watermarks
            .values_mut()
            .for_each(|staged_watermarks| {
                std::mem::take(staged_watermarks);
            });
    }

    /// Handle a new watermark message. Optionally returns the watermark message to emit and the
    /// buffer id.
    pub fn handle_watermark(&mut self, buffer_id: Id, watermark: Watermark) -> Option<Watermark> {
        // Note: The staged watermark buffer should be created before handling the watermark.
        let staged = self.other_buffered_watermarks.get_mut(&buffer_id).unwrap();

        if staged.in_heap {
            staged.push_with_cap(watermark);
            None
        } else {
            staged.in_heap = true;
            self.first_buffered_watermarks
                .push(Reverse((watermark, buffer_id)));
            self.check_watermark_heap()
        }
    }

    /// Check the watermark heap and decide whether to emit a watermark message.
    pub fn check_watermark_heap(&mut self) -> Option<Watermark> {
        let len = self.other_buffered_watermarks.len();
        let mut watermark_to_emit = None;
        while !self.first_buffered_watermarks.is_empty()
            && (self.first_buffered_watermarks.len() == len
                || watermark_to_emit.as_ref().is_some_and(|watermark| {
                    watermark == &self.first_buffered_watermarks.peek().unwrap().0.0
                }))
        {
            let Reverse((watermark, id)) = self.first_buffered_watermarks.pop().unwrap();
            watermark_to_emit = Some(watermark);
            let staged = self.other_buffered_watermarks.get_mut(&id).unwrap();
            if let Some(first) = staged.staged.pop_front() {
                self.first_buffered_watermarks.push(Reverse((first, id)));
            } else {
                staged.in_heap = false;
            }
        }
        watermark_to_emit
    }

    /// Remove buffers and return watermark to emit.
    pub fn remove_buffer(&mut self, buffer_ids_to_remove: HashSet<Id>) -> Option<Watermark> {
        self.first_buffered_watermarks
            .retain(|Reverse((_, id))| !buffer_ids_to_remove.contains(id));
        self.other_buffered_watermarks
            .retain(|id, _| !buffer_ids_to_remove.contains(id));
        // Call `check_watermark_heap` in case the only buffers(s) that does not have watermark in
        // heap is removed
        self.check_watermark_heap()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::{BufferedWatermarks, MAX_STAGED_WATERMARKS_PER_INPUT, Watermark};

    fn wm(v: i64) -> Watermark {
        Watermark::new(0, DataType::Int64, ScalarImpl::Int64(v))
    }

    #[test]
    fn test_staged_watermarks_capped() {
        // One upstream keeps sending while the other has not sent any watermark yet.
        // Staged watermarks should be capped.
        let mut stalled_case = BufferedWatermarks::with_ids([1_u8, 2_u8]);
        assert!(stalled_case.handle_watermark(1, wm(1)).is_none());

        for i in 2..=(MAX_STAGED_WATERMARKS_PER_INPUT as i64 + 5) {
            assert!(stalled_case.handle_watermark(1, wm(i)).is_none());
        }

        let staged = &stalled_case
            .other_buffered_watermarks
            .get(&1)
            .unwrap()
            .staged;
        assert_eq!(staged.len(), MAX_STAGED_WATERMARKS_PER_INPUT);
        assert_eq!(staged.front().unwrap().val, ScalarImpl::Int64(6));
        assert_eq!(
            staged.back().unwrap().val,
            ScalarImpl::Int64(MAX_STAGED_WATERMARKS_PER_INPUT as i64 + 5)
        );

        // If the stalled upstream becomes active again, watermark propagation should resume and
        // eventually drain the buffered watermarks.
        for i in 1..=(MAX_STAGED_WATERMARKS_PER_INPUT as i64 + 5) {
            let emitted = stalled_case
                .handle_watermark(2, wm(i))
                .expect("watermark should be emitted when stalled upstream becomes active again");
            assert_eq!(emitted.val, ScalarImpl::Int64(i));
        }
        let staged_1 = &stalled_case
            .other_buffered_watermarks
            .get(&1)
            .unwrap()
            .staged;
        let staged_2 = &stalled_case
            .other_buffered_watermarks
            .get(&2)
            .unwrap()
            .staged;
        assert!(staged_1.is_empty());
        assert!(staged_2.is_empty());
        assert!(stalled_case.first_buffered_watermarks.is_empty());
    }
}
