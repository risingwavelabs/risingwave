// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::types::*;

use crate::Result;

/// `EqGroups` encodes the grouping information in the sort aggregate algorithm.
///
/// - `EqGroups::intersect` combines `EqGroups` from each column into a single one.
/// - `SortAggExecutor` needs the grouping information to perform the grouped aggregation.
///
/// Internally, `EqGroups` is encoded as the indices that each new group starts.
/// Specially, a leading `0` means (the 0-th tuple of) this chunk starts a new
/// group compared to (the last tuple of) the previous chunk, and there is no leading
/// `0` when it continues the same group or there is no previous chunk.
// pub struct EqGroups(Vec<usize>);
#[derive(Default, Debug)]
pub struct EqGroups {
    pub indices: Vec<usize>,
}

impl EqGroups {
    pub fn new(indices: Vec<usize>) -> Self {
        Self { indices }
    }

    /// `intersect` combines the grouping information from each column into a single one.
    /// This is required so that we know `group by c1, c2` with `c1 = [a, a, c, c, d, d]`
    /// and `c2 = [g, h, h, h, h, h]` actually forms 4 groups: `[(a, g), (a, h), (c, h), (d, h)]`.
    ///
    /// Since the internal encoding is a sequence of sorted indices, this is effectively
    /// merging all sequences into a single one with deduplication. In the example above,
    /// the `EqGroups` of `c1` is `[2, 4]` and that of `c2` is `[1]`, so the output of
    /// `intersect` would be `[1, 2, 4]` identifying the new groups starting at these indices.
    pub fn intersect(columns: &[EqGroups]) -> EqGroups {
        let mut ret = Vec::new();
        // Use of BinaryHeap here is not to get a performant implementation but a
        // concise one. The number of group columns would not be huge.
        // Storing iterator rather than (ci, idx) in heap actually makes the implementation
        // more verbose:
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=1e3b098ee3ef352d5a0cac03b3193799
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap = BinaryHeap::new();
        for (ci, column) in columns.iter().enumerate() {
            if let Some(ri) = column.indices.first() {
                heap.push(Reverse((ri, ci, 0)));
            }
        }
        while let Some(Reverse((ri, ci, idx))) = heap.pop() {
            if let Some(ri_next) = columns[ci].indices.get(idx + 1) {
                heap.push(Reverse((ri_next, ci, idx + 1)));
            }
            if ret.last() == Some(ri) {
                continue;
            }
            ret.push(*ri);
        }
        EqGroups::new(ret)
    }
}

/// `SortedGrouper` contains the state of a group column in the sort aggregate
/// algorithm, just like `Aggregator` contains the state of an aggregate column.
/// TODO(yuchao): This can be deprecated and the logic can be moved to `SortAggExecutor`.
pub trait SortedGrouper: Send + 'static {
    /// `detect_groups` detects the `EqGroups` from the `input` array if appended
    /// to current state. See the documentation of `EqGroups` to learn more.
    ///
    /// This is a `dry-run` and does not update its state yet, because it does not
    /// have grouping information from all group columns yet.
    fn detect_groups(&self, input: &ArrayImpl) -> Result<EqGroups>;

    /// `update` updates the state of the grouper with a slice of input within a
    /// same group.
    fn update(&mut self, input: &ArrayImpl, start_idx: usize, end_idx: usize) -> Result<()>;

    /// `output` the state to the `builder`. Expected to be called at the end of
    /// each group.
    /// After `output` the internal state is reset.
    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}
pub type BoxedSortedGrouper = Box<dyn SortedGrouper>;

pub fn create_sorted_grouper(input_type: DataType) -> Result<BoxedSortedGrouper> {
    match input_type {
        // DataType::Int16 => Ok(Box::new(GeneralSortedGrouper::<I16Array>::new())),
        // DataType::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array>::new())),
        DataType::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array>::new())),
        // DataType::Int64 => Ok(Box::new(GeneralSortedGrouper::new::<I64Array>())),
        unimpl_input => todo!("unsupported sorted grouper: input={:?}", unimpl_input),
    }
}

pub struct GeneralSortedGrouper<T>
where
    T: Array,
    for<'a> T::RefItem<'a>: Eq,
{
    // Technically `group_value` is meaningless when `ongoing == false` and this
    // should be a single `Option<Option<T:OwnedItem>>`. But it actually makes this
    // simple struct less readable.
    ongoing: bool,
    group_value: Option<T::OwnedItem>,
}

impl<T> GeneralSortedGrouper<T>
where
    T: Array,
    for<'a> T::RefItem<'a>: Eq,
{
    pub fn new() -> Self {
        Self {
            ongoing: false,
            group_value: None,
        }
    }

    pub fn detect_groups_concrete(&self, input: &T) -> Result<EqGroups> {
        let mut ret = Vec::new();
        let mut ongoing = self.ongoing;
        let mut ongoing_group = self.group_value.as_ref().map(|x| x.as_scalar_ref());
        for (i, v) in input.iter().enumerate() {
            if ongoing && ongoing_group == v {
                continue;
            }
            if ongoing {
                ret.push(i);
            }
            ongoing = true;
            ongoing_group = v;
        }
        Ok(EqGroups::new(ret))
    }

    pub fn update_concrete(
        &mut self,
        input: &T,
        start_row_id: usize,
        _end_row_id: usize,
    ) -> Result<()> {
        self.group_value = input.value_at(start_row_id).map(|x| x.to_owned_scalar());
        Ok(())
    }

    pub fn output_concrete(&mut self, builder: &mut T::Builder) -> Result<()> {
        builder.append(self.group_value.as_ref().map(|x| x.as_scalar_ref()));
        self.ongoing = false;
        self.group_value = None;
        Ok(())
    }
}

macro_rules! impl_sorted_grouper {
    ($input:ty, $input_variant:ident) => {
        impl SortedGrouper for GeneralSortedGrouper<$input> {
            fn detect_groups(&self, input: &ArrayImpl) -> Result<EqGroups> {
                if let ArrayImpl::$input_variant(i) = input {
                    self.detect_groups_concrete(i)
                } else {
                    bail!("Input fail to match {}.", stringify!($input_variant))
                }
            }

            fn update(
                &mut self,
                input: &ArrayImpl,
                start_idx: usize,
                end_idx: usize,
            ) -> Result<()> {
                if let ArrayImpl::$input_variant(i) = input {
                    self.update_concrete(i, start_idx, end_idx)
                } else {
                    bail!("Input fail to match {}.", stringify!($input_variant))
                }
            }

            fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                if let ArrayBuilderImpl::$input_variant(b) = builder {
                    self.output_concrete(b)
                } else {
                    bail!("Builder fail to match {}.", stringify!($input_variant))
                }
            }
        }
    };
}
impl_sorted_grouper! { I16Array, Int16 }
impl_sorted_grouper! { I32Array, Int32 }
impl_sorted_grouper! { I64Array, Int64 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_int32() -> Result<()> {
        let mut g = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut builder = I32ArrayBuilder::new(0);

        let input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]);
        let eq = g.detect_groups_concrete(&input)?;
        assert_eq!(eq.indices, vec![2]);
        g.update_concrete(&input, 0, *eq.indices.first().unwrap())?;
        g.output_concrete(&mut builder)?;
        g.update_concrete(&input, *eq.indices.first().unwrap(), input.len())?;

        let input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]);
        let eq = g.detect_groups_concrete(&input)?;
        assert_eq!(eq.indices, vec![1]);
        g.update_concrete(&input, 0, *eq.indices.first().unwrap())?;
        g.output_concrete(&mut builder)?;
        g.update_concrete(&input, *eq.indices.first().unwrap(), input.len())?;
        g.output_concrete(&mut builder)?;

        assert_eq!(
            builder.finish().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(4)]
        );
        Ok(())
    }

    #[test]
    fn group_intersect() {
        let groups = vec![EqGroups::new(vec![0, 2, 4]), EqGroups::new(vec![1, 2, 5])];
        assert_eq!(EqGroups::intersect(&groups).indices, vec![0, 1, 2, 4, 5]);
    }
}
