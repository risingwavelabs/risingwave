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
use risingwave_common::ensure;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::*;

/// `EqGroups` encodes the grouping information in the sort aggregate algorithm.
///
/// - `SortedGrouper::split_groups` creates a `EqGroups` from a single column.
/// - `EqGroups::intersect` combines `EqGroups` from each column into a single one.
/// - `{SortedGrouper,Aggregator}::update_and_output_with_sorted_groups` needs the
/// grouping information to perform the grouped aggregation.
///
/// Internally, `EqGroups` is encoded as the indices that each new group starts.
/// Specially, a leading `0` means (the 0-th tuple of) this chunk starts a new
/// group compared to (the last tuple of) the previous chunk, and there is no leading
/// `0` when it continues the same group or there is no previous chunk.
// pub struct EqGroups(Vec<usize>);
#[derive(Default)]
pub struct EqGroups {
    indices: Vec<usize>,
    offset: usize, // offset to next index waiting for processing
    limit: usize,  // limit the number of groups can be processed
}

impl EqGroups {
    pub fn new(indices: Vec<usize>) -> Self {
        Self {
            indices,
            ..Default::default()
        }
    }

    pub fn starting_indices(&self) -> &[usize] {
        &self.indices[self.offset..]
    }

    pub fn len(&self) -> usize {
        self.indices.len() - self.offset
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_reach_limit(&self, group_cnt: usize) -> bool {
        self.limit != 0 && self.limit == group_cnt
    }

    pub fn chunk_offset(&self) -> usize {
        if self.offset > 0 {
            self.indices[self.offset - 1]
        } else {
            0
        }
    }

    pub fn advance_offset(&mut self) {
        if (self.limit == 0) || (self.offset + self.limit >= self.indices.len()) {
            self.offset = self.indices.len();
        } else {
            self.offset += self.limit;
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn reset_limit(&mut self) {
        self.limit = 0;
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
            if let Some(ri) = column.starting_indices().first() {
                heap.push(Reverse((ri, ci, 0)));
            }
        }
        while let Some(Reverse((ri, ci, idx))) = heap.pop() {
            if let Some(ri_next) = columns[ci].starting_indices().get(idx + 1) {
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
pub trait SortedGrouper: Send + 'static {
    /// `detect_groups` detects the `EqGroups` from the `input` array if appended
    /// to current state. See the documentation of `EqGroups` to learn more.
    ///
    /// This is a `dry-run` and does not update its state yet, because it does not
    /// have grouping information from all group columns yet.
    ///
    /// `offset` offset to the input child chunk
    fn detect_groups(&self, input: &ArrayImpl) -> Result<EqGroups>;

    /// `update_and_output_with_sorted_groups` updates with each subslice of the
    /// `input` array according to the `EqGroups`. Finished groups are outputted
    /// to `builder` immediately along the way. After this call, the internal state
    /// is about the last group which may continue in the next chunk. It can be
    /// obtained with `output` when there are no more upstream data.
    fn update_and_output_with_sorted_groups(
        &mut self,
        input: &ArrayImpl,
        builder: &mut ArrayBuilderImpl,
        groups: &EqGroups,
    ) -> Result<()>;

    /// `output` the state to the `builder`. Expected to be called once to obtain
    /// the last group, when there are no more upstream data.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}
pub type BoxedSortedGrouper = Box<dyn SortedGrouper>;

pub fn create_sorted_grouper(input_type: DataType) -> Result<BoxedSortedGrouper> {
    match input_type {
        // DataType::Int16 => Ok(Box::new(GeneralSortedGrouper::<I16Array>::new())),
        // DataType::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array>::new())),
        DataType::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        })),
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
    #[allow(dead_code)]
    pub fn new(ongoing: bool, group_value: Option<T::OwnedItem>) -> Self {
        Self {
            ongoing,
            group_value,
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

    pub fn update_and_output_with_sorted_groups_concrete(
        &mut self,
        input: &T,
        builder: &mut T::Builder,
        groups: &EqGroups,
    ) -> Result<()> {
        let mut group_cnt = 0;
        let mut groups_iter = groups.starting_indices().iter().peekable();
        let mut cur = self.group_value.as_ref().map(|x| x.as_scalar_ref());

        let chunk_offset = groups.chunk_offset();
        for (i, v) in input.iter().skip(chunk_offset).enumerate() {
            if groups_iter.peek() == Some(&&(i + chunk_offset)) {
                groups_iter.next();
                group_cnt += 1;
                ensure!(self.ongoing);
                builder.append(cur)?;
            }
            self.ongoing = true;
            cur = v;

            // save current states and exit when we reach limit
            if groups.limit() != 0 && group_cnt == groups.limit() {
                break;
            }
        }
        self.group_value = cur.map(|x| x.to_owned_scalar());
        Ok(())
    }

    pub fn output_concrete(&self, builder: &mut T::Builder) -> Result<()> {
        builder
            .append(self.group_value.as_ref().map(|x| x.as_scalar_ref()))
            .map_err(Into::into)
    }
}

macro_rules! impl_sorted_grouper {
    ($input:ty, $input_variant:ident) => {
        impl SortedGrouper for GeneralSortedGrouper<$input> {
            fn detect_groups(&self, input: &ArrayImpl) -> Result<EqGroups> {
                if let ArrayImpl::$input_variant(i) = input {
                    self.detect_groups_concrete(i)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }

            fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                if let ArrayBuilderImpl::$input_variant(b) = builder {
                    self.output_concrete(b)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Builder fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }

            fn update_and_output_with_sorted_groups(
                &mut self,
                input: &ArrayImpl,
                builder: &mut ArrayBuilderImpl,
                groups: &EqGroups,
            ) -> Result<()> {
                if let (ArrayImpl::$input_variant(i), ArrayBuilderImpl::$input_variant(b)) =
                    (input, builder)
                {
                    self.update_and_output_with_sorted_groups_concrete(i, b, groups)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {} or builder fail to match {}.",
                        stringify!($input_variant),
                        stringify!($input_variant)
                    ))
                    .into())
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
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::agg_call::Type;
    use risingwave_pb::expr::AggCall;

    use super::*;
    use crate::vector_op::agg::functions::*;
    use crate::vector_op::agg::general_agg::GeneralAgg;
    use crate::vector_op::agg::AggStateFactory;

    #[test]
    fn group_int32() -> Result<()> {
        let mut g = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut builder = I32ArrayBuilder::new(0).unwrap();

        let input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq = g.detect_groups_concrete(&input)?;
        g.update_and_output_with_sorted_groups_concrete(&input, &mut builder, &eq)?;
        assert_eq!(eq.starting_indices(), &vec![2]);

        let input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq = g.detect_groups_concrete(&input)?;
        g.update_and_output_with_sorted_groups_concrete(&input, &mut builder, &eq)?;
        assert_eq!(eq.starting_indices(), &vec![1]);

        g.output_concrete(&mut builder)?;
        assert_eq!(
            builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(4)]
        );
        Ok(())
    }

    #[test]
    fn group_intersect() {
        let groups = vec![EqGroups::new(vec![0, 2, 4]), EqGroups::new(vec![1, 2, 5])];
        assert_eq!(
            EqGroups::intersect(&groups).starting_indices(),
            &vec![0, 1, 2, 4, 5]
        );
    }

    #[test]
    fn vec_agg_group() -> Result<()> {
        let mut g0 = GeneralSortedGrouper::<I32Array>::new(false, None);
        let mut g0_builder = I32ArrayBuilder::new(0).unwrap();
        let mut g1 = GeneralSortedGrouper::<I32Array>::new(false, None);
        let mut g1_builder = I32ArrayBuilder::new(0).unwrap();
        let mut a = GeneralAgg::<I32Array, _, I64Array>::new(DataType::Int64, 0, sum, None);
        let mut a_builder = I64ArrayBuilder::new(0).unwrap();

        let g0_input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq0 = g0.detect_groups_concrete(&g0_input)?;
        let g1_input = I32Array::from_slice(&[Some(7), Some(8), Some(8)]).unwrap();
        let eq1 = g1.detect_groups_concrete(&g1_input)?;
        let eq = EqGroups::intersect(&[eq0, eq1]);
        g0.update_and_output_with_sorted_groups_concrete(&g0_input, &mut g0_builder, &eq)?;
        g1.update_and_output_with_sorted_groups_concrete(&g1_input, &mut g1_builder, &eq)?;
        let a_input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        a.update_and_output_with_sorted_groups_concrete(&a_input, &mut a_builder, &eq)?;

        let g0_input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq0 = g0.detect_groups_concrete(&g0_input)?;
        let g1_input = I32Array::from_slice(&[Some(8), Some(8), Some(8)]).unwrap();
        let eq1 = g1.detect_groups_concrete(&g1_input)?;
        let eq = EqGroups::intersect(&[eq0, eq1]);
        g0.update_and_output_with_sorted_groups_concrete(&g0_input, &mut g0_builder, &eq)?;
        g1.update_and_output_with_sorted_groups_concrete(&g1_input, &mut g1_builder, &eq)?;
        let a_input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        a.update_and_output_with_sorted_groups_concrete(&a_input, &mut a_builder, &eq)?;

        g0.output_concrete(&mut g0_builder)?;
        g1.output_concrete(&mut g1_builder)?;
        a.output_concrete(&mut a_builder)?;
        assert_eq!(
            g0_builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(3), Some(4)]
        );
        assert_eq!(
            g1_builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(7), Some(8), Some(8), Some(8)]
        );
        assert_eq!(
            a_builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(4), Some(5)]
        );
        Ok(())
    }

    #[test]
    fn vec_count_star() {
        let mut g0 = GeneralSortedGrouper::<I32Array>::new(false, None);
        let mut g0_builder = I32ArrayBuilder::new(0).unwrap();
        let prost = AggCall {
            r#type: Type::Count as i32,
            args: vec![],
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
        };
        let mut agg = AggStateFactory::new(&prost)
            .unwrap()
            .create_agg_state()
            .unwrap();
        let mut a_builder = agg.return_type().create_array_builder(0).unwrap();

        let input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq = g0.detect_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        agg.update_and_output_with_sorted_groups(
            &DataChunk::new(vec![Column::new(Arc::new(input.into()))], 3),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        let input = I32Array::from_slice(&[Some(3), Some(3), Some(3)]).unwrap();
        let eq = g0.detect_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        agg.update_and_output_with_sorted_groups(
            &DataChunk::new(vec![Column::new(Arc::new(input.into()))], 3),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        let input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq = g0.detect_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        agg.update_and_output_with_sorted_groups(
            &DataChunk::new(vec![Column::new(Arc::new(input.into()))], 3),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        g0.output_concrete(&mut g0_builder).unwrap();
        agg.output(&mut a_builder).unwrap();
        assert_eq!(
            g0_builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(4)]
        );
        assert_eq!(
            a_builder
                .finish()
                .unwrap()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(5), Some(2)]
        );
    }
}
