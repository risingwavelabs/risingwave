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

pub mod hnsw;

pub mod utils;

pub use risingwave_common::array::{
    VectorDistanceType as VectorDistance, VectorItemType as VectorItem,
};
pub use risingwave_common::types::{VectorRef, VectorVal as Vector};
pub use risingwave_common::vector::{MeasureDistance, MeasureDistanceBuilder};

use crate::vector::utils::BoundedNearest;

pub trait OnNearestItem<O> = for<'i> Fn(VectorRef<'i>, VectorDistance, &'i [u8]) -> O;

pub struct NearestBuilder<'a, O, M: MeasureDistanceBuilder> {
    measure: M::Measure<'a>,
    nearest: BoundedNearest<O>,
}

impl<'a, O, M: MeasureDistanceBuilder> NearestBuilder<'a, O, M> {
    pub fn new(target: VectorRef<'a>, n: usize) -> Self {
        assert!(n > 0);
        NearestBuilder {
            measure: M::new(target),
            nearest: BoundedNearest::new(n),
        }
    }

    pub fn add<'b>(
        &mut self,
        vecs: impl IntoIterator<Item = (VectorRef<'b>, &'b [u8])> + 'b,
        on_nearest_item: impl OnNearestItem<O>,
    ) {
        for (vec, info) in vecs {
            let distance = self.measure.measure(vec);
            self.nearest
                .insert(distance, || on_nearest_item(vec, distance, info));
        }
    }

    pub fn finish(self) -> Vec<O> {
        self.nearest.collect()
    }
}

#[cfg(any(test, feature = "test"))]
pub mod test_utils {
    use std::cmp::min;

    use bytes::Bytes;

    use crate::vector::VectorDistance;

    pub fn gen_info(i: usize) -> Bytes {
        Bytes::copy_from_slice(i.to_le_bytes().as_slice())
    }

    pub fn top_n<O>(input: &mut Vec<(VectorDistance, O)>, n: usize) {
        input.sort_by(|(first_distance, _), (second_distance, _)| {
            first_distance.total_cmp(second_distance)
        });
        let n = min(n, input.len());
        input.resize_with(n, || unreachable!());
    }

    pub use risingwave_common::test_utils::rand_array::gen_vector_for_test as gen_vector;
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::array::VectorVal;
    use risingwave_common::vector::MeasureDistanceBuilder;
    use risingwave_common::vector::distance::L2SqrDistance;

    use crate::vector::NearestBuilder;
    use crate::vector::test_utils::{gen_info, gen_vector, top_n};

    fn gen_random_input(count: usize) -> Vec<(VectorVal, Bytes)> {
        (0..count).map(|i| (gen_vector(10), gen_info(i))).collect()
    }

    #[test]
    fn test_empty_top_n() {
        let vec = gen_vector(10);
        let builder = NearestBuilder::<'_, (), L2SqrDistance>::new(vec.to_ref(), 10);
        assert!(builder.finish().is_empty());
    }

    fn test_inner(count: usize, n: usize) {
        let input = gen_random_input(count);
        let vec = gen_vector(10);
        let mut builder = NearestBuilder::<'_, _, L2SqrDistance>::new(vec.to_ref(), 10);
        builder.add(
            input.iter().map(|(v, b)| (v.to_ref(), b.as_ref())),
            |_, d, b| (d, Bytes::copy_from_slice(b)),
        );
        let output = builder.finish();
        let mut expected_output = input
            .into_iter()
            .map(|(v, b)| (L2SqrDistance::distance(vec.to_ref(), v.to_ref()), b))
            .collect_vec();
        top_n(&mut expected_output, n);
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_not_full_top_n() {
        test_inner(5, 10);
    }

    #[test]
    fn test_exact_size_top_n() {
        test_inner(10, 10);
    }

    #[test]
    fn test_oversize_top_n() {
        test_inner(20, 10);
    }
}
