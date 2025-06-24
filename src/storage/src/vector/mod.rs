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

pub mod distance;
pub use distance::DistanceMeasurement;

pub mod utils;

use std::sync::Arc;

use crate::vector::utils::BoundedNearest;

pub type VectorItem = f32;
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorInner<T>(T);

pub type Vector = VectorInner<Arc<[VectorItem]>>;
pub type VectorRef<'a> = VectorInner<&'a [VectorItem]>;
pub type VectorMutRef<'a> = VectorInner<&'a mut [VectorItem]>;

impl Vector {
    pub fn new(inner: &[VectorItem]) -> Self {
        Self(Arc::from(inner))
    }

    pub fn to_ref(&self) -> VectorRef<'_> {
        VectorInner(self.0.as_ref())
    }

    pub fn clone_from_ref(r: VectorRef<'_>) -> Self {
        Self(Vec::from(r.0).into())
    }

    pub fn get_mut(&mut self) -> Option<VectorMutRef<'_>> {
        Arc::get_mut(&mut self.0).map(VectorInner)
    }

    /// # Safety
    ///
    /// safe under the same condition to [`Arc::get_mut_unchecked`]
    pub unsafe fn get_mut_unchecked(&mut self) -> VectorMutRef<'_> {
        // safety: under unsafe function
        unsafe { VectorInner(Arc::get_mut_unchecked(&mut self.0)) }
    }
}

impl<'a> VectorRef<'a> {
    pub fn from_slice(slice: &'a [VectorItem]) -> Self {
        VectorInner(slice)
    }
}

impl<T: AsRef<[VectorItem]>> VectorInner<T> {
    pub fn dimension(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn as_slice(&self) -> &[VectorItem] {
        self.0.as_ref()
    }

    pub fn magnitude(&self) -> VectorItem {
        self.0
            .as_ref()
            .iter()
            .map(|item| item.powi(2))
            .sum::<VectorItem>()
            .sqrt()
    }

    pub fn normalized(&self) -> Vector {
        let slice = self.0.as_ref();
        let len = slice.len();
        let mut uninit = Arc::new_uninit_slice(len);
        // safety: just initialized, must be owned
        let uninit_mut = unsafe { Arc::get_mut_unchecked(&mut uninit) };
        let magnitude = self.magnitude();
        for i in 0..len {
            // safety: 0 <= i < len
            unsafe {
                uninit_mut
                    .get_unchecked_mut(i)
                    .write(slice.get_unchecked(i) / magnitude)
            };
        }
        // safety: initialized with len, and have set all item
        unsafe { VectorInner(uninit.assume_init()) }
    }
}

pub type VectorDistance = f32;

pub trait OnNearestItem<O> = for<'i> Fn(VectorRef<'i>, VectorDistance, &'i [u8]) -> O;

pub trait MeasureDistance<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance;
}

pub trait MeasureDistanceBuilder {
    type Measure<'a>: MeasureDistance<'a>;
    fn new(target: VectorRef<'_>) -> Self::Measure<'_>;

    fn distance(target: VectorRef<'_>, other: VectorRef<'_>) -> VectorDistance
    where
        Self: Sized,
    {
        Self::new(target).measure(other)
    }
}

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
    use std::sync::LazyLock;

    use bytes::Bytes;
    use itertools::Itertools;
    use parking_lot::Mutex;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::store::Vector;
    use crate::vector::VectorItem;

    pub fn gen_vector(d: usize) -> Vector {
        static RNG: LazyLock<Mutex<StdRng>> =
            LazyLock::new(|| Mutex::new(StdRng::seed_from_u64(233)));
        Vector::new(
            &(0..d)
                .map(|_| RNG.lock().random::<VectorItem>())
                .collect_vec(),
        )
    }

    pub fn gen_info(i: usize) -> Bytes {
        Bytes::copy_from_slice(i.to_le_bytes().as_slice())
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::vector::distance::L2Distance;
    use crate::vector::test_utils::{gen_info, gen_vector};
    use crate::vector::{MeasureDistanceBuilder, NearestBuilder, Vector, VectorDistance};

    fn gen_random_input(count: usize) -> Vec<(Vector, Bytes)> {
        (0..count).map(|i| (gen_vector(10), gen_info(i))).collect()
    }

    #[test]
    fn test_empty_top_n() {
        let vec = gen_vector(10);
        let builder = NearestBuilder::<'_, (), L2Distance>::new(vec.to_ref(), 10);
        assert!(builder.finish().is_empty());
    }

    fn top_n<O>(input: &mut Vec<(VectorDistance, O)>, n: usize) {
        input.sort_by(|(first_distance, _), (second_distance, _)| {
            first_distance.total_cmp(second_distance)
        });
        let n = min(n, input.len());
        input.resize_with(n, || unreachable!());
    }

    fn test_inner(count: usize, n: usize) {
        let input = gen_random_input(count);
        let vec = gen_vector(10);
        let mut builder = NearestBuilder::<'_, _, L2Distance>::new(vec.to_ref(), 10);
        builder.add(
            input.iter().map(|(v, b)| (v.to_ref(), b.as_ref())),
            |_, d, b| (d, Bytes::copy_from_slice(b)),
        );
        let output = builder.finish();
        let mut expected_output = input
            .into_iter()
            .map(|(v, b)| (L2Distance::distance(vec.to_ref(), v.to_ref()), b))
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
