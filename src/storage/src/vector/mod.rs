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

mod utils;

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
    ///     safe under the same condition to [`Arc::get_mut_unchecked`]
    pub unsafe fn get_mut_unchecked(&mut self) -> VectorMutRef<'_> {
        // safety: under unsafe function
        unsafe { VectorInner(Arc::get_mut_unchecked(&mut self.0)) }
    }
}

impl<T: AsRef<[VectorItem]>> VectorInner<T> {
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

pub trait OnNearestItemFn<O> =
    for<'i> Fn(VectorRef<'i>, VectorDistance, &'i [u8]) -> O + Send + 'static;

#[macro_export]
macro_rules! for_all_distance_measurement {
    ($macro:ident $($param:tt)*) => {
        $macro! {
            {
                (L1, $crate::vector::L1Distance),
                (L2, $crate::vector::L2Distance),
                (KlDivergence, $crate::vector::KlDivergenceDistance),
                (Cosine, $crate::vector::CosineDistance),
                (InnerProduct, $crate::vector::InnerProductDistance),
            }
            $($param)*
        }
    };
}

macro_rules! define_measure {
    ({
        $(($distance_name:ident, $_distance_type:ty),)+
    }) => {
        pub enum DistanceMeasurement {
            $($distance_name),+
        }
    };
    () => {
        for_all_distance_measurement! {define_measure}
    };
}

define_measure!();

#[macro_export]
macro_rules! dispatch_measurement {
    ({
        $(($distance_name:ident, $distance_type:ty),)+
    },
    $measurement:expr, $type_name:ident, $body:expr) => {
        match $measurement {
            $(
                DistanceMeasurement::$distance_name => {
                    type $type_name = $distance_type;
                    $body
                }
            ),+
        }
    };
    ($measurement:expr, $type_name:ident, $body:expr) => {
        $crate::for_all_distance_measurement! {dispatch_measurement, $measurement, $type_name, $body}
    };
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

pub struct L1Distance;

pub struct L1DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L1Distance {
    type Measure<'a> = L1DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L1DistanceMeasure(target)
    }
}

impl<'a> MeasureDistance<'a> for L1DistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.0.0.len();
        assert_eq!(len, other.0.len());
        // In this implementation, we don't take the square root to avoid unnecessary computation, because
        // we only want comparison rather than the actual distance.
        (0..len)
            .map(|i| {
                let diff = self.0.0[i] - other.0[i];
                diff.abs()
            })
            .sum()
    }
}

pub trait MeasureDistance<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance;
}

pub struct L2Distance;

pub struct L2DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L2Distance {
    type Measure<'a> = L2DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L2DistanceMeasure(target)
    }
}

impl<'a> MeasureDistance<'a> for L2DistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.0.0.len();
        assert_eq!(len, other.0.len());
        // In this implementation, we don't take the square root to avoid unnecessary computation, because
        // we only want comparison rather than the actual distance.
        (0..len).map(|i| (self.0.0[i] - other.0[i]).powi(2)).sum()
    }
}

pub struct KlDivergenceDistance;
pub struct KlDivergenceDistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for KlDivergenceDistance {
    type Measure<'a> = KlDivergenceDistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> KlDivergenceDistanceMeasure<'_> {
        KlDivergenceDistanceMeasure(target)
    }
}

impl<'a> MeasureDistance<'a> for KlDivergenceDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.0.0.len();
        assert_eq!(len, other.0.len());
        (0..len)
            .map(|i| {
                let target_prob = self.0.0[i];
                target_prob * (target_prob / other.0[i]).ln()
            })
            .sum()
    }
}

pub struct CosineDistance;
pub struct CosineDistanceMeasure<'a> {
    target: VectorRef<'a>,
    magnitude: VectorItem,
}

impl MeasureDistanceBuilder for CosineDistance {
    type Measure<'a> = CosineDistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        let magnitude = target.magnitude();
        CosineDistanceMeasure { target, magnitude }
    }
}

impl<'a> MeasureDistance<'a> for CosineDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.target.0.len();
        assert_eq!(len, other.0.len());
        let magnitude_mul = other.magnitude() * self.magnitude;
        1.0 - (0..len)
            .map(|i| self.target.0[i] * other.0[i] / magnitude_mul)
            .sum::<VectorDistance>()
    }
}

pub struct InnerProductDistance;
pub struct InnerProductDistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for InnerProductDistance {
    type Measure<'a> = InnerProductDistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        InnerProductDistanceMeasure(target)
    }
}

impl<'a> MeasureDistance<'a> for InnerProductDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.0.0.len();
        assert_eq!(len, other.0.len());
        -(0..len)
            .map(|i| self.0.0[i] * other.0[i])
            .sum::<VectorDistance>()
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
        on_nearest_item: impl OnNearestItemFn<O>,
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

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::sync::LazyLock;

    use bytes::Bytes;
    use expect_test::expect;
    use itertools::Itertools;
    use parking_lot::Mutex;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::vector::{
        CosineDistance, InnerProductDistance, KlDivergenceDistance, L1Distance, L2Distance,
        MeasureDistanceBuilder, NearestBuilder, Vector, VectorDistance, VectorInner, VectorItem,
    };

    const VECTOR_LEN: usize = 10;

    const VEC1: [f32; VECTOR_LEN] = [
        0.45742255, 0.04135585, 0.7236407, 0.82355756, 0.837814, 0.09387952, 0.8907283, 0.20203716,
        0.2039721, 0.7972273,
    ];

    const VEC2: [f32; VECTOR_LEN] = [
        0.9755903, 0.42836714, 0.45131344, 0.8602846, 0.61997443, 0.9501612, 0.65076965,
        0.22877127, 0.97690505, 0.44438475,
    ];

    pub(super) fn gen_vector(d: usize) -> Vector {
        static RNG: LazyLock<Mutex<StdRng>> =
            LazyLock::new(|| Mutex::new(StdRng::seed_from_u64(233)));
        Vector::new(
            &(0..d)
                .map(|_| RNG.lock().random::<VectorItem>())
                .collect_vec(),
        )
    }

    pub(super) fn gen_info(i: usize) -> Bytes {
        Bytes::copy_from_slice(i.to_le_bytes().as_slice())
    }

    fn gen_random_input(count: usize) -> Vec<(Vector, Bytes)> {
        (0..count)
            .map(|i| (gen_vector(VECTOR_LEN), gen_info(i)))
            .collect()
    }

    #[test]
    fn test_distance() {
        let first_vec = [0.238474, 0.578234];
        let second_vec = [0.9327183, 0.387495];
        let [v1_1, v1_2] = first_vec;
        let [v2_1, v2_2] = second_vec;
        let first_vec = VectorInner(&first_vec[..]);
        let second_vec = VectorInner(&second_vec[..]);
        {
            assert_eq!(first_vec.magnitude(), (v1_1.powi(2) + v1_2.powi(2)).sqrt());
            let mut normalized_vec =
                Vector::new(&[v1_1 / first_vec.magnitude(), v1_2 / first_vec.magnitude()]);
            assert_eq!(first_vec.normalized(), normalized_vec);
            assert!(normalized_vec.get_mut().is_some());
            let mut normalized_vec_clone = normalized_vec.clone();
            assert!(normalized_vec.get_mut().is_none());
            assert!(normalized_vec_clone.get_mut().is_none());
            drop(normalized_vec);
            assert!(normalized_vec_clone.get_mut().is_some());
        }
        assert_eq!(
            L1Distance::distance(first_vec, second_vec),
            (v1_1 - v2_1).abs() + (v1_2 - v2_2).abs()
        );
        assert_eq!(
            L2Distance::distance(first_vec, second_vec),
            (v1_1 - v2_1).powi(2) + (v1_2 - v2_2).powi(2)
        );
        assert_eq!(
            KlDivergenceDistance::distance(first_vec, second_vec),
            v1_1 * (v1_1 / v2_1).ln() + v1_2 * (v1_2 / v2_2).ln()
        );
        assert_eq!(
            CosineDistance::distance(first_vec, second_vec),
            1.0 - (v1_1 * v2_1 + v1_2 * v2_2)
                / ((v1_1.powi(2) + v1_2.powi(2)).sqrt() * (v2_1.powi(2) + v2_2.powi(2)).sqrt())
        );
        assert_eq!(
            InnerProductDistance::distance(first_vec, second_vec),
            -(v1_1 * v2_1 + v1_2 * v2_2)
        );
    }

    #[test]
    fn test_expect_distance() {
        expect![[r#"
            3.6808228
        "#]]
        .assert_debug_eq(&L1Distance::distance(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
        expect![[r#"
            2.054677
        "#]]
        .assert_debug_eq(&L2Distance::distance(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
        expect![[r#"
            0.29847336
        "#]]
        .assert_debug_eq(&KlDivergenceDistance::distance(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
        expect![[r#"
            0.22848958
        "#]]
        .assert_debug_eq(&CosineDistance::distance(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
        expect![[r#"
            -3.2870955
        "#]]
        .assert_debug_eq(&InnerProductDistance::distance(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
    }
    #[test]
    fn test_empty_top_n() {
        let builder = NearestBuilder::<'_, (), L2Distance>::new(VectorInner(&VEC1), 10);
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
        let mut builder = NearestBuilder::<'_, _, L2Distance>::new(VectorInner(&VEC1), 10);
        builder.add(
            input.iter().map(|(v, b)| (v.to_ref(), b.as_ref())),
            |_, d, b| (d, Bytes::copy_from_slice(b)),
        );
        let output = builder.finish();
        let mut expected_output = input
            .into_iter()
            .map(|(v, b)| (L2Distance::distance(VectorInner(&VEC1), v.to_ref()), b))
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
