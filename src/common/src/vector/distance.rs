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

use std::simd::Simd;
use std::simd::num::SimdFloat;

use crate::array::VectorDistanceType as VectorDistance;
use crate::types::{F32, VectorRef};
use crate::vector::{MeasureDistance, MeasureDistanceBuilder};

#[macro_export]
macro_rules! for_all_distance_measurement {
    ($macro:ident $($param:tt)*) => {
        $macro! {
            {
                (L1, $crate::vector::distance::L1Distance),
                (L2, $crate::vector::distance::L2Distance),
                (Cosine, $crate::vector::distance::CosineDistance),
                (InnerProduct, $crate::vector::distance::InnerProductDistance),
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
macro_rules! dispatch_distance_measurement {
    ({
        $(($distance_name:ident, $distance_type:ty),)+
    },
    $measurement:expr, $type_name:ident, $body:expr) => {
        match $measurement {
            $(
                $crate::vector::distance::DistanceMeasurement::$distance_name => {
                    type $type_name = $distance_type;
                    $body
                }
            ),+
        }
    };
    ($measurement:expr, $type_name:ident, $body:expr) => {
        $crate::for_all_distance_measurement! {dispatch_distance_measurement, $measurement, $type_name, $body}
    };
}

pub struct L1Distance;

pub struct L1DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L1Distance {
    type Measure<'a> = L1DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L1DistanceMeasure(target)
    }
}

#[cfg_attr(not(test), expect(dead_code))]
fn l1_trivial(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let first = first.as_slice();
    let second = second.as_slice();
    let len = first.len();
    assert_eq!(len, second.len());
    (0..len)
        .map(|i| {
            let diff = first[i].0 - second[i].0;
            diff.abs() as VectorDistance
        })
        .sum()
}

pub fn l1_faiss(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    faiss::utils::fvec_l1(
        F32::inner_slice(first.as_slice()),
        F32::inner_slice(second.as_slice()),
    ) as VectorDistance
}

impl<'a> MeasureDistance for L1DistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        l1_faiss(self.0, other)
    }
}

pub struct L2Distance;

/// Measure the l2 distance
///
/// In this implementation, we don't take the square root to avoid unnecessary computation, because
/// we only want comparison rather than the actual distance.
pub struct L2DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L2Distance {
    type Measure<'a> = L2DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L2DistanceMeasure(target)
    }
}

#[cfg_attr(not(test), expect(dead_code))]
fn l2_trivial(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let first = first.as_slice();
    let second = second.as_slice();
    let len = first.len();
    assert_eq!(len, second.len());
    (0..len)
        .map(|i| ((first[i].0 - second[i].0) as VectorDistance).powi(2))
        .sum()
}

pub fn l2_faiss(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    faiss::utils::fvec_l2sqr(
        F32::inner_slice(first.as_slice()),
        F32::inner_slice(second.as_slice()),
    ) as VectorDistance
}

impl<'a> MeasureDistance for L2DistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        l2_faiss(self.0, other)
    }
}

pub struct CosineDistance;
pub struct CosineDistanceMeasure<'a> {
    target: VectorRef<'a>,
    magnitude: f32,
}

impl MeasureDistanceBuilder for CosineDistance {
    type Measure<'a> = CosineDistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        let magnitude = target.magnitude();
        CosineDistanceMeasure { target, magnitude }
    }
}

impl<'a> MeasureDistance for CosineDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        let len = self.target.as_slice().len();
        assert_eq!(len, other.as_slice().len());
        let magnitude_mul = other.magnitude() * self.magnitude;
        if magnitude_mul < f32::MIN_POSITIVE {
            // If either vector is zero, the distance is the further 1.1
            return 1.1;
        }
        1.0 - inner_product_faiss(self.target, other) / magnitude_mul as VectorDistance
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

#[cfg_attr(not(test), expect(dead_code))]
fn inner_product_trivial(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let first = first.as_slice();
    let second = second.as_slice();
    let len = first.len();
    assert_eq!(len, second.len());
    (0..len)
        .map(|i| (first[i].0 * second[i].0) as VectorDistance)
        .sum::<VectorDistance>()
}

#[cfg_attr(not(test), expect(dead_code))]
fn inner_product_simd(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let first = F32::inner_slice(first.as_slice());
    let second = F32::inner_slice(second.as_slice());
    let len = first.len();
    assert_eq!(len, second.len());
    let mut sum = 0.0;
    let mut start = 0;
    let mut end = start + 32;
    while end <= len {
        let this = Simd::<f32, 32>::from_slice(&first[start..end]);
        let target = Simd::<f32, 32>::from_slice(&second[start..end]);
        sum += (this * target).reduce_sum() as VectorDistance;
        start += 32;
        end += 32;
    }
    (start..len)
        .map(|i| (first[i] * second[i]) as VectorDistance)
        .sum::<VectorDistance>()
        + sum
}

pub fn inner_product_faiss(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    faiss::utils::fvec_inner_product(
        F32::inner_slice(first.as_slice()),
        F32::inner_slice(second.as_slice()),
    ) as VectorDistance
}

impl<'a> MeasureDistance for InnerProductDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        -inner_product_faiss(self.0, other)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::*;
    use crate::array::VectorVal;
    use crate::test_utils::rand_array::gen_vector_for_test;

    const VECTOR_LEN: usize = 10;

    const VEC1: [f32; VECTOR_LEN] = [
        0.45742255, 0.04135585, 0.7236407, 0.82355756, 0.837814, 0.09387952, 0.8907283, 0.20203716,
        0.2039721, 0.7972273,
    ];

    const VEC2: [f32; VECTOR_LEN] = [
        0.9755903, 0.42836714, 0.45131344, 0.8602846, 0.61997443, 0.9501612, 0.65076965,
        0.22877127, 0.97690505, 0.44438475,
    ];

    const FLOAT_ABS_EPS: f32 = 2e-5;
    const FLOAT_REL_EPS: f32 = 1e-6;

    macro_rules! assert_eq_float {
        ($first:expr, $second:expr) => {{
            let a: f32 = $first;
            let b: f32 = $second;
            let diff = (a - b).abs();
            let tol = FLOAT_ABS_EPS.max(FLOAT_REL_EPS * a.abs().max(b.abs()));
            assert!(
                diff <= tol,
                "Expected: {}, Actual: {}, |Δ|={} > tol={}",
                b,
                a,
                diff,
                tol
            );
        }};
    }

    #[test]
    fn test_distance() {
        let first_vec = [0.238474_f32, 0.578234];
        let second_vec = [0.9327183_f32, 0.387495];
        let [v1_1, v1_2] = first_vec;
        let [v2_1, v2_2] = second_vec;
        let first_vec = VectorVal {
            inner: first_vec.map(Into::into).to_vec().into_boxed_slice(),
        };
        let second_vec = VectorVal {
            inner: second_vec.map(Into::into).to_vec().into_boxed_slice(),
        };
        let first_vec = first_vec.to_ref();
        let second_vec = second_vec.to_ref();
        assert_eq_float!(
            L1Distance::distance(first_vec, second_vec) as _,
            (v1_1 - v2_1).abs() + (v1_2 - v2_2).abs()
        );
        assert_eq_float!(
            L2Distance::distance(first_vec, second_vec) as _,
            (v1_1 - v2_1).powi(2) + (v1_2 - v2_2).powi(2)
        );
        assert_eq_float!(
            CosineDistance::distance(first_vec, second_vec) as _,
            1.0 - (v1_1 * v2_1 + v1_2 * v2_2)
                / ((v1_1.powi(2) + v1_2.powi(2)).sqrt() * (v2_1.powi(2) + v2_2.powi(2)).sqrt())
        );
        assert_eq_float!(
            InnerProductDistance::distance(first_vec, second_vec) as _,
            -(v1_1 * v2_1 + v1_2 * v2_2)
        );
        {
            let v1 = gen_vector_for_test(128);
            let v2 = gen_vector_for_test(128);
            let trivial = inner_product_trivial(v1.to_ref(), v2.to_ref()) as _;
            assert_eq_float!(inner_product_simd(v1.to_ref(), v2.to_ref()) as _, trivial);
            assert_eq_float!(inner_product_faiss(v1.to_ref(), v2.to_ref()) as _, trivial);
            assert_eq_float!(
                l2_trivial(v1.to_ref(), v2.to_ref()) as _,
                l2_faiss(v1.to_ref(), v2.to_ref()) as _
            );
            assert_eq_float!(
                l1_trivial(v1.to_ref(), v2.to_ref()) as _,
                l1_faiss(v1.to_ref(), v2.to_ref()) as _
            );
        }
    }

    #[test]
    fn test_expect_distance() {
        expect![[r#"
            3.6808228
        "#]]
        .assert_debug_eq(&L1Distance::distance(
            VectorRef::from_slice(&VEC1.map(Into::into)[..]),
            VectorRef::from_slice(&VEC2.map(Into::into)[..]),
        ));
        expect![[r#"
            2.054677
        "#]]
        .assert_debug_eq(&L2Distance::distance(
            VectorRef::from_slice(&VEC1.map(Into::into)[..]),
            VectorRef::from_slice(&VEC2.map(Into::into)[..]),
        ));
        expect![[r#"
            0.22848952
        "#]]
        .assert_debug_eq(&CosineDistance::distance(
            VectorRef::from_slice(&VEC1.map(Into::into)[..]),
            VectorRef::from_slice(&VEC2.map(Into::into)[..]),
        ));
        expect![[r#"
            -3.2870955
        "#]]
        .assert_debug_eq(&InnerProductDistance::distance(
            VectorRef::from_slice(&VEC1.map(Into::into)[..]),
            VectorRef::from_slice(&VEC2.map(Into::into)[..]),
        ));
    }
}
