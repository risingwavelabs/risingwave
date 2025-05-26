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

use risingwave_pb::hummock::PbDistanceType;

use crate::vector::{
    MeasureDistance, MeasureDistanceBuilder, VectorDistance, VectorItem, VectorRef,
};

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

        impl From<PbDistanceType> for DistanceMeasurement {
            fn from(value: PbDistanceType) -> Self {
                match value {
                    PbDistanceType::Unspecified => {
                        unreachable!()
                    }
                    $(
                        PbDistanceType::$distance_name => {
                            DistanceMeasurement::$distance_name
                        }
                    ),+
                }
            }
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

pub struct L1Distance;

pub struct L1DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L1Distance {
    type Measure<'a> = L1DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L1DistanceMeasure(target)
    }
}

impl<'a> MeasureDistance for L1DistanceMeasure<'a> {
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

pub struct L2Distance;

pub struct L2DistanceMeasure<'a>(VectorRef<'a>);

impl MeasureDistanceBuilder for L2Distance {
    type Measure<'a> = L2DistanceMeasure<'a>;

    fn new(target: VectorRef<'_>) -> Self::Measure<'_> {
        L2DistanceMeasure(target)
    }
}

impl<'a> MeasureDistance for L2DistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = self.0.0.len();
        assert_eq!(len, other.0.len());
        // In this implementation, we don't take the square root to avoid unnecessary computation, because
        // we only want comparison rather than the actual distance.
        (0..len).map(|i| (self.0.0[i] - other.0[i]).powi(2)).sum()
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

impl<'a> MeasureDistance for CosineDistanceMeasure<'a> {
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

#[cfg_attr(not(test), expect(dead_code))]
fn inner_product_trivial(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let len = first.0.len();
    assert_eq!(len, second.0.len());
    -(0..len)
        .map(|i| first.0[i] * second.0[i])
        .sum::<VectorItem>()
}

#[cfg_attr(not(test), expect(dead_code))]
fn inner_product_simd(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    let len = first.0.len();
    assert_eq!(len, second.0.len());
    let mut sum = 0.0;
    let mut start = 0;
    let mut end = start + 32;
    while end <= len {
        let this = Simd::<VectorItem, 32>::from_slice(&first.0[start..end]);
        let target = Simd::<VectorItem, 32>::from_slice(&second.0[start..end]);
        sum += (this * target).reduce_sum();
        start += 32;
        end += 32;
    }
    -((start..len)
        .map(|i| first.0[i] * second.0[i])
        .sum::<VectorDistance>()
        + sum)
}

fn inner_product_faiss(first: VectorRef<'_>, second: VectorRef<'_>) -> VectorDistance {
    -faiss::utils::fvec_inner_product(first.0, second.0)
}

impl<'a> MeasureDistance for InnerProductDistanceMeasure<'a> {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistance {
        inner_product_faiss(self.0, other)
    }
}

#[cfg(test)]
mod tests {

    use expect_test::expect;

    use super::*;
    use crate::vector::test_utils::gen_vector;
    use crate::vector::{MeasureDistanceBuilder, Vector, VectorInner};

    const VECTOR_LEN: usize = 10;

    const VEC1: [f32; VECTOR_LEN] = [
        0.45742255, 0.04135585, 0.7236407, 0.82355756, 0.837814, 0.09387952, 0.8907283, 0.20203716,
        0.2039721, 0.7972273,
    ];

    const VEC2: [f32; VECTOR_LEN] = [
        0.9755903, 0.42836714, 0.45131344, 0.8602846, 0.61997443, 0.9501612, 0.65076965,
        0.22877127, 0.97690505, 0.44438475,
    ];

    const FLOAT_ALLOWED_BIAS: f32 = 1e-5;

    macro_rules! assert_eq_float {
        ($first:expr, $second:expr) => {
            assert!(($first - $second) < FLOAT_ALLOWED_BIAS)
        };
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
            CosineDistance::distance(first_vec, second_vec),
            1.0 - (v1_1 * v2_1 + v1_2 * v2_2)
                / ((v1_1.powi(2) + v1_2.powi(2)).sqrt() * (v2_1.powi(2) + v2_2.powi(2)).sqrt())
        );
        assert_eq!(
            InnerProductDistance::distance(first_vec, second_vec),
            -(v1_1 * v2_1 + v1_2 * v2_2)
        );
        {
            let v1 = gen_vector(128);
            let v2 = gen_vector(128);
            let trivial = inner_product_trivial(v1.to_ref(), v2.to_ref());
            assert_eq_float!(inner_product_simd(v1.to_ref(), v2.to_ref()), trivial);
            assert_eq_float!(inner_product_faiss(v1.to_ref(), v2.to_ref()), trivial);
        }
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
}
