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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::sync::Arc;

pub type VectorItem = f32;
#[derive(Clone, Copy, Debug)]
pub struct VectorInner<T>(T);

pub type Vector = VectorInner<Arc<[VectorItem]>>;
pub type VectorRef<'a> = VectorInner<&'a [VectorItem]>;

impl Vector {
    pub fn new(inner: Vec<VectorItem>) -> Self {
        Self(inner.into())
    }

    pub fn to_ref(&self) -> VectorRef<'_> {
        VectorInner(self.0.as_ref())
    }

    pub fn clone_from_ref(r: VectorRef<'_>) -> Self {
        Self(Vec::from(r.0).into())
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
                (Cosine, $crate::vector::CosineDistance),
                (KlDivergence, $crate::vector::KlDivergenceDistance)
            }
            $($param)*
        }
    };
}

macro_rules! define_measure {
    ({
        $(($distance_name:ident, $_distance_type:ty)),+
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
        $(($distance_name:ident, $distance_type:ty)),+
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

pub trait MeasureDistance {
    fn measure(target: VectorRef<'_>, other: VectorRef<'_>) -> VectorDistance;
}

pub struct CosineDistance;

impl MeasureDistance for CosineDistance {
    fn measure(target: VectorRef<'_>, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = target.0.len();
        assert_eq!(len, other.0.len());
        (0..len)
            .map(|i| {
                let diff = target.0[i] - other.0[i];
                diff * diff
            })
            .sum()
    }
}

pub struct KlDivergenceDistance;

impl MeasureDistance for KlDivergenceDistance {
    fn measure(target: VectorRef<'_>, other: VectorRef<'_>) -> VectorDistance {
        // TODO: use some library with simd support
        let len = target.0.len();
        assert_eq!(len, other.0.len());
        (0..len)
            .map(|i| {
                let other_prob = other.0[i];
                other_prob * (other_prob / target.0[i]).ln()
            })
            .sum()
    }
}

struct NearestNode<O> {
    distance: VectorDistance,
    output: O,
}

impl<O> PartialEq for NearestNode<O> {
    fn eq(&self, other: &Self) -> bool {
        self.distance.eq(&other.distance)
    }
}

impl<O> Eq for NearestNode<O> {}

impl<O> PartialOrd for NearestNode<O> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<O> Ord for NearestNode<O> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or_else(|| {
                panic!(
                    "failed to compare distance {} and {}",
                    self.distance, other.distance
                )
            })
    }
}

pub struct NearestBuilder<'a, O, M: MeasureDistance> {
    target: VectorRef<'a>,
    /// max-heap with
    heap: BinaryHeap<NearestNode<O>>,
    n: usize,
    _phantom: PhantomData<M>,
}

impl<'a, O, M: MeasureDistance> NearestBuilder<'a, O, M> {
    pub fn new(target: VectorRef<'a>, n: usize) -> Self {
        assert!(n > 0);
        NearestBuilder {
            target,
            heap: BinaryHeap::with_capacity(n),
            n,
            _phantom: Default::default(),
        }
    }

    pub fn add<'b>(
        &mut self,
        vecs: impl IntoIterator<Item = (VectorRef<'b>, &'b [u8])> + 'b,
        on_nearest_item: impl OnNearestItemFn<O>,
    ) {
        for (vec, info) in vecs {
            let distance = M::measure(self.target, vec);
            if self.heap.len() >= self.n {
                let mut top = self.heap.peek_mut().expect("non-empty");
                if top.distance > distance {
                    top.distance = distance;
                    top.output = on_nearest_item(vec, distance, info);
                }
            } else {
                let output = on_nearest_item(vec, distance, info);
                self.heap.push(NearestNode { distance, output });
            }
        }
    }

    pub fn finish(mut self) -> Vec<O> {
        let size = self.heap.len();
        let mut vec = Vec::with_capacity(size);
        let uninit_slice = vec.spare_capacity_mut();
        let mut i = size;
        // elements are popped from max to min, so we write elements from back to front to ensure that the output is sorted ascendingly.
        while let Some(node) = self.heap.pop() {
            i -= 1;
            // safety: `i` is initialized as the size of `self.heap`. It must have decremented for once, and can
            // decrement for at most `size` time, so it must be that 0 <= i < size
            unsafe {
                uninit_slice.get_unchecked_mut(i).write(node.output);
            }
        }
        assert_eq!(i, 0);
        // safety: should have write `size` elements to the vector.
        unsafe { vec.set_len(size) }
        vec
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::iter::repeat_with;

    use bytes::Bytes;
    use expect_test::expect;
    use itertools::Itertools;
    use rand::{Rng, rng};

    use crate::vector::{
        CosineDistance, KlDivergenceDistance, MeasureDistance, NearestBuilder, Vector,
        VectorDistance, VectorInner, VectorItem,
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

    fn gen_vector() -> Vector {
        let mut rng = rng();
        Vector::new(
            repeat_with(|| rng.random::<VectorItem>())
                .take(VECTOR_LEN)
                .collect(),
        )
    }

    fn gen_random_input(count: usize) -> Vec<(Vector, Bytes)> {
        (0..count)
            .map(|i| {
                (
                    gen_vector(),
                    Bytes::copy_from_slice(i.to_le_bytes().as_slice()),
                )
            })
            .collect()
    }

    #[test]
    fn test_distance() {
        expect![[r#"
            2.054677
        "#]]
        .assert_debug_eq(&CosineDistance::measure(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
        expect![[r#"
            4.672073
        "#]]
        .assert_debug_eq(&KlDivergenceDistance::measure(
            VectorInner(&VEC1),
            VectorInner(&VEC2),
        ));
    }
    #[test]
    fn test_empty_top_n() {
        let builder = NearestBuilder::<'_, (), CosineDistance>::new(VectorInner(&VEC1), 10);
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
        let mut builder = NearestBuilder::<'_, _, CosineDistance>::new(VectorInner(&VEC1), 10);
        builder.add(
            input.iter().map(|(v, b)| (v.to_ref(), b.as_ref())),
            |_, d, b| (d, Bytes::copy_from_slice(b)),
        );
        let output = builder.finish();
        let mut expected_output = input
            .into_iter()
            .map(|(v, b)| (CosineDistance::measure(VectorInner(&VEC1), v.to_ref()), b))
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
