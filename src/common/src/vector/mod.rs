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

use std::slice;

use bytes::{Buf, BufMut};
use risingwave_common_estimate_size::EstimateSize;
use tracing::warn;

use crate::array::{VectorDistanceType, VectorItemType, VectorRef, VectorVal};
use crate::types::F32;

#[derive(Clone, Copy, PartialEq, Eq, EstimateSize)]
pub struct VectorInner<T> {
    pub(crate) inner: T,
}

impl<T: Ord> PartialOrd for VectorInner<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord> Ord for VectorInner<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

pub trait MeasureDistance {
    fn measure(&self, other: VectorRef<'_>) -> VectorDistanceType;
}

pub trait MeasureDistanceBuilder {
    type Measure<'a>: MeasureDistance + 'a;
    fn new(target: VectorRef<'_>) -> Self::Measure<'_>;

    fn distance(target: VectorRef<'_>, other: VectorRef<'_>) -> VectorDistanceType
    where
        Self: Sized,
    {
        Self::new(target).measure(other)
    }
}

#[cfg_attr(not(test), expect(dead_code))]
fn l2_norm_trivial(vec: &VectorInner<impl AsRef<[VectorItemType]>>) -> f32 {
    vec.inner
        .as_ref()
        .iter()
        .map(|item| item.0.powi(2))
        .sum::<f32>()
        .sqrt()
}

fn l2_norm_faiss(vec: &VectorInner<impl AsRef<[VectorItemType]>>) -> f32 {
    faiss::utils::fvec_norm_l2sqr(F32::inner_slice(vec.inner.as_ref())).sqrt()
}

impl<T: AsRef<[VectorItemType]>> VectorInner<T> {
    pub fn dimension(&self) -> usize {
        self.inner.as_ref().len()
    }

    pub fn as_slice(&self) -> &[VectorItemType] {
        self.inner.as_ref()
    }

    pub fn as_raw_slice(&self) -> &[f32] {
        F32::inner_slice(self.inner.as_ref())
    }

    pub fn l2_norm(&self) -> f32 {
        l2_norm_faiss(self)
    }

    pub fn normalized(&self) -> VectorVal {
        let slice = self.inner.as_ref();
        let len = slice.len();
        let mut inner = Vec::with_capacity(len);
        let l2_norm = self.l2_norm();
        if l2_norm < f32::MIN_POSITIVE {
            warn!("normalize 0-norm vector. return original value");
            return VectorVal {
                inner: self.inner.as_ref().to_vec().into_boxed_slice(),
            };
        }
        // TODO: vectorize it
        inner.extend((0..len).map(|i| {
            // safety: 0 <= i < len
            unsafe { slice.get_unchecked(i) / l2_norm }
        }));
        VectorInner {
            inner: inner.into_boxed_slice(),
        }
    }
}

pub fn encode_vector_payload(payload: &[VectorItemType], mut buf: impl BufMut) {
    let vector_payload_ptr = payload.as_ptr() as *const u8;
    // safety: correctly set the size of vector_payload
    let vector_payload_slice =
        unsafe { slice::from_raw_parts(vector_payload_ptr, size_of_val(payload)) };
    buf.put_slice(vector_payload_slice);
}

pub fn decode_vector_payload(vector_item_count: usize, mut buf: impl Buf) -> Vec<VectorItemType> {
    let mut vector_payload = Vec::with_capacity(vector_item_count);

    let vector_payload_ptr = vector_payload.spare_capacity_mut().as_mut_ptr() as *mut u8;
    // safety: no data append to vector_payload, and correctly set the size of vector_payload
    let vector_payload_slice = unsafe {
        slice::from_raw_parts_mut(
            vector_payload_ptr,
            vector_item_count * size_of::<VectorItemType>(),
        )
    };
    buf.copy_to_slice(vector_payload_slice);
    // safety: have written correct amount of data
    unsafe {
        vector_payload.set_len(vector_item_count);
    }

    vector_payload
}

#[cfg(test)]
mod tests {
    use crate::array::VectorVal;
    use crate::vector::{l2_norm_faiss, l2_norm_trivial};

    #[test]
    fn test_vector() {
        let vec = [0.238474, 0.578234];
        let [v1_1, v1_2] = vec;
        let vec = VectorVal {
            inner: vec.map(Into::into).to_vec().into_boxed_slice(),
        };

        assert_eq!(vec.l2_norm(), (v1_1.powi(2) + v1_2.powi(2)).sqrt());
        assert_eq!(l2_norm_faiss(&vec), l2_norm_trivial(&vec));

        let normalized_vec = VectorVal::from_iter(
            [v1_1 / vec.l2_norm(), v1_2 / vec.l2_norm()].map(|v| v.try_into().unwrap()),
        );
        assert_eq!(vec.normalized(), normalized_vec);
    }
}
