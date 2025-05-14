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

use std::sync::Arc;

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

pub type VectorDistance = f32;

pub trait OnNearestItemFn<O> =
    for<'i> Fn(VectorRef<'i>, VectorDistance, &'i [u8]) -> O + Send + 'static;

pub enum DistanceMeasurement {}
