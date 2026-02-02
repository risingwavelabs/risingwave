// Copyright 2023 RisingWave Labs
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

use risingwave_license::LicenseKeyRef;
use risingwave_pb::meta::PbSystemParams;

use super::common::{ParamValue, SystemParamsRead};

/// A wrapper for `PbSystemParams` that implements `SystemParamsRead` and provides `Arc`-based access.
#[derive(Clone, Debug, PartialEq)]
pub struct SystemParamsReader<I = Arc<PbSystemParams>> {
    inner: I,
}

impl From<PbSystemParams> for SystemParamsReader {
    fn from(params: PbSystemParams) -> Self {
        Self {
            inner: Arc::new(params),
        }
    }
}

impl<I> SystemParamsReader<I>
where
    I: std::ops::Deref<Target = PbSystemParams>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &PbSystemParams {
        &self.inner
    }
}

macro_rules! impl_system_params_read_trait {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        impl<I> SystemParamsRead for SystemParamsReader<I>
        where
            I: std::ops::Deref<Target = PbSystemParams>,
        {
            $(
                fn $field(&self) -> <$type as ParamValue>::Borrowed<'_> {
                    if let Some(v) = &self.inner.$field {
                        return v.into();
                    }
                    if let Some(v) = $default {
                        return v;
                    }
                    panic!("system param {:?} must be initialized", stringify!($field));
                }
            )*
        }
    };
}

crate::for_all_params!(impl_system_params_read_trait);
