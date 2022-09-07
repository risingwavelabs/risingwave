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

use std::any::{type_name, Any};
use std::sync::Arc;

pub use self::prost::*;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

pub mod addr;
pub mod bit_util;
pub mod chunk_coalesce;
pub mod compress;
pub mod encoding_for_comparison;
pub mod env_var;
pub mod hash_util;
pub mod ordered;
pub mod prost;
pub mod sort_util;
#[macro_use]
pub mod try_match;
pub mod epoch;
mod future_utils;
pub mod scan_range;
pub mod schema_check;
pub mod value_encoding;
pub mod worker_util;

pub use future_utils::select_all;

pub fn downcast_ref<S, T>(source: &S) -> Result<&T>
where
    S: AsRef<dyn Any> + ?Sized,
    T: 'static,
{
    source.as_ref().downcast_ref::<T>().ok_or_else(|| {
        RwError::from(InternalError(format!(
            "Failed to cast to {}",
            type_name::<T>()
        )))
    })
}

pub fn downcast_arc<T>(source: Arc<dyn Any + Send + Sync>) -> Result<Arc<T>>
where
    T: 'static + Send + Sync,
{
    source.downcast::<T>().map_err(|_| {
        RwError::from(InternalError(format!(
            "Failed to cast to {}",
            type_name::<T>()
        )))
    })
}

pub fn downcast_mut<S, T>(source: &mut S) -> Result<&mut T>
where
    S: AsMut<dyn Any> + ?Sized,
    T: 'static,
{
    source.as_mut().downcast_mut::<T>().ok_or_else(|| {
        RwError::from(InternalError(format!(
            "Failed to cast to {}",
            type_name::<T>()
        )))
    })
}
