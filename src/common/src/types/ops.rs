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

use crate::error::ErrorCode::InternalError;
use crate::error::Result;
pub trait CheckedAdd<T>
where
    Self: Sized,
{
    fn checked_add(&self, rhs: T) -> Result<Self>;
}

impl<T: num_traits::CheckedAdd> CheckedAdd<T> for T {
    fn checked_add(&self, rhs: T) -> Result<Self> {
        let res = <Self as num_traits::CheckedAdd>::checked_add(self, &rhs)
            .ok_or_else(|| InternalError("CheckedAdd Error".to_string()))?;
        Ok(res)
    }
}
