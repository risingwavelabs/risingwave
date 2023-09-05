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

use risingwave_common::{array::*, types::{Scalar, DefaultOrdered, ScalarImpl}, types::ToOwnedDatum};
use risingwave_expr_macro::function;

use crate::{Result, ExprError};

#[function("array_min(list) -> int32")] 
pub fn array_min(list: ListRef<'_>) -> Result<i32> {
    let datum_ref = list.iter().map(|x| DefaultOrdered(x.unwrap())).min();
    let datum_ref = datum_ref.map(|x| x.into_inner());
    if let Some(ScalarImpl::Int32(scalar)) = datum_ref.to_owned_datum() {
        Ok(scalar)
    } else {
        Err(ExprError::NumericOutOfRange)
    }
}