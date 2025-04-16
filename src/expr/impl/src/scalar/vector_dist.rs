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

use risingwave_common::types::{F64, VectorRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::{ExprError, Result, function};

#[function("l2_distance(vector, vector) -> float8"/*, type_infer = "unreachable"*/)]
fn l2_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    let lhs = lhs.into_inner();
    let rhs = rhs.into_inner();
    if lhs.len() != rhs.len() {
        return Err(ExprError::InvalidParam {
            name: "l2_distance",
            reason: format!(
                "different vector dimensions {} and {}",
                lhs.len(),
                rhs.len()
            )
            .into(),
        });
    }
    let mut sum = 0.0f32;
    for (l, r) in lhs.iter().zip_eq_fast(rhs.iter()) {
        let diff = l.unwrap().into_float32().0 - r.unwrap().into_float32().0;
        sum += diff * diff;
    }
    Ok((sum as f64).sqrt().into())
}
