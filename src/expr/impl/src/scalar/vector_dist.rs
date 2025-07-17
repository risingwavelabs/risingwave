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

fn check_dims(name: &'static str, lhs: &[f32], rhs: &[f32]) -> Result<()> {
    if lhs.len() != rhs.len() {
        return Err(ExprError::InvalidParam {
            name,
            reason: format!(
                "different vector dimensions {} and {}",
                lhs.len(),
                rhs.len()
            )
            .into(),
        });
    }
    Ok(())
}

#[function("l2_distance(vector, vector) -> float8"/*, type_infer = "unreachable"*/)]
fn l2_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    let lhs = lhs.into_slice();
    let rhs = rhs.into_slice();
    check_dims("l2_distance", lhs, rhs)?;

    let mut sum = 0.0f32;
    for (l, r) in lhs.iter().zip_eq_fast(rhs.iter()) {
        let diff = l - r;
        sum += diff * diff;
    }
    Ok((sum as f64).sqrt().into())
}

#[function("cosine_distance(vector, vector) -> float8")]
fn cosine_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    let lhs = lhs.into_slice();
    let rhs = rhs.into_slice();
    check_dims("cosine_distance", lhs, rhs)?;

    let mut dot_product = 0.0f32;
    let mut lhs_norm = 0.0f32;
    let mut rhs_norm = 0.0f32;
    for (l, r) in lhs.iter().zip_eq_fast(rhs.iter()) {
        dot_product += l * r;
        lhs_norm += l * l;
        rhs_norm += r * r;
    }
    let similarity = dot_product as f64 / (lhs_norm as f64 * rhs_norm as f64).sqrt();

    Ok((1.0 - similarity.clamp(-1.0, 1.0)).into())
}

#[function("l1_distance(vector, vector) -> float8")]
fn l1_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    let lhs = lhs.into_slice();
    let rhs = rhs.into_slice();
    check_dims("l1_distance", lhs, rhs)?;

    let mut sum = 0.0f32;
    for (l, r) in lhs.iter().zip_eq_fast(rhs.iter()) {
        sum += (l - r).abs();
    }
    Ok((sum as f64).into())
}
