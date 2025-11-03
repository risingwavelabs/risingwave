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

use crate::types::{Datum, DatumRef, Decimal, ScalarImpl, ScalarRefImpl};

/// Strategy for filling gaps in time series data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FillStrategy {
    Interpolate,
    Locf,
    Null,
}

/// Calculates the step size for interpolation between two values.
///
/// # Parameters
/// - `d1`: The starting value as a `DatumRef`.
/// - `d2`: The ending value as a `DatumRef`.
/// - `steps`: The number of steps to interpolate between `d1` and `d2`.
///
/// # Returns
/// Returns a `Datum` representing the step size for each interpolation step,
/// or `None` if the input values are not compatible or `steps` is zero.
///
/// # Calculation
/// For supported types, computes `(d2 - d1) / steps` and returns the result as a `Datum`.
pub fn calculate_interpolation_step(d1: DatumRef<'_>, d2: DatumRef<'_>, steps: usize) -> Datum {
    let (Some(s1), Some(s2)) = (d1, d2) else {
        return None;
    };
    if steps == 0 {
        return None;
    }
    match (s1, s2) {
        (ScalarRefImpl::Int16(v1), ScalarRefImpl::Int16(v2)) => {
            Some(ScalarImpl::Int16((v2 - v1) / steps as i16))
        }
        (ScalarRefImpl::Int32(v1), ScalarRefImpl::Int32(v2)) => {
            Some(ScalarImpl::Int32((v2 - v1) / steps as i32))
        }
        (ScalarRefImpl::Int64(v1), ScalarRefImpl::Int64(v2)) => {
            Some(ScalarImpl::Int64((v2 - v1) / steps as i64))
        }
        (ScalarRefImpl::Float32(v1), ScalarRefImpl::Float32(v2)) => {
            Some(ScalarImpl::Float32((v2 - v1) / steps as f32))
        }
        (ScalarRefImpl::Float64(v1), ScalarRefImpl::Float64(v2)) => {
            Some(ScalarImpl::Float64((v2 - v1) / steps as f64))
        }
        (ScalarRefImpl::Decimal(v1), ScalarRefImpl::Decimal(v2)) => {
            Some(ScalarImpl::Decimal((v2 - v1) / Decimal::from(steps)))
        }
        _ => None,
    }
}

/// Mutates the `current` datum by adding the value of `step` to it.
///
/// This function is used during the interpolation process in gap filling,
/// where it incrementally updates the datum to generate intermediate values
/// between known data points.
pub fn apply_interpolation_step(current: &mut Datum, step: &ScalarImpl) {
    if let Some(curr) = current.as_mut() {
        match (curr, step) {
            (ScalarImpl::Int16(v1), ScalarImpl::Int16(v2)) => *v1 += v2,
            (ScalarImpl::Int32(v1), ScalarImpl::Int32(v2)) => *v1 += v2,
            (ScalarImpl::Int64(v1), ScalarImpl::Int64(v2)) => *v1 += v2,
            (ScalarImpl::Float32(v1), ScalarImpl::Float32(v2)) => *v1 += v2,
            (ScalarImpl::Float64(v1), ScalarImpl::Float64(v2)) => *v1 += v2,
            (ScalarImpl::Decimal(v1), ScalarImpl::Decimal(v2)) => *v1 = *v1 + *v2,
            _ => (),
        }
    }
}
