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

use risingwave_common::array::Finite32;
use risingwave_common::types::{
    DataType, F32, F64, ListRef, ListValue, ScalarRefImpl, VectorRef, VectorVal,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::vector::MeasureDistanceBuilder;
use risingwave_common::vector::distance::{L1Distance, L2SqrDistance, inner_product_faiss};
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};

fn check_dims(name: &'static str, lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<()> {
    if lhs.dimension() != rhs.dimension() {
        return Err(ExprError::InvalidParam {
            name,
            reason: format!(
                "different vector dimensions {} and {}",
                lhs.dimension(),
                rhs.dimension()
            )
            .into(),
        });
    }
    Ok(())
}

/// ```slt
/// query R
/// SELECT l2_distance('[0,0]'::vector(2), '[3,4]');
/// ----
/// 5
///
/// query R
/// SELECT l2_distance('[0,0]'::vector(2), '[0,1]');
/// ----
/// 1
///
/// query error dimensions
/// SELECT l2_distance('[1,2]'::vector(2), '[3]');
///
/// query R
/// SELECT l2_distance('[3e38]'::vector(1), '[-3e38]');
/// ----
/// Infinity
///
/// query R
/// SELECT l2_distance('[1,1,1,1,1,1,1,1,1]'::vector(9), '[1,1,1,1,1,1,1,4,5]');
/// ----
/// 5
///
/// query R
/// SELECT '[0,0]'::vector(2) <-> '[3,4]';
/// ----
/// 5
/// ```
#[function("l2_distance(vector, vector) -> float8"/*, type_infer = "unreachable"*/)]
fn l2_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    check_dims("l2_distance", lhs, rhs)?;
    Ok(L2SqrDistance::distance(lhs, rhs).sqrt().into())
}

/// ```slt
/// query R
/// SELECT abs(cosine_distance('[1,2]'::vector(2), '[2,4]')) < 1e-5;
/// ----
/// t
///
/// query R
/// SELECT cosine_distance('[1,2]'::vector(2), '[0,0]');
/// ----
/// NaN
///
/// query R
/// SELECT abs(cosine_distance('[1,1]'::vector(2), '[1,1]')) < 1e-5;
/// ----
/// t
///
/// query R
/// SELECT abs(cosine_distance('[1,0]'::vector(2), '[0,2]') - 1.0) < 1e-5;
/// ----
/// t
///
/// query R
/// SELECT abs(cosine_distance('[1,1]'::vector(2), '[-1,-1]') - 2) < 1e-5;
/// ----
/// t
///
/// query error dimensions
/// SELECT cosine_distance('[1,2]'::vector(2), '[3]');
///
/// query R
/// SELECT cosine_distance('[1,1]'::vector(2), '[1.1,1.1]');
/// ----
/// 0
///
/// query R
/// SELECT cosine_distance('[1,1]'::vector(2), '[-1.1,-1.1]');
/// ----
/// 2
///
/// query R
/// SELECT cosine_distance('[3e38]'::vector(1), '[3e38]');
/// ----
/// NaN
///
/// query R
/// SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::vector(9), '[1,2,3,4,5,6,7,8,9]');
/// ----
/// 0
///
/// query R
/// SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::vector(9), '[-1,-2,-3,-4,-5,-6,-7,-8,-9]');
/// ----
/// 2
///
/// query R
/// SELECT '[1,2]'::vector(2) <=> '[2,4]';
/// ----
/// 0
/// ```
#[function("cosine_distance(vector, vector) -> float8")]
fn cosine_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    check_dims("cosine_distance", lhs, rhs)?;
    Ok(risingwave_common::vector::distance::cosine_distance(lhs, rhs).into())
}

/// ```slt
/// query R
/// SELECT l1_distance('[0,0]'::vector(2), '[3,4]');
/// ----
/// 7
///
/// query R
/// SELECT l1_distance('[0,0]'::vector(2), '[0,1]');
/// ----
/// 1
///
/// query error dimensions
/// SELECT l1_distance('[1,2]'::vector(2), '[3]');
///
/// query R
/// SELECT l1_distance('[3e38]'::vector(1), '[-3e38]');
/// ----
/// Infinity
///
/// query R
/// SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::vector(9), '[1,2,3,4,5,6,7,8,9]');
/// ----
/// 0
///
/// query R
/// SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::vector(9), '[0,3,2,5,4,7,6,9,8]');
/// ----
/// 9
///
/// query R
/// SELECT '[0,0]'::vector(2) <+> '[3,4]';
/// ----
/// 7
/// ```
#[function("l1_distance(vector, vector) -> float8")]
fn l1_distance(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    check_dims("l1_distance", lhs, rhs)?;
    Ok(L1Distance::distance(lhs, rhs).into())
}

/// ```slt
/// query R
/// SELECT inner_product('[1,2]'::vector(2), '[3,4]');
/// ----
/// 11
///
/// query error dimensions
/// SELECT inner_product('[1,2]'::vector(2), '[3]');
///
/// query R
/// SELECT inner_product('[3e38]'::vector(1), '[3e38]');
/// ----
/// Infinity
///
/// query R
/// SELECT inner_product('[1,1,1,1,1,1,1,1,1]'::vector(9), '[1,2,3,4,5,6,7,8,9]');
/// ----
/// 45
///
/// query R
/// SELECT '[1,2]'::vector(2) <#> '[3,4]';
/// ----
/// -11
/// ```
#[function("inner_product(vector, vector) -> float8")]
fn inner_product(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<F64> {
    check_dims("inner_product", lhs, rhs)?;
    Ok(inner_product_faiss(lhs, rhs).into())
}

/// ```slt
/// query R
/// SELECT '[1,2,3]'::vector(3) + '[4,5,6]';
/// ----
/// [5,7,9]
///
/// query error out of range: overflow
/// SELECT '[3e38]'::vector(1) + '[3e38]';
///
/// query error dimensions
/// SELECT '[1,2]'::vector(2) + '[3]';
/// ```
#[function("add(vector, vector) -> vector", type_infer = "unreachable")]
fn vector_add(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<VectorVal> {
    check_dims("vector_add", lhs, rhs)?;
    let lhs = lhs.as_raw_slice();
    let rhs = rhs.as_raw_slice();

    let result = lhs
        .iter()
        .zip_eq_fast(rhs.iter())
        .map(|(l, r)| Finite32::try_from(l + r))
        .try_collect()
        .map_err(|_| ExprError::NumericOverflow)?;
    Ok(result)
}

/// ```slt
/// query R
/// SELECT '[1,2,3]'::vector(3) - '[4,5,6]';
/// ----
/// [-3,-3,-3]
///
/// query error out of range: overflow
/// SELECT '[-3e38]'::vector(1) - '[3e38]';
///
/// query error dimensions
/// SELECT '[1,2]'::vector(2) - '[3]';
/// ```
#[function("subtract(vector, vector) -> vector", type_infer = "unreachable")]
fn vector_subtract(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<VectorVal> {
    check_dims("vector_subtract", lhs, rhs)?;
    let lhs = lhs.as_raw_slice();
    let rhs = rhs.as_raw_slice();

    let result = lhs
        .iter()
        .zip_eq_fast(rhs.iter())
        .map(|(l, r)| Finite32::try_from(l - r))
        .try_collect()
        .map_err(|_| ExprError::NumericOverflow)?;
    Ok(result)
}

/// ```slt
/// query R
/// SELECT '[1,2,3]'::vector(3) * '[4,5,6]';
/// ----
/// [4,10,18]
///
/// query error out of range: overflow
/// SELECT '[1e37]'::vector(1) * '[1e37]';
///
/// query error out of range: underflow
/// SELECT '[1e-37]'::vector(1) * '[1e-37]';
///
/// query error dimensions
/// SELECT '[1,2]'::vector(2) * '[3]';
/// ```
#[function("multiply(vector, vector) -> vector", type_infer = "unreachable")]
fn vector_multiply(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<VectorVal> {
    check_dims("vector_multiply", lhs, rhs)?;
    let lhs = lhs.as_raw_slice();
    let rhs = rhs.as_raw_slice();

    let result = lhs
        .iter()
        .zip_eq_fast(rhs.iter())
        .map(|(l, r)| {
            let v = l * r;
            match v == 0. && !(*l == 0. || *r == 0.) {
                true => Err(ExprError::NumericUnderflow),
                false => Finite32::try_from(v).map_err(|_| ExprError::NumericOverflow),
            }
        })
        .try_collect()?;
    Ok(result)
}

/// ```slt
/// query R
/// SELECT '[1,2,3]'::vector(3) || '[4,5]';
/// ----
/// [1,2,3,4,5]
///
/// query error cast
/// SELECT '[1,2,3]'::vector(3) || null;
///
/// query R
/// SELECT '[1,2,3]'::vector(3) || null::vector(4);
/// ----
/// NULL
///
/// query error vector cannot have more than 16000 dimensions
/// SELECT null::vector(16000) || '[1]';
/// ```
#[function("vec_concat(vector, vector) -> vector", type_infer = "unreachable")]
fn vector_concat(lhs: VectorRef<'_>, rhs: VectorRef<'_>) -> Result<VectorVal> {
    let lhs = lhs.as_raw_slice();
    let rhs = rhs.as_raw_slice();

    let result = lhs
        .iter()
        .chain(rhs)
        .copied()
        .map(Finite32::try_from)
        .try_collect()
        .map_err(|_| ExprError::NumericOverflow)?;
    Ok(result)
}

/// ```slt
/// query T
/// SELECT '[1,2,3]'::vector(3)::real[];
/// ----
/// {1,2,3}
/// ```
#[function("cast(vector) -> float4[]")]
fn vector_to_float4(v: VectorRef<'_>) -> ListValue {
    v.as_raw_slice().iter().copied().map(F32::from).collect()
}

/// ```slt
/// query T
/// SELECT ARRAY[1,2,3]::vector(3);
/// ----
/// [1,2,3]
///
/// query T
/// SELECT ARRAY[1.0,2.0,3.0]::vector(3);
/// ----
/// [1,2,3]
///
/// query T
/// SELECT ARRAY[1,2,3]::float4[]::vector(3);
/// ----
/// [1,2,3]
///
/// query T
/// SELECT ARRAY[1,2,3]::float8[]::vector(3);
/// ----
/// [1,2,3]
///
/// query T
/// SELECT ARRAY[1,2,3]::numeric[]::vector(3);
/// ----
/// [1,2,3]
///
/// query T
/// SELECT '{1,2,3}'::real[]::vector(3);
/// ----
/// [1,2,3]
///
/// query error expected 2 dimensions, not 3
/// SELECT '{1,2,3}'::real[]::vector(2);
///
/// query error array must not contain nulls
/// SELECT '{NULL}'::real[]::vector(1);
///
/// query error NaN not allowed in vector
/// SELECT '{NaN}'::real[]::vector(1);
///
/// query error inf not allowed in vector
/// SELECT '{Infinity}'::real[]::vector(1);
///
/// query error -inf not allowed in vector
/// SELECT '{-Infinity}'::real[]::vector(1);
///
/// query error dimension
/// SELECT '{}'::real[]::vector(1);
///
/// query error cannot cast
/// SELECT '{{1}}'::real[][]::vector(1);
///
/// query T
/// SELECT '{1,2,3}'::double precision[]::vector(3);
/// ----
/// [1,2,3]
///
/// query error expected 2 dimensions, not 3
/// SELECT '{1,2,3}'::double precision[]::vector(2);
///
/// query error out of range
/// SELECT '{4e38,-4e38}'::double precision[]::vector(2);
///
/// # Caveat: pgvector does not check underflow and returns 0 here.
/// query error out of range
/// SELECT '{1e-46,-1e-46}'::double precision[]::vector(2);
/// ```
#[function("cast(int4[]) -> vector", type_infer = "unreachable")]
#[function("cast(decimal[]) -> vector", type_infer = "unreachable")]
#[function("cast(float4[]) -> vector", type_infer = "unreachable")]
#[function("cast(float8[]) -> vector", type_infer = "unreachable")]
fn array_to_vector(array: ListRef<'_>, ctx: &Context) -> Result<VectorVal> {
    macro_rules! bail_invalid_param {
        ($($arg:tt)*) => {
            return Err(ExprError::InvalidParam {
                name: "array_to_vector",
                reason: format!($($arg)*).into(),
            });
        };
    }

    let DataType::Vector(size) = ctx.return_type else {
        unreachable!()
    };
    if array.len() != size {
        bail_invalid_param!("expected {} dimensions, not {}", size, array.len());
    }
    let result = array
        .iter()
        .map(|scalar| {
            let Some(scalar) = scalar else {
                bail_invalid_param!("array must not contain nulls");
            };
            let val = match scalar {
                ScalarRefImpl::Int32(val) => val.into(),
                ScalarRefImpl::Decimal(val) => {
                    val.try_into().map_err(|_| ExprError::NumericOverflow)?
                }
                ScalarRefImpl::Float32(val) => val,
                ScalarRefImpl::Float64(val) => {
                    val.try_into().map_err(|_| ExprError::NumericOverflow)?
                }
                _ => unreachable!(),
            };
            Finite32::try_from(val.0).map_err(|err| ExprError::InvalidParam {
                name: "array_to_vector",
                reason: err.into(),
            })
        })
        .try_collect()?;
    Ok(result)
}

/// ```slt
/// query R
/// SELECT round(vector_norm('[1,1]'::vector(2))::numeric, 5);
/// ----
/// 1.41421
///
/// query R
/// SELECT vector_norm('[3,4]'::vector(2));
/// ----
/// 5
///
/// query R
/// SELECT vector_norm('[0,1]'::vector(2));
/// ----
/// 1
///
/// query R
/// SELECT vector_norm('[3e18,4e18]'::vector(2))::real;
/// ----
/// 5e+18
///
/// query R
/// SELECT vector_norm('[0,0]'::vector(2));
/// ----
/// 0
///
/// query R
/// SELECT vector_norm('[2]'::vector(1));
/// ----
/// 2
/// ```
#[function("l2_norm(vector) -> float8")]
fn l2_norm(vector: VectorRef<'_>) -> F64 {
    (vector.l2_norm() as f64).into()
}

/// ```slt
/// query R
/// SELECT l2_normalize('[3,4]'::vector(2));
/// ----
/// [0.6,0.8]
///
/// query R
/// SELECT l2_normalize('[3,0]'::vector(2));
/// ----
/// [1,0]
///
/// query R
/// SELECT l2_normalize('[0,0.1]'::vector(2));
/// ----
/// [0,1]
///
/// query R
/// SELECT l2_normalize('[0,0]'::vector(2));
/// ----
/// [0,0]
///
/// query R
/// SELECT l2_normalize('[3e18]'::vector(1));
/// ----
/// [1]
/// ```
#[function(
    "l2_normalize(vector) -> vector",
    type_infer = "|args| Ok(args[0].clone())"
)]
fn l2_normalize(vector: VectorRef<'_>) -> VectorVal {
    vector.normalized()
}

#[derive(Debug)]
pub struct SubvectorContext {
    pub start: usize,
    pub end: usize,
}

impl SubvectorContext {
    pub fn from_start_count(start: i32, count: i32) -> Result<Self> {
        Ok(Self {
            start: (start - 1) as usize,
            end: (start + count - 1) as usize,
        })
    }
}

/// ```slt
/// query R
/// SELECT subvector('[1,2,3,4,5]'::vector(5), 1, 3);
/// ----
/// [1,2,3]
///
/// query R
/// SELECT subvector('[1,2,3,4,5]'::vector(5), 3, 2);
/// ----
/// [3,4]
///
/// query R
/// SELECT subvector('[1,2,3,4,5]'::vector(5), 1, 5);
/// ----
/// [1,2,3,4,5]
///
/// query R
/// SELECT subvector('[1,2,3,4,5]'::vector(5), 5, 1);
/// ----
/// [5]
///
/// query R
/// SELECT subvector('[1,2,3,4,5]'::vector(5), 2, 3);
/// ----
/// [2,3,4]
///
/// query R
/// select subvector(vec, 1, 3) from (values ('[1,2,3,4,5]'::vector(5)), ('[6,7,8,9,10]'::vector(5))) as t(vec);
/// ----
/// [1,2,3]
/// [6,7,8]
///
/// statement error
/// SELECT subvector('[1,2,3,4,5]'::vector(5), -1, 2);
///
/// statement error
/// SELECT subvector('[6,7,8,9,10]'::vector(5), 1, 6);
///
/// statement error
/// SELECT subvector('[6,7,8,9,10]'::vector(5), 5, 2);
/// ```
#[function(
    "subvector(vector, int4, int4) -> vector",
    prebuild = "SubvectorContext::from_start_count($1, $2)?",
    type_infer = "unreachable"
)]
fn subvector(v: VectorRef<'_>, ctx: &SubvectorContext) -> Result<VectorVal> {
    Ok(v.subvector(ctx.start, ctx.end))
}
