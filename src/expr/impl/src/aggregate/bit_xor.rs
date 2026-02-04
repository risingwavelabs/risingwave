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

use std::ops::BitXor;

use risingwave_expr::aggregate;

/// Computes the bitwise XOR of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int2, b int4, c int8);
///
/// query III
/// select bit_xor(a), bit_xor(b), bit_xor(c) from t;
/// ----
/// NULL NULL NULL
///
/// statement ok
/// insert into t values
///    (3, 3, 3),
///    (6, 6, 6),
///    (null, null, null);
///
/// query III
/// select bit_xor(a), bit_xor(b), bit_xor(c) from t;
/// ----
/// 5 5 5
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bit_xor(*int) -> auto")]
fn bit_xor<T>(state: T, input: T, _retract: bool) -> T
where
    T: BitXor<Output = T>,
{
    state.bitxor(input)
}
