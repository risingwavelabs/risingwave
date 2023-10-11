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

use std::marker::PhantomData;
use std::ops::BitOr;

use risingwave_common::types::{ListRef, ListValue, ScalarImpl};
use risingwave_expr::aggregate;

use super::bit_and::Bits;

/// Computes the bitwise OR of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int2, b int4, c int8);
///
/// query III
/// select bit_or(a), bit_or(b), bit_or(c) from t;
/// ----
/// NULL NULL NULL
///
/// statement ok
/// insert into t values
///    (1, 1, 1),
///    (2, 2, 2),
///    (null, null, null);
///
/// query III
/// select bit_or(a), bit_or(b), bit_or(c) from t;
/// ----
/// 3 3 3
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bit_or(*int) -> auto")]
fn bit_or_append_only<T>(state: T, input: T) -> T
where
    T: BitOr<Output = T>,
{
    state.bitor(input)
}

/// Computes the bitwise OR of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int2, b int4, c int8);
///
/// statement ok
/// create materialized view mv as
/// select bit_or(a) a, bit_or(b) b, bit_or(c) c from t;
///
/// query III
/// select * from mv;
/// ----
/// NULL NULL NULL
///
/// statement ok
/// insert into t values
///    (6, 6, 6),
///    (3, 3, 3),
///    (null, null, null);
///
/// query III
/// select * from mv;
/// ----
/// 7 7 7
///
/// statement ok
/// delete from t where a = 3;
///
/// query III
/// select * from mv;
/// ----
/// 6 6 6
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BitOrUpdatable<T> {
    _phantom: PhantomData<T>,
}

#[aggregate("bit_or(int2) -> int2", state = "int8[]", generic = "i16")]
#[aggregate("bit_or(int4) -> int4", state = "int8[]", generic = "i32")]
#[aggregate("bit_or(int8) -> int8", state = "int8[]", generic = "i64")]
impl<T: Bits> BitOrUpdatable<T> {
    // state is the number of 1s for each bit.

    fn create_state(&self) -> ListValue {
        ListValue::new(vec![Some(ScalarImpl::Int64(0)); T::BITS])
    }

    fn accumulate(&self, mut state: ListValue, input: T) -> ListValue {
        for i in 0..T::BITS {
            if input.get_bit(i) {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count += 1;
            }
        }
        state
    }

    fn retract(&self, mut state: ListValue, input: T) -> ListValue {
        for i in 0..T::BITS {
            if input.get_bit(i) {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count -= 1;
            }
        }
        state
    }

    fn finalize(&self, state: ListRef<'_>) -> T {
        let mut result = T::default();
        for i in 0..T::BITS {
            let count = state.get(i).unwrap().unwrap().into_int64();
            if count != 0 {
                result.set_bit(i);
            }
        }
        result
    }
}
