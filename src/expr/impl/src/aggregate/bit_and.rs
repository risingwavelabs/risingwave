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

use std::marker::PhantomData;
use std::ops::BitAnd;

use risingwave_common::array::I64Array;
use risingwave_common::types::{ListRef, ListValue};
use risingwave_expr::aggregate;

/// Computes the bitwise AND of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int2, b int4, c int8);
///
/// query III
/// select bit_and(a), bit_and(b), bit_and(c) from t;
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
/// select bit_and(a), bit_and(b), bit_and(c) from t;
/// ----
/// 2 2 2
///
/// statement ok
/// drop table t;
/// ```
// XXX: state = "ref" is required so that
// for the first non-null value, the state is set to that value.
#[aggregate("bit_and(*int) -> auto", state = "ref")]
fn bit_and_append_only<T>(state: T, input: T) -> T
where
    T: BitAnd<Output = T>,
{
    state.bitand(input)
}

/// Computes the bitwise AND of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int2, b int4, c int8);
///
/// statement ok
/// create materialized view mv as
/// select bit_and(a) a, bit_and(b) b, bit_and(c) c from t;
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
/// 2 2 2
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
struct BitAndUpdatable<T> {
    _phantom: PhantomData<T>,
}

#[aggregate("bit_and(int2) -> int2", state = "int8[]", generic = "i16")]
#[aggregate("bit_and(int4) -> int4", state = "int8[]", generic = "i32")]
#[aggregate("bit_and(int8) -> int8", state = "int8[]", generic = "i64")]
impl<T: Bits> BitAndUpdatable<T> {
    // state is the number of 0s for each bit.

    fn create_state(&self) -> ListValue {
        ListValue::new(I64Array::from_iter(std::iter::repeat_n(0, T::BITS)).into())
    }

    fn accumulate(&self, mut state: ListValue, input: T) -> ListValue {
        let counts = state.as_i64_mut_slice().expect("invalid state");
        for (i, count) in counts.iter_mut().enumerate() {
            if !input.get_bit(i) {
                *count += 1;
            }
        }
        state
    }

    fn retract(&self, mut state: ListValue, input: T) -> ListValue {
        let counts = state.as_i64_mut_slice().expect("invalid state");
        for (i, count) in counts.iter_mut().enumerate() {
            if !input.get_bit(i) {
                *count -= 1;
            }
        }
        state
    }

    fn finalize(&self, state: ListRef<'_>) -> T {
        let counts = state.as_i64_slice().expect("invalid state");
        let mut result = T::default();
        for (i, count) in counts.iter().enumerate() {
            if *count == 0 {
                result.set_bit(i);
            }
        }
        result
    }
}

pub trait Bits: Default {
    const BITS: usize;
    fn get_bit(&self, i: usize) -> bool;
    fn set_bit(&mut self, i: usize);
}

impl Bits for i16 {
    const BITS: usize = 16;

    fn get_bit(&self, i: usize) -> bool {
        (*self >> i) & 1 == 1
    }

    fn set_bit(&mut self, i: usize) {
        *self |= 1 << i;
    }
}

impl Bits for i32 {
    const BITS: usize = 32;

    fn get_bit(&self, i: usize) -> bool {
        (*self >> i) & 1 == 1
    }

    fn set_bit(&mut self, i: usize) {
        *self |= 1 << i;
    }
}

impl Bits for i64 {
    const BITS: usize = 64;

    fn get_bit(&self, i: usize) -> bool {
        (*self >> i) & 1 == 1
    }

    fn set_bit(&mut self, i: usize) {
        *self |= 1 << i;
    }
}
