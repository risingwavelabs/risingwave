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

use std::ops::BitAnd;

use risingwave_common::types::{ListRef, ListValue, ScalarImpl};
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
/// create table t (a int2);
///
/// statement ok
/// create materialized view mv as select bit_and(a) from t;
///
/// query I
/// select * from mv;
/// ----
/// NULL
///
/// statement ok
/// insert into t values (3), (6), (null);
///
/// query I
/// select * from mv;
/// ----
/// 2
///
/// statement ok
/// delete from t where a = 3;
///
/// query I
/// select * from mv;
/// ----
/// 6
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BitAndI16Updatable;

#[aggregate("bit_and(int2) -> int2", state = "int8[]")]
impl BitAndI16Updatable {
    // state is the number of 0s for each bit.

    fn create_state(&self) -> ListValue {
        ListValue::new(vec![Some(ScalarImpl::Int64(0)); 16])
    }

    fn accumulate(&self, mut state: ListValue, input: i16) -> ListValue {
        for i in 0..16 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count += 1;
            }
        }
        state
    }

    fn retract(&self, mut state: ListValue, input: i16) -> ListValue {
        for i in 0..16 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count -= 1;
            }
        }
        state
    }

    fn finalize(&self, state: ListRef<'_>) -> i16 {
        let mut result = 0;
        for i in 0..16 {
            let count = state.get(i).unwrap().unwrap().into_int64();
            if count == 0 {
                result |= 1 << i;
            }
        }
        result
    }
}

/// Computes the bitwise AND of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int4);
///
/// statement ok
/// create materialized view mv as select bit_and(a) from t;
///
/// query I
/// select * from mv;
/// ----
/// NULL
///
/// statement ok
/// insert into t values (3), (6), (null);
///
/// query I
/// select * from mv;
/// ----
/// 2
///
/// statement ok
/// delete from t where a = 3;
///
/// query I
/// select * from mv;
/// ----
/// 6
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BitAndI32Updatable;

#[aggregate("bit_and(int4) -> int4", state = "int8[]")]
impl BitAndI32Updatable {
    // state is the number of 0s for each bit.

    fn create_state(&self) -> ListValue {
        ListValue::new(vec![Some(ScalarImpl::Int64(0)); 32])
    }

    fn accumulate(&self, mut state: ListValue, input: i32) -> ListValue {
        for i in 0..32 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count += 1;
            }
        }
        state
    }

    fn retract(&self, mut state: ListValue, input: i32) -> ListValue {
        for i in 0..32 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count -= 1;
            }
        }
        state
    }

    fn finalize(&self, state: ListRef<'_>) -> i32 {
        let mut result = 0;
        for i in 0..32 {
            let count = state.get(i).unwrap().unwrap().into_int64();
            if count == 0 {
                result |= 1 << i;
            }
        }
        result
    }
}

/// Computes the bitwise AND of all non-null input values.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (a int8);
///
/// statement ok
/// create materialized view mv as select bit_and(a) from t;
///
/// query I
/// select * from mv;
/// ----
/// NULL
///
/// statement ok
/// insert into t values (3), (6), (null);
///
/// query I
/// select * from mv;
/// ----
/// 2
///
/// statement ok
/// delete from t where a = 3;
///
/// query I
/// select * from mv;
/// ----
/// 6
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BitAndI64Updatable;

#[aggregate("bit_and(int8) -> int8", state = "int8[]")]
impl BitAndI64Updatable {
    // state is the number of 0s for each bit.

    fn create_state(&self) -> ListValue {
        ListValue::new(vec![Some(ScalarImpl::Int64(0)); 64])
    }

    fn accumulate(&self, mut state: ListValue, input: i64) -> ListValue {
        for i in 0..64 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count += 1;
            }
        }
        state
    }

    fn retract(&self, mut state: ListValue, input: i64) -> ListValue {
        for i in 0..64 {
            if (input >> i) & 1 == 0 {
                let Some(ScalarImpl::Int64(count)) = &mut state[i] else {
                    panic!("invalid state");
                };
                *count -= 1;
            }
        }
        state
    }

    fn finalize(&self, state: ListRef<'_>) -> i64 {
        let mut result = 0;
        for i in 0..64 {
            let count = state.get(i).unwrap().unwrap().into_int64();
            if count == 0 {
                result |= 1 << i;
            }
        }
        result
    }
}
