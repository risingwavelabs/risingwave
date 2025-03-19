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

use risingwave_expr::aggregate;

/// Returns true if all non-null input values are true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b1 boolean, b2 boolean, b3 boolean, b4 boolean);
///
/// query T
/// select bool_and(b1) from t;
/// ----
/// NULL
///
/// statement ok
/// insert into t values
///     (true,  null, false, null),
///     (false, true, null,  null),
///     (null,  true, false, null);
///
/// query TTTTTT
/// select
///     bool_and(b1),
///     bool_and(b2),
///     bool_and(b3),
///     bool_and(b4),
///     bool_and(NOT b2),
///     bool_and(NOT b3)
/// FROM t;
/// ----
/// f t f NULL f t
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_and(boolean) -> boolean", state = "ref")]
fn bool_and_append_only(state: bool, input: bool) -> bool {
    state && input
}

/// Returns true if all non-null input values are true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b boolean);
///
/// statement ok
/// create materialized view mv as select bool_and(b) from t;
///
/// query T
/// select * from mv;
/// ----
/// NULL
///
/// statement ok
/// insert into t values (true), (false), (null);
///
/// query T
/// select * from mv;
/// ----
/// f
///
/// statement ok
/// delete from t where b is false;
///
/// query T
/// select * from mv;
/// ----
/// t
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BoolAndUpdatable;

#[aggregate("bool_and(boolean) -> boolean", state = "int8")]
impl BoolAndUpdatable {
    // state is the number of false values

    fn accumulate(&self, state: i64, input: bool) -> i64 {
        if input { state } else { state + 1 }
    }

    fn retract(&self, state: i64, input: bool) -> i64 {
        if input { state } else { state - 1 }
    }

    #[allow(dead_code)] // TODO: support merge
    fn merge(&self, state1: i64, state2: i64) -> i64 {
        state1 + state2
    }

    fn finalize(&self, state: i64) -> bool {
        state == 0
    }
}
