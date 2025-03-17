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

/// Returns true if any non-null input value is true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b1 boolean, b2 boolean, b3 boolean, b4 boolean);
///
/// query T
/// select bool_or(b1) from t;
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
///     bool_or(b1),
///     bool_or(b2),
///     bool_or(b3),
///     bool_or(b4),
///     bool_or(NOT b2),
///     bool_or(NOT b3)
/// FROM t;
/// ----
/// t t f NULL f t
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_or(boolean) -> boolean")]
fn bool_or_append_only(state: bool, input: bool) -> bool {
    state || input
}

/// Returns true if any non-null input value is true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b boolean);
///
/// statement ok
/// create materialized view mv as select bool_or(b) from t;
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
/// t
///
/// statement ok
/// delete from t where b is true;
///
/// query T
/// select * from mv;
/// ----
/// f
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[derive(Debug, Default, Clone)]
struct BoolOrUpdatable;

#[aggregate("bool_or(boolean) -> boolean", state = "int8")]
impl BoolOrUpdatable {
    // state is the number of true values

    fn accumulate(&self, state: i64, input: bool) -> i64 {
        if input { state + 1 } else { state }
    }

    fn retract(&self, state: i64, input: bool) -> i64 {
        if input { state - 1 } else { state }
    }

    #[allow(dead_code)] // TODO: support merge
    fn merge(&self, state1: i64, state2: i64) -> i64 {
        state1 + state2
    }

    fn finalize(&self, state: i64) -> bool {
        state != 0
    }
}
