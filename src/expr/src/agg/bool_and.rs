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

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::ScalarImpl;
use risingwave_expr_macro::aggregate;

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
#[aggregate("bool_and(boolean) -> boolean")]
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
#[aggregate(
    "bool_and(boolean) -> boolean",
    state = "BoolAndState",
    state_type = "int64"
)]
fn bool_and(mut state: BoolAndState, input: bool, retract: bool) -> BoolAndState {
    if input == false {
        if retract {
            state.false_count -= 1;
        } else {
            state.false_count += 1;
        }
    }
    state
}

/// State for retractable `bool_and` aggregate.
#[derive(Debug, Default, Clone, EstimateSize)]
struct BoolAndState {
    false_count: i64,
}

// state -> result
impl From<BoolAndState> for bool {
    fn from(state: BoolAndState) -> Self {
        state.false_count == 0
    }
}

// first input -> state
impl From<bool> for BoolAndState {
    fn from(b: bool) -> Self {
        Self {
            false_count: if b { 0 } else { 1 },
        }
    }
}

impl From<ScalarImpl> for BoolAndState {
    fn from(d: ScalarImpl) -> Self {
        let ScalarImpl::Int64(v) = d else {
            panic!("unexpected state for `bool_and`: {:?}", d);
        };
        Self { false_count: v }
    }
}

impl From<BoolAndState> for ScalarImpl {
    fn from(state: BoolAndState) -> Self {
        ScalarImpl::Int64(state.false_count)
    }
}
