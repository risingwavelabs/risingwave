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

use std::collections::HashMap;

risingwave_expr_impl::enable!();

use itertools::Itertools;
use risingwave_expr::sig::{FuncName, FuncSign, SigDataType, FUNCTION_REGISTRY};
#[test]
fn test_func_sig_map() {
    // convert FUNC_SIG_MAP to a more convenient map for testing
    let mut new_map: HashMap<FuncName, HashMap<Vec<SigDataType>, Vec<FuncSign>>> = HashMap::new();
    for sig in FUNCTION_REGISTRY.iter_scalars() {
        // exclude deprecated functions
        if sig.deprecated {
            continue;
        }

        new_map
            .entry(sig.name)
            .or_default()
            .entry(sig.inputs_type.to_vec())
            .or_default()
            .push(sig.clone());
    }

    let mut duplicated: Vec<_> = new_map
        .into_values()
        .flat_map(|funcs_with_same_name| {
            funcs_with_same_name.into_values().filter_map(|v| {
                if v.len() > 1 {
                    Some(format!(
                        "{}({}) -> {}",
                        v[0].name.as_str_name().to_ascii_lowercase(),
                        v[0].inputs_type.iter().format(", "),
                        v.iter().map(|sig| &sig.ret_type).format("/")
                    ))
                } else {
                    None
                }
            })
        })
        .collect();
    duplicated.sort();

    // This snapshot shows the function signatures without a unique match. Frontend has to
    // handle them specially without relying on FuncSigMap.
    let expected = expect_test::expect![[r#"
        [
            "cast(anyarray) -> varchar/anyarray",
            "cast(bigint) -> rw_int256/integer/smallint/numeric/double precision/real/varchar",
            "cast(boolean) -> integer/varchar",
            "cast(date) -> timestamp/varchar",
            "cast(double precision) -> numeric/real/bigint/integer/smallint/varchar",
            "cast(integer) -> rw_int256/smallint/numeric/double precision/real/bigint/boolean/varchar",
            "cast(interval) -> time/varchar",
            "cast(jsonb) -> boolean/double precision/real/numeric/bigint/integer/smallint/varchar",
            "cast(numeric) -> double precision/real/bigint/integer/smallint/varchar",
            "cast(real) -> numeric/bigint/integer/smallint/double precision/varchar",
            "cast(rw_int256) -> double precision/varchar",
            "cast(smallint) -> rw_int256/numeric/double precision/real/bigint/integer/varchar",
            "cast(time) -> interval/varchar",
            "cast(timestamp) -> time/date/varchar",
            "cast(varchar) -> jsonb/interval/timestamp/time/date/rw_int256/real/double precision/numeric/smallint/integer/bigint/varchar/boolean/bytea/anyarray",
        ]
    "#]];
    expected.assert_debug_eq(&duplicated);
}
