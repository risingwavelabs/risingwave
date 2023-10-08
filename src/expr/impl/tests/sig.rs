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

use std::collections::BTreeMap;

risingwave_expr_impl::enable!();

use itertools::Itertools;
use risingwave_common::types::DataTypeName;
use risingwave_expr::sig::func::{func_sigs, FuncSign};
use risingwave_pb::expr::expr_node::PbType;

#[test]
fn test_func_sig_map() {
    // convert FUNC_SIG_MAP to a more convenient map for testing
    let mut new_map: BTreeMap<PbType, BTreeMap<Vec<DataTypeName>, Vec<FuncSign>>> = BTreeMap::new();
    for sig in func_sigs() {
        // exclude deprecated functions
        if sig.deprecated {
            continue;
        }

        new_map
            .entry(sig.func)
            .or_default()
            .entry(sig.inputs_type.to_vec())
            .or_default()
            .push(sig.clone());
    }

    let duplicated: BTreeMap<_, Vec<_>> = new_map
        .into_iter()
        .filter_map(|(k, funcs_with_same_name)| {
            let funcs_with_same_name_type: Vec<_> = funcs_with_same_name
                .into_values()
                .filter_map(|v| {
                    if v.len() > 1 {
                        Some(
                            format!(
                                "{:}({:?}) -> {:?}",
                                v[0].func.as_str_name(),
                                v[0].inputs_type.iter().format(", "),
                                v.iter().map(|sig| sig.ret_type).format("/")
                            )
                            .to_ascii_lowercase(),
                        )
                    } else {
                        None
                    }
                })
                .collect();
            if !funcs_with_same_name_type.is_empty() {
                Some((k, funcs_with_same_name_type))
            } else {
                None
            }
        })
        .collect();

    // This snapshot shows the function signatures without a unique match. Frontend has to
    // handle them specially without relying on FuncSigMap.
    let expected = expect_test::expect![[r#"
        {
            Cast: [
                "cast(boolean) -> int32/varchar",
                "cast(int16) -> int256/decimal/float64/float32/int64/int32/varchar",
                "cast(int32) -> int256/int16/decimal/float64/float32/int64/boolean/varchar",
                "cast(int64) -> int256/int32/int16/decimal/float64/float32/varchar",
                "cast(float32) -> decimal/int64/int32/int16/float64/varchar",
                "cast(float64) -> decimal/float32/int64/int32/int16/varchar",
                "cast(decimal) -> float64/float32/int64/int32/int16/varchar",
                "cast(date) -> timestamp/varchar",
                "cast(varchar) -> jsonb/interval/timestamp/time/date/int256/float32/float64/decimal/int16/int32/int64/varchar/boolean/bytea/list",
                "cast(time) -> interval/varchar",
                "cast(timestamp) -> time/date/varchar",
                "cast(interval) -> time/varchar",
                "cast(list) -> varchar/list",
                "cast(jsonb) -> boolean/float64/float32/decimal/int64/int32/int16/varchar",
                "cast(int256) -> float64/varchar",
            ],
            ArrayAccess: [
                "array_access(list, int32) -> boolean/int16/int32/int64/int256/float32/float64/decimal/serial/date/time/timestamp/timestamptz/interval/varchar/bytea/jsonb/list/struct",
            ],
            ArrayMin: [
                "array_min(list) -> bytea/varchar/timestamptz/timestamp/time/date/int256/serial/decimal/float32/float64/int16/int32/int64",
            ],
            ArrayMax: [
                "array_max(list) -> bytea/varchar/timestamptz/timestamp/time/date/int256/serial/decimal/float32/float64/int16/int32/int64",
            ],
            ArraySum: [
                "array_sum(list) -> interval/decimal/float64/float32/int64",
            ],
        }
        "#]];
    expected.assert_debug_eq(&duplicated);
}
