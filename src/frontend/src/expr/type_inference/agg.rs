// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::super::AggCall;
use super::DataTypeName;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AggFuncSign {
    pub func: AggKind,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

#[derive(Default)]
pub struct AggFuncSigMap(HashMap<(AggKind, usize), Vec<AggFuncSign>>);
impl AggFuncSigMap {
    fn insert(&mut self, func: AggKind, param_types: Vec<DataTypeName>, ret_type: DataTypeName) {
        let arity = param_types.len();
        let inputs_type = param_types.into_iter().map(Into::into).collect();
        let sig = AggFuncSign {
            func: func.clone(),
            inputs_type,
            ret_type,
        };
        self.0.entry((func, arity)).or_default().push(sig)
    }
}

/// This function builds type derived map for all built-in functions that take a fixed number
/// of arguments.  They can be determined to have one or more type signatures since some are
/// compatible with more than one type.
/// Type signatures and arities of variadic functions are checked
/// [elsewhere](crate::expr::FunctionCall::new).
fn build_type_derive_map() -> AggFuncSigMap {
    use {AggKind as A, DataTypeName as T};
    let mut map = AggFuncSigMap::default();

    let all_types = [
        T::Boolean,
        T::Int16,
        T::Int32,
        T::Int64,
        T::Decimal,
        T::Float32,
        T::Float64,
        T::Varchar,
        T::Date,
        T::Timestamp,
        T::Timestampz,
        T::Time,
        T::Interval,
    ];

    // Call infer_return_type to check the return type. If it throw error shows that the type is not
    // inferred.
    for agg in [
        A::Sum,
        A::Min,
        A::Max,
        A::Count,
        A::Avg,
        A::StringAgg,
        A::SingleValue,
        A::ApproxCountDistinct,
    ] {
        for input in all_types {
            match AggCall::infer_return_type(&agg, &[DataType::from(input)]) {
                Ok(v) => map.insert(agg.clone(), vec![input], DataTypeName::from(v)),
                Err(_e) => continue,
            }
        }
    }
    map
}

lazy_static::lazy_static! {
    static ref AGG_FUNC_SIG_MAP: AggFuncSigMap = {
        build_type_derive_map()
    };
}

/// The table of function signatures.
pub fn agg_func_sigs() -> impl Iterator<Item = &'static AggFuncSign> {
    AGG_FUNC_SIG_MAP.0.values().flatten()
}
