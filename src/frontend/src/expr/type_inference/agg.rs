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
use std::sync::LazyLock;

use risingwave_common::types::{DataType, DataTypeName};
use risingwave_expr::expr::AggKind;

// Use AggCall to infer return type
use super::super::AggCall;

// Same as FuncSign in type_inference/func.rs except this is for aggregate function
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AggFuncSig {
    pub func: AggKind,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

// Same as FuncSigMap in type_inference/func.rs except this is for aggregate function
#[derive(Default)]
pub struct AggFuncSigMap(HashMap<(AggKind, usize), Vec<AggFuncSig>>);
impl AggFuncSigMap {
    fn insert(&mut self, func: AggKind, param_types: Vec<DataTypeName>, ret_type: DataTypeName) {
        let arity = param_types.len();
        let inputs_type = param_types.into_iter().map(Into::into).collect();
        let sig = AggFuncSig {
            func,
            inputs_type,
            ret_type,
        };
        self.0.entry((func, arity)).or_default().push(sig)
    }
}

/// This function builds type derived map for all built-in aggregate functions that take a fixed
/// number of arguments (In fact mostly is one).
static AGG_FUNC_SIG_MAP: LazyLock<AggFuncSigMap> = LazyLock::new(|| {
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
        A::ApproxCountDistinct,
    ] {
        for input in all_types {
            match AggCall::infer_return_type(&agg, &[DataType::from(input)]) {
                Ok(v) => map.insert(agg, vec![input], DataTypeName::from(v)),
                Err(_e) => continue,
            }
        }
    }
    // Handle special case for `string_agg`, for it accepts two input arguments.
    map.insert(
        AggKind::StringAgg,
        vec![DataTypeName::Varchar, DataTypeName::Varchar],
        DataTypeName::Varchar,
    );
    map
});

/// The table of function signatures.
pub fn agg_func_sigs() -> impl Iterator<Item = &'static AggFuncSig> {
    AGG_FUNC_SIG_MAP.0.values().flatten()
}
