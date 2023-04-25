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
use std::sync::LazyLock;

use risingwave_common::types::{DataType, DataTypeName};

use crate::function::aggregate::AggKind;

// Same as FuncSign in func.rs except this is for aggregate function
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AggFuncSig {
    pub func: AggKind,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

impl AggFuncSig {
    /// Returns a string describing the aggregation without return type.
    pub fn to_string_no_return(&self) -> String {
        format!(
            "{}({})",
            self.func,
            self.inputs_type
                .iter()
                .map(|t| format!("{t:?}"))
                .collect::<Vec<_>>()
                .join(",")
        )
        .to_lowercase()
    }
}

// Same as FuncSigMap in func.rs except this is for aggregate function
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
        T::Timestamptz,
        T::Time,
        T::Interval,
    ];

    // Call infer_return_type to check the return type. If it throw error shows that the type is not
    // inferred.
    for agg in [
        A::BitAnd,
        A::BitOr,
        A::BitXor,
        A::Sum,
        A::Min,
        A::Max,
        A::Count,
        A::Avg,
        A::ApproxCountDistinct,
    ] {
        for input in all_types {
            if let Some(v) = infer_return_type(&agg, &[DataType::from(input)]) {
                map.insert(agg, vec![input], DataTypeName::from(v));
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

/// Infer the return type for the given agg call.
/// Returns `None` if not supported or the arguments are invalid.
pub fn infer_return_type(agg_kind: &AggKind, inputs: &[DataType]) -> Option<DataType> {
    // The function signatures are aligned with postgres, see
    // https://www.postgresql.org/docs/current/functions-aggregate.html.
    let return_type = match (&agg_kind, inputs) {
        // Min, Max, FirstValue, BitAnd, BitOr, BitXor
        (
            AggKind::Min
            | AggKind::Max
            | AggKind::FirstValue
            | AggKind::BitAnd
            | AggKind::BitOr
            | AggKind::BitXor,
            [input],
        ) => input.clone(),
        (
            AggKind::Min
            | AggKind::Max
            | AggKind::FirstValue
            | AggKind::BitAnd
            | AggKind::BitOr
            | AggKind::BitXor,
            _,
        ) => return None,
        // Avg
        (AggKind::Avg, [input]) => match input {
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Decimal => {
                DataType::Decimal
            }
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Int256 => DataType::Float64,
            DataType::Interval => DataType::Interval,
            _ => return None,
        },
        (AggKind::Avg, _) => return None,

        // Sum
        (AggKind::Sum, [input]) => match input {
            DataType::Int16 => DataType::Int64,
            DataType::Int32 => DataType::Int64,
            DataType::Int64 => DataType::Decimal,
            DataType::Int256 => DataType::Int256,
            DataType::Decimal => DataType::Decimal,
            DataType::Float32 => DataType::Float32,
            DataType::Float64 => DataType::Float64,
            DataType::Interval => DataType::Interval,
            _ => return None,
        },

        (AggKind::Sum, _) => return None,

        // StdDev/Var, stddev_pop, stddev_samp, var_pop, var_samp
        (
            AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp,
            [input],
        ) => match input {
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Decimal => {
                DataType::Decimal
            }
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Int256 => DataType::Float64,
            _ => return None,
        },

        (AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp, _) => {
            return None
        }

        (AggKind::Sum0, [DataType::Int64]) => DataType::Int64,
        (AggKind::Sum0, _) => return None,

        // ApproxCountDistinct
        (AggKind::ApproxCountDistinct, [_]) => DataType::Int64,
        (AggKind::ApproxCountDistinct, _) => return None,

        // Count
        (AggKind::Count, [] | [_]) => DataType::Int64,
        (AggKind::Count, _) => return None,

        // StringAgg
        (AggKind::StringAgg, [DataType::Varchar, DataType::Varchar]) => DataType::Varchar,
        (AggKind::StringAgg, _) => return None,

        // ArrayAgg
        (AggKind::ArrayAgg, [input]) => DataType::List {
            datatype: Box::new(input.clone()),
        },
        (AggKind::ArrayAgg, _) => return None,
    };

    Some(return_type)
}
