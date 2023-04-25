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

use itertools::Itertools;
use risingwave_common::types::{DataType, DataTypeName};

use crate::agg::{AggCall, AggKind, BoxedAggState};
use crate::Result;

pub static AGG_FUNC_SIG_MAP: LazyLock<AggFuncSigMap> = LazyLock::new(|| unsafe {
    let mut map = AggFuncSigMap::default();
    tracing::info!("{} aggregations loaded.", AGG_FUNC_SIG_MAP_INIT.len());
    for desc in AGG_FUNC_SIG_MAP_INIT.drain(..) {
        map.insert(desc);
    }
    map
});

// Same as FuncSign in func.rs except this is for aggregate function
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AggFuncSig {
    pub func: AggKind,
    pub inputs_type: &'static [DataTypeName],
    pub ret_type: DataTypeName,
    pub build: fn(agg: AggCall) -> Result<BoxedAggState>,
}

impl AggFuncSig {
    /// Returns a string describing the aggregation without return type.
    pub fn to_string_no_return(&self) -> String {
        format!(
            "{}({})->{:?}",
            self.func,
            self.inputs_type.iter().map(|t| format!("{t:?}")).join(","),
            self.ret_type
        )
        .to_lowercase()
    }
}

// Same as FuncSigMap in func.rs except this is for aggregate function
#[derive(Default)]
pub struct AggFuncSigMap(HashMap<(AggKind, usize), Vec<AggFuncSig>>);

impl AggFuncSigMap {
    fn insert(&mut self, sig: AggFuncSig) {
        let arity = sig.inputs_type.len();
        self.0.entry((sig.func, arity)).or_default().push(sig);
    }

    /// Returns a function signature with the same type, argument types and return type.
    pub fn get(
        &self,
        ty: AggKind,
        args: &[DataTypeName],
        ret: DataTypeName,
    ) -> Option<&AggFuncSig> {
        let v = self.0.get(&(ty, args.len()))?;
        v.iter()
            .find(|d| d.inputs_type == args && d.ret_type == ret)
    }
}

/// The table of function signatures.
pub fn agg_func_sigs() -> impl Iterator<Item = &'static AggFuncSig> {
    AGG_FUNC_SIG_MAP.0.values().flatten()
}

/// Register a function into global registry.
///
/// # Safety
///
/// This function must be called sequentially.
///
/// It is designed to be used by `#[function]` macro.
/// Users SHOULD NOT call this function.
#[doc(hidden)]
pub unsafe fn _register(desc: AggFuncSig) {
    AGG_FUNC_SIG_MAP_INIT.push(desc);
}

/// The global registry of function signatures on initialization.
///
/// `#[function]` macro will generate a `#[ctor]` function to register the signature into this
/// vector. The calls are guaranteed to be sequential. The vector will be drained and moved into
/// `FUNC_SIG_MAP` on the first access of `FUNC_SIG_MAP`.
static mut AGG_FUNC_SIG_MAP_INIT: Vec<AggFuncSig> = Vec::new();

/// Infer the return type for the given agg call.
/// Returns `None` if not supported or the arguments are invalid.
pub fn infer_return_type(agg_kind: AggKind, inputs: &[DataType]) -> Option<DataType> {
    // The function signatures are aligned with postgres, see
    // https://www.postgresql.org/docs/current/functions-aggregate.html.
    let return_type = match (agg_kind, inputs) {
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
