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

//! Metadata of expressions.

use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::PbType as ScalarFunctionType;
use risingwave_pb::expr::table_function::PbType as TableFunctionType;

use crate::agg::{AggCall, AggKind as AggregateFunctionType, BoxedAggregateFunction};
use crate::error::Result;
use crate::expr::BoxedExpression;
use crate::table_function::BoxedTableFunction;
use crate::ExprError;

pub mod cast;

pub static FUNC_SIG_MAP: LazyLock<FuncSigMap> = LazyLock::new(|| unsafe {
    let mut map = FuncSigMap::default();
    tracing::info!("{} function signatures loaded.", FUNC_SIG_MAP_INIT.len());
    for desc in FUNC_SIG_MAP_INIT.drain(..) {
        map.insert(desc);
    }
    map
});

/// The table of function signatures.
pub fn func_sigs() -> impl Iterator<Item = &'static FuncSign> {
    FUNC_SIG_MAP.0.values().flatten()
}

#[derive(Default, Clone, Debug)]
pub struct FuncSigMap(HashMap<FuncName, Vec<FuncSign>>);

impl FuncSigMap {
    /// Inserts a function signature.
    pub fn insert(&mut self, desc: FuncSign) {
        self.0.entry(desc.name).or_default().push(desc)
    }

    /// Returns a function signature with the same type, argument types and return type.
    /// Deprecated functions are included.
    pub fn get(
        &self,
        name: impl Into<FuncName>,
        args: &[DataType],
        ret: &DataType,
    ) -> Option<&FuncSign> {
        let v = self.0.get(&name.into())?;
        v.iter().find(|d| d.match_args_ret(args, ret))
    }

    /// Returns all function signatures with the same type and number of arguments.
    /// Deprecated functions are excluded.
    pub fn get_with_arg_nums(&self, name: impl Into<FuncName>, nargs: usize) -> Vec<&FuncSign> {
        match self.0.get(&name.into()) {
            Some(v) => v
                .iter()
                .filter(|d| d.match_number_of_args(nargs) && !d.deprecated)
                .collect(),
            None => vec![],
        }
    }

    /// Returns a function signature with the given type, argument types, return type.
    ///
    /// The `append_only` flag only works when both append-only and retractable version exist.
    /// Otherwise, return the signature of the only version.
    pub fn get_aggregate(
        &self,
        ty: AggregateFunctionType,
        args: &[DataType],
        ret: &DataType,
        append_only: bool,
    ) -> Option<&FuncSign> {
        let v = self.0.get(&ty.into())?;
        let mut iter = v.iter().filter(|d| d.match_type(args, ret));
        if iter.clone().count() == 2 {
            iter.find(|d| d.append_only == append_only)
        } else {
            iter.next()
        }
    }

    /// Returns the return type for the given function and arguments.
    pub fn get_return_type(
        &self,
        name: impl Into<FuncName>,
        args: &[DataType],
    ) -> Result<DataType> {
        let name = name.into();
        let v = self
            .0
            .get(&name)
            .ok_or_else(|| ExprError::UnsupportedFunction(name.to_string()))?;
        let sig = v
            .iter()
            .find(|d| d.match_args(args))
            .ok_or_else(|| ExprError::UnsupportedFunction(name.to_string()))?;
        (sig.type_infer)(args)
    }
}

/// A function signature.
#[derive(Clone)]
pub struct FuncSign {
    /// The name of the function.
    pub name: FuncName,

    /// The argument types.
    pub inputs_type: Vec<SigDataType>,

    /// Whether the function is variadic.
    pub variadic: bool,

    /// The return type.
    pub ret_type: SigDataType,

    /// A function to build the expression.
    pub build: FuncBuilder,

    /// A function to infer the return type from argument types.
    pub type_infer: fn(args: &[DataType]) -> Result<DataType>,

    /// Whether the function is deprecated and should not be used in the frontend.
    /// For backward compatibility, it is still available in the backend.
    pub deprecated: bool,

    /// The state type of the aggregate function.
    pub state_type: Option<DataType>,

    /// Whether the aggregate function is append-only.
    pub append_only: bool,
}

impl fmt::Debug for FuncSign {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}{}) -> {}{}",
            self.name.as_str_name().to_ascii_lowercase(),
            self.inputs_type.iter().format(", "),
            if self.variadic {
                if self.inputs_type.is_empty() {
                    "..."
                } else {
                    ", ..."
                }
            } else {
                ""
            },
            if self.name.is_table() { "setof " } else { "" },
            self.ret_type,
        )?;
        if self.append_only {
            write!(f, " [append-only]")?;
        }
        if self.deprecated {
            write!(f, " [deprecated]")?;
        }
        Ok(())
    }
}

impl FuncSign {
    /// Returns true if the argument types match the function signature.
    pub fn match_args(&self, args: &[DataType]) -> bool {
        if !self.match_number_of_args(args.len()) {
            return false;
        }
        self.inputs_type
            .iter()
            .zip(args.iter())
            .all(|(matcher, arg)| matcher.matches(arg))
    }

    /// Returns true if the argument types match the function signature.
    fn match_args_ret(&self, args: &[DataType], ret: &DataType) -> bool {
        self.match_args(args) && self.ret_type.matches(ret)
    }

    /// Returns true if the number of arguments matches the function signature.
    fn match_number_of_args(&self, n: usize) -> bool {
        if self.variadic {
            n >= self.inputs_type.len()
        } else {
            n == self.inputs_type.len()
        }
    }

    /// Returns true if the function is a scalar function.
    pub const fn is_scalar(&self) -> bool {
        matches!(self.name, FuncName::Scalar(_))
    }

    /// Returns true if the function is a table function.
    pub const fn is_table_function(&self) -> bool {
        matches!(self.name, FuncName::Table(_))
    }

    /// Returns true if the function is a aggregate function.
    pub const fn is_aggregate(&self) -> bool {
        matches!(self.name, FuncName::Aggregate(_))
    }

    /// Builds the scalar function.
    pub fn build_scalar(
        &self,
        return_type: DataType,
        children: Vec<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        match self.build {
            FuncBuilder::Scalar(f) => f(return_type, children),
            _ => panic!("Expected a scalar function"),
        }
    }

    /// Builds the table function.
    pub fn build_table(
        &self,
        return_type: DataType,
        chunk_size: usize,
        children: Vec<BoxedExpression>,
    ) -> Result<BoxedTableFunction> {
        match self.build {
            FuncBuilder::Table(f) => f(return_type, chunk_size, children),
            _ => panic!("Expected a table function"),
        }
    }

    /// Builds the aggregate function.
    pub fn build_aggregate(&self, agg: &AggCall) -> Result<BoxedAggregateFunction> {
        match self.build {
            FuncBuilder::Aggregate(f) => f(agg),
            _ => panic!("Expected an aggregate function"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FuncName {
    Scalar(ScalarFunctionType),
    Table(TableFunctionType),
    Aggregate(AggregateFunctionType),
}

impl From<ScalarFunctionType> for FuncName {
    fn from(ty: ScalarFunctionType) -> Self {
        Self::Scalar(ty)
    }
}

impl From<TableFunctionType> for FuncName {
    fn from(ty: TableFunctionType) -> Self {
        Self::Table(ty)
    }
}

impl From<AggregateFunctionType> for FuncName {
    fn from(ty: AggregateFunctionType) -> Self {
        Self::Aggregate(ty)
    }
}

impl fmt::Display for FuncName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str_name().to_ascii_lowercase())
    }
}

impl FuncName {
    /// Returns the name of the function in `UPPER_CASE` style.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Scalar(ty) => ty.as_str_name(),
            Self::Table(ty) => ty.as_str_name(),
            Self::Aggregate(ty) => ty.to_protobuf().as_str_name(),
        }
    }

    /// Returns true if the function is a table function.
    const fn is_table(&self) -> bool {
        matches!(self, Self::Table(_))
    }

    pub fn as_scalar(&self) -> ScalarFunctionType {
        match self {
            Self::Scalar(ty) => *ty,
            _ => panic!("Expected a scalar function"),
        }
    }

    pub fn as_aggregate(&self) -> AggregateFunctionType {
        match self {
            Self::Aggregate(ty) => *ty,
            _ => panic!("Expected an aggregate function"),
        }
    }
}

/// An extended data type that can be used to declare a function's argument or result type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SigDataType {
    /// Exact data type
    Exact(DataType),
    /// Accepts any data type
    Any,
    /// Accepts any array data type
    AnyArray,
    /// Accepts any struct data type
    AnyStruct,
}

impl From<DataType> for SigDataType {
    fn from(dt: DataType) -> Self {
        SigDataType::Exact(dt)
    }
}

impl std::fmt::Display for SigDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact(dt) => write!(f, "{}", dt),
            Self::Any => write!(f, "any"),
            Self::AnyArray => write!(f, "anyarray"),
            Self::AnyStruct => write!(f, "anystruct"),
        }
    }
}

impl SigDataType {
    /// Returns true if the data type matches.
    pub fn matches(&self, dt: &DataType) -> bool {
        match self {
            Self::Exact(ty) => ty == dt,
            Self::Any => true,
            Self::AnyArray => dt.is_array(),
            Self::AnyStruct => dt.is_struct(),
        }
    }

    /// Returns the exact data type.
    pub fn as_exact(&self) -> &DataType {
        match self {
            Self::Exact(ty) => ty,
            t => panic!("expected data type, but got: {t}"),
        }
    }

    /// Returns true if the data type is exact.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

#[derive(Clone, Copy)]
pub enum FuncBuilder {
    Scalar(fn(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression>),
    Table(
        fn(
            return_type: DataType,
            chunk_size: usize,
            children: Vec<BoxedExpression>,
        ) -> Result<BoxedTableFunction>,
    ),
    Aggregate(fn(agg: &AggCall) -> Result<BoxedAggregateFunction>),
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
pub unsafe fn _register(desc: FuncSign) {
    FUNC_SIG_MAP_INIT.push(desc)
}

/// The global registry of function signatures on initialization.
///
/// `#[function]` macro will generate a `#[ctor]` function to register the signature into this
/// vector. The calls are guaranteed to be sequential. The vector will be drained and moved into
/// `FUNC_SIG_MAP` on the first access of `FUNC_SIG_MAP`.
static mut FUNC_SIG_MAP_INIT: Vec<FuncSign> = Vec::new();

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_func_sig_map() {
        // convert FUNC_SIG_MAP to a more convenient map for testing
        let mut new_map: HashMap<FuncName, HashMap<Vec<SigDataType>, Vec<FuncSign>>> =
            HashMap::new();
        for (func, sigs) in &FUNC_SIG_MAP.0 {
            for sig in sigs {
                // validate the FUNC_SIG_MAP is consistent
                assert_eq!(func, &sig.name);
                // exclude deprecated functions
                if sig.deprecated {
                    continue;
                }

                new_map
                    .entry(*func)
                    .or_default()
                    .entry(sig.inputs_type.to_vec())
                    .or_default()
                    .push(sig.clone());
            }
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
                "cast(timestamp) -> date/time/varchar",
                "cast(varchar) -> date/time/timestamp/jsonb/interval/rw_int256/real/double precision/numeric/smallint/integer/bigint/varchar/boolean/bytea/anyarray",
                "sum(bigint) -> numeric/bigint",
            ]
        "#]];
        expected.assert_debug_eq(&duplicated);
    }
}
