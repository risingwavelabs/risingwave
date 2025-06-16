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

//! Metadata of expressions.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
use risingwave_pb::expr::expr_node::PbType as ScalarFunctionType;
use risingwave_pb::expr::table_function::PbType as TableFunctionType;

use crate::ExprError;
use crate::aggregate::{AggCall, BoxedAggregateFunction};
use crate::error::Result;
use crate::expr::BoxedExpression;
use crate::table_function::BoxedTableFunction;

mod udf;

pub use self::udf::*;

/// The global registry of all function signatures.
pub static FUNCTION_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(|| {
    let mut map = FunctionRegistry::default();
    tracing::info!("found {} functions", FUNCTIONS.len());
    for f in FUNCTIONS {
        map.insert(f());
    }
    map
});

/// A set of function signatures.
#[derive(Default, Clone, Debug)]
pub struct FunctionRegistry(HashMap<FuncName, Vec<FuncSign>>);

impl FunctionRegistry {
    /// Inserts a function signature.
    pub fn insert(&mut self, sig: FuncSign) {
        let list = self.0.entry(sig.name.clone()).or_default();
        if sig.is_aggregate() {
            // merge retractable and append-only aggregate
            if let Some(existing) = list
                .iter_mut()
                .find(|d| d.inputs_type == sig.inputs_type && d.ret_type == sig.ret_type)
            {
                let (
                    FuncBuilder::Aggregate {
                        retractable,
                        append_only,
                        retractable_state_type,
                        append_only_state_type,
                    },
                    FuncBuilder::Aggregate {
                        retractable: r1,
                        append_only: a1,
                        retractable_state_type: rs1,
                        append_only_state_type: as1,
                    },
                ) = (&mut existing.build, sig.build)
                else {
                    panic!("expected aggregate function")
                };
                if let Some(f) = r1 {
                    *retractable = Some(f);
                    *retractable_state_type = rs1;
                }
                if let Some(f) = a1 {
                    *append_only = Some(f);
                    *append_only_state_type = as1;
                }
                return;
            }
        }
        list.push(sig);
    }

    /// Remove a function signature from registry.
    pub fn remove(&mut self, sig: FuncSign) -> Option<FuncSign> {
        let pos = self
            .0
            .get_mut(&sig.name)?
            .iter()
            .positions(|s| s.inputs_type == sig.inputs_type && s.ret_type == sig.ret_type)
            .rev()
            .collect_vec();
        let mut ret = None;
        for p in pos {
            ret = Some(self.0.get_mut(&sig.name)?.swap_remove(p));
        }
        ret
    }

    /// Returns a function signature with the same type, argument types and return type.
    /// Deprecated functions are included.
    pub fn get(
        &self,
        name: impl Into<FuncName>,
        args: &[DataType],
        ret: &DataType,
    ) -> Result<&FuncSign, ExprError> {
        let name = name.into();
        let err = |candidates: &Vec<FuncSign>| {
            // Note: if we return error here, it probably means there is a bug in frontend type inference,
            // because such error should be caught in the frontend.
            ExprError::UnsupportedFunction(format!(
                "{}({}) -> {}{}",
                name,
                args.iter().format(", "),
                ret,
                if candidates.is_empty() {
                    "".to_owned()
                } else {
                    format!(
                        "\nHINT: Supported functions:\n{}",
                        candidates
                            .iter()
                            .map(|d| format!(
                                "  {}({}) -> {}",
                                d.name,
                                d.inputs_type.iter().format(", "),
                                d.ret_type
                            ))
                            .format("\n")
                    )
                }
            ))
        };
        let v = self.0.get(&name).ok_or_else(|| err(&vec![]))?;
        v.iter()
            .find(|d| d.match_args_ret(args, ret))
            .ok_or_else(|| err(v))
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

    /// Returns the return type for the given function and arguments.
    /// Deprecated functions are excluded.
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
            .find(|d| d.match_args(args) && !d.deprecated)
            .ok_or_else(|| ExprError::UnsupportedFunction(name.to_string()))?;
        (sig.type_infer)(args)
    }

    /// Returns an iterator of all function signatures.
    pub fn iter(&self) -> impl Iterator<Item = &FuncSign> {
        self.0.values().flatten()
    }

    /// Returns an iterator of all scalar functions.
    pub fn iter_scalars(&self) -> impl Iterator<Item = &FuncSign> {
        self.iter().filter(|d| d.is_scalar())
    }

    /// Returns an iterator of all aggregate functions.
    pub fn iter_aggregates(&self) -> impl Iterator<Item = &FuncSign> {
        self.iter().filter(|d| d.is_aggregate())
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
        // allow `zip` as the length of `args` may be larger than `inputs_type`
        #[allow(clippy::disallowed_methods)]
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

    /// Returns true if the aggregate function is append-only.
    pub const fn is_append_only(&self) -> bool {
        matches!(
            self.build,
            FuncBuilder::Aggregate {
                retractable: None,
                ..
            }
        )
    }

    /// Returns true if the aggregate function has a retractable version.
    pub const fn is_retractable(&self) -> bool {
        matches!(
            self.build,
            FuncBuilder::Aggregate {
                retractable: Some(_),
                ..
            }
        )
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

    /// Builds the aggregate function. If both retractable and append-only versions exist, the
    /// retractable version will be built.
    pub fn build_aggregate(&self, agg: &AggCall) -> Result<BoxedAggregateFunction> {
        match self.build {
            FuncBuilder::Aggregate {
                retractable,
                append_only,
                ..
            } => retractable.or(append_only).unwrap()(agg),
            _ => panic!("Expected an aggregate function"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FuncName {
    Scalar(ScalarFunctionType),
    Table(TableFunctionType),
    Aggregate(PbAggKind),
    Udf(String),
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

impl From<PbAggKind> for FuncName {
    fn from(ty: PbAggKind) -> Self {
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
    pub fn as_str_name(&self) -> Cow<'static, str> {
        match self {
            Self::Scalar(ty) => ty.as_str_name().into(),
            Self::Table(ty) => ty.as_str_name().into(),
            Self::Aggregate(ty) => ty.as_str_name().into(),
            Self::Udf(name) => name.clone().into(),
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

    pub fn as_aggregate(&self) -> PbAggKind {
        match self {
            Self::Aggregate(kind) => *kind,
            _ => panic!("Expected an aggregate function"),
        }
    }
}

/// An extended data type that can be used to declare a function's argument or result type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SigDataType {
    /// Exact data type
    Exact(DataType),
    /// Accepts any data type
    Any,
    /// Accepts any array data type
    AnyArray,
    /// Accepts any struct data type
    AnyStruct,
    /// TODO: not all type can be used as a map key.
    AnyMap,
    /// Vector of a certain size
    /// Named without `Any` prefix to align with PostgreSQL
    Vector,
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
            Self::AnyMap => write!(f, "anymap"),
            Self::Vector => write!(f, "vector"),
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
            Self::AnyMap => dt.is_map(),
            Self::Vector => matches!(dt, DataType::Vector(_)),
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

#[derive(Clone)]
pub enum FuncBuilder {
    Scalar(fn(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression>),
    Table(
        fn(
            return_type: DataType,
            chunk_size: usize,
            children: Vec<BoxedExpression>,
        ) -> Result<BoxedTableFunction>,
    ),
    // An aggregate function may contain both or either one of retractable and append-only versions.
    Aggregate {
        retractable: Option<fn(agg: &AggCall) -> Result<BoxedAggregateFunction>>,
        append_only: Option<fn(agg: &AggCall) -> Result<BoxedAggregateFunction>>,
        /// The state type of the retractable aggregate function.
        /// `None` means equal to the return type.
        retractable_state_type: Option<DataType>,
        /// The state type of the append-only aggregate function.
        /// `None` means equal to the return type.
        append_only_state_type: Option<DataType>,
    },
    Udf,
}

/// A static distributed slice of functions defined by `#[function]`.
#[linkme::distributed_slice]
pub static FUNCTIONS: [fn() -> FuncSign];
