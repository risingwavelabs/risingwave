// Copyright 2026 RisingWave Labs
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

//! Integration of RisingWave scalar functions with DataFusion.
//!
//! This module provides the bridge between RisingWave's scalar function system and DataFusion's
//! expression evaluation engine. It enables RisingWave scalar functions to be executed within
//! DataFusion query plans.
//!
//! ## Overview
//!
//! The main entry point is [`convert_function_call`], which converts a RisingWave [`FunctionCall`]
//! into a DataFusion expression ([`DFExpr`]). The module uses rule-like abstractions to determine
//! whether a RisingWave expression can be directly converted to a native DataFusion expression,
//! or if it requires wrapping with [`ScalarUDFImpl`].
//!
//! If an expression matches one of the conversion rules (unary operations, binary operations,
//! case expressions, or cast expressions), it is converted directly to the corresponding DataFusion
//! expression. Otherwise, the expression falls back to being wrapped in [`RwScalarFunction`],
//! which uses RisingWave's native scalar function execution engine within DataFusion's query plan.
//!
//! ## RisingWave Scalar Function Wrapper
//!
//! For functions that DataFusion cannot handle directly, [`RwScalarFunction`] implements
//! [`ScalarUDFImpl`] to wrap RisingWave's expression evaluation logic. This allows seamless
//! execution of RisingWave functions within DataFusion's query execution engine.
//!
//! The wrapper handles:
//! - Type casting from DataFusion types to RisingWave types
//! - Data chunk creation and manipulation
//! - Async expression evaluation using RisingWave's executor
//! - Result conversion back to DataFusion-compatible types

mod convert;
mod data_type_ext;
mod rw_scalar_function;

pub use convert::convert_function_call;
pub use data_type_ext::RwDataTypeDataFusionExt;
pub use rw_scalar_function::RwScalarFunction;
