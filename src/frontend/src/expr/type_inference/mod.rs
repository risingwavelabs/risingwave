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

//! This type inference is just to infer the return type of function calls, and make sure the
//! functionCall expressions have same input type requirement and return type definition as backend.

mod agg;
mod cast;
mod func;
pub use agg::{agg_func_sigs, AggFuncSig};
pub use cast::{
    align_types, cast_map_array, cast_ok, cast_ok_base, cast_sigs, least_restrictive, CastContext,
    CastSig,
};
pub use func::{func_sigs, infer_type, FuncSign};
