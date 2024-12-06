// Copyright 2024 RisingWave Labs
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

//! This type inference is just to infer the return type of function calls, and make sure the
//! functionCall expressions have same input type requirement and return type definition as backend.

mod cast;
mod func;
pub use cast::{
    align_types, bail_cast_error, cast, cast_error, cast_ok, cast_ok_base, cast_sigs, CastContext,
    CastError, CastErrorInner, CastSig, CAST_TABLE,
};
pub use func::{infer_some_all, infer_type, infer_type_name, infer_type_with_sigmap, FuncSign};
