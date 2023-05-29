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

use itertools::Itertools;
use risingwave_common::types::DataTypeName;

pub mod agg;
pub mod cast;
pub mod func;
pub mod table_function;

/// Utility struct for debug printing of function signature.
pub(crate) struct FuncSigDebug<'a, T> {
    pub func: T,
    pub inputs_type: &'a [DataTypeName],
    pub ret_type: DataTypeName,
    pub set_returning: bool,
}

impl<'a, T: std::fmt::Display> std::fmt::Debug for FuncSigDebug<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = format!(
            "{}({:?}) -> {}{:?}",
            self.func,
            self.inputs_type.iter().format(","),
            if self.set_returning { "setof " } else { "" },
            self.ret_type
        )
        .to_ascii_lowercase();

        f.write_str(&s)
    }
}
