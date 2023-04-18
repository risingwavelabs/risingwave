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

use std::slice;

use risingwave_common::types::DataType;

/// An aggregation function may accept 0, 1 or 2 arguments.
#[derive(Clone, Debug)]
pub enum AggArgs {
    /// `None` is used for function calls that accept 0 argument, e.g. `count(*)`.
    None,
    /// `Unary` is used for function calls that accept 1 argument, e.g. `sum(x)`.
    Unary(DataType, usize),
    /// `Binary` is used for function calls that accept 2 arguments, e.g. `string_agg(x, delim)`.
    Binary([DataType; 2], [usize; 2]),
}

impl AggArgs {
    /// return the types of arguments.
    pub fn arg_types(&self) -> &[DataType] {
        use AggArgs::*;
        match self {
            None => Default::default(),
            Unary(typ, _) => slice::from_ref(typ),
            Binary(typs, _) => typs,
        }
    }

    /// return the indices of the arguments in [`risingwave_common::array::StreamChunk`].
    pub fn val_indices(&self) -> &[usize] {
        use AggArgs::*;
        match self {
            None => Default::default(),
            Unary(_, val_idx) => slice::from_ref(val_idx),
            Binary(_, val_indices) => val_indices,
        }
    }
}
