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

//! Function signatures.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::LazyLock;

use risingwave_common::types::DataTypeName;
use risingwave_pb::expr::expr_node::Type as ExprType;
use risingwave_pb::expr::ExprNode;

use crate::error::Result;
use crate::expr::BoxedExpression;

pub static FUNC_SIG_MAP: LazyLock<FuncSigMap> = LazyLock::new(|| unsafe {
    let mut map = FuncSigMap::default();
    for desc in FUNC_SIG_MAP_INIT.drain(..) {
        map.insert(desc);
    }
    map
});

/// The table of function signatures.
pub fn func_sigs() -> impl Iterator<Item = &'static FunctionDescriptor> {
    FUNC_SIG_MAP.0.values().flatten()
}

#[derive(Default, Clone)]
pub struct FuncSigMap(HashMap<(ExprType, usize), Vec<FunctionDescriptor>>);

impl Deref for FuncSigMap {
    type Target = HashMap<(ExprType, usize), Vec<FunctionDescriptor>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FuncSigMap {
    pub fn insert(&mut self, desc: FunctionDescriptor) {
        self.0
            .entry((desc.ty, desc.args.len()))
            .or_default()
            .push(desc)
    }

    pub fn get(&self, ty: ExprType, args: &[DataTypeName]) -> Option<&FunctionDescriptor> {
        let v = self.0.get(&(ty, args.len()))?;
        v.iter().find(|d| d.args == args)
    }
}

/// A function signature.
#[derive(Clone)]
pub struct FunctionDescriptor {
    pub name: &'static str,
    pub ty: ExprType,
    pub args: &'static [DataTypeName],
    pub ret: DataTypeName,
    pub build_from_prost: fn(prost: &ExprNode) -> Result<BoxedExpression>,
}

impl FunctionDescriptor {
    /// Returns a string describing the function without return type.
    pub fn to_string_no_return(&self) -> String {
        format!(
            "{}({})",
            self.ty.as_str_name(),
            self.args
                .iter()
                .map(|t| format!("{t:?}"))
                .collect::<Vec<_>>()
                .join(",")
        )
        .to_lowercase()
    }
}

/// Register a function into global registry.
pub fn register(desc: FunctionDescriptor) {
    unsafe { FUNC_SIG_MAP_INIT.push(desc) }
}

static mut FUNC_SIG_MAP_INIT: Vec<FunctionDescriptor> = Vec::new();
