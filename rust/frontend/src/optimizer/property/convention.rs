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

use paste::paste;

use super::super::plan_node::*;
use crate::for_all_plan_nodes;

#[derive(Debug, PartialEq)]
pub enum Convention {
    Logical,
    Batch,
    Stream,
}

pub trait WithConvention {
    fn convention(&self) -> Convention;
}

/// Define module for each node.
macro_rules! impl_convention_for_plan_node {
    ([], $({ $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithConvention for [<$convention $name>] {
                fn convention(&self) -> Convention {
                    Convention::$convention
                }
            }
        })*
    }
}
for_all_plan_nodes! { impl_convention_for_plan_node }
