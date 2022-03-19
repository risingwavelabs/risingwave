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
//
use fixedbitset::FixedBitSet;
use paste::paste;
use risingwave_common::catalog::Schema;

use super::super::plan_node::*;
use crate::for_logical_plan_nodes;

pub trait WithSchema {
    fn schema(&self) -> &Schema;

    fn must_contain_columns(&self, required_cols: &FixedBitSet) {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );
    }
}

/// Define module for each node.
macro_rules! impl_with_schema_for_logical_node {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithSchema for [<$convention $name>] {
                fn schema(&self) -> &Schema {
                    &self.base.schema
                }
            }
        })*
    }
}
for_logical_plan_nodes! {impl_with_schema_for_logical_node }
