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
use std::fmt;
use std::hash::Hash;

use educe::Educe;
use risingwave_common::catalog::{Schema, TableVersionId};

use super::GenericPlanRef;
use crate::catalog::TableId;
use crate::OptimizerContextRef;
use crate::expr::ExprImpl;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Insert<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    table_name: String, // explain-only
    table_id: TableId,
    table_version_id: TableVersionId,
    input: PlanRef,
    column_indices: Vec<usize>,              // columns in which to insert
    default_columns: Vec<(usize, ExprImpl)>, // columns to be set to default
    row_id_index: Option<usize>,
    returning: bool,
}
