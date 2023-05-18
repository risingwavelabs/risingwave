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
use crate::expr::ExprImpl;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Insert<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub column_indices: Vec<usize>, // columns in which to insert
    pub default_columns: Vec<(usize, ExprImpl)>, // columns to be set to default
    pub row_id_index: Option<usize>,
    pub returning: bool,
}
