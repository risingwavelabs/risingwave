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

use itertools::Itertools;

use crate::array::Row;

#[derive(Clone, Debug)]
pub struct CompactedRow {
    pub row: Vec<u8>,
}

impl CompactedRow {
    pub fn from_row(row: &Row) -> Self {
        let value_indices = (0..row.0.len()).collect_vec();
        Self {
            row: row.serialize(&value_indices),
        }
    }
}
