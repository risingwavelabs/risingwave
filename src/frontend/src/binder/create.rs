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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::error::Result;

use crate::Binder;

impl Binder {
    pub fn bind_columns_to_context(
        &mut self,
        name: String,
        column_catalogs: Vec<ColumnCatalog>,
    ) -> Result<()> {
        let columns = column_catalogs
            .iter()
            .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
            .collect_vec();
        self.bind_table_to_context(columns, name, None)
    }

    pub fn get_column_binding_index(
        &mut self,
        table_name: String,
        column_name: &String,
    ) -> Result<usize> {
        Ok(self
            .context
            .get_column_binding_index(&Some(table_name), column_name)?)
    }
}
