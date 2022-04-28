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

use super::{ColumnDesc, OrderedColumnDesc, TableId};

/// the table descriptor of table with cell based encoding in state store and include all
/// information for compute node to access data of the table.
#[derive(Debug, Clone, Default)]
pub struct TableDesc {
    /// Id of the table, to find in storage.
    pub table_id: TableId,
    /// The keys used to sort in storage.
    pub order_desc: Vec<OrderedColumnDesc>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,
    /// Distribution keys of this table, which corresponds to the corresponding column of the
    /// index. e.g., if `distribution_keys = [1, 2]`, then `columns[1]` and `columns[2]` are used
    /// as distribution key.
    pub distribution_keys: Vec<usize>,
    /// Column indices for primary keys.
    pub pks: Vec<usize>,
}
