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

use std::collections::HashMap;

pub struct StateTableColumnMapping {
    /// index: state table column index, value: upstream column index
    upstream_columns: Vec<usize>,
    /// key: upstream column index, value: state table column index
    mapping: HashMap<usize, usize>,
}

impl StateTableColumnMapping {
    /// Creates a new column mapping with the upstream columns included in state table.
    pub fn new(upstream_columns: Vec<usize>) -> Self {
        let mapping = upstream_columns
            .iter()
            .enumerate()
            .map(|(i, col_idx)| (*col_idx, i))
            .collect();
        Self {
            upstream_columns,
            mapping,
        }
    }

    /// Convert upstream chunk column index to state table column index.
    pub fn upstream_to_state_table(&self, idx: usize) -> Option<usize> {
        self.mapping.get(&idx).copied()
    }

    /// Return the number of columns in the mapping.
    pub fn len(&self) -> usize {
        self.upstream_columns.len()
    }

    /// Return slice of all upstream columns.
    pub fn upstream_columns(&self) -> &[usize] {
        &self.upstream_columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_mapping() {
        let mapping = StateTableColumnMapping::new(vec![2, 3, 0, 1]);
        assert_eq!(mapping.upstream_to_state_table(2), Some(0));
        assert_eq!(mapping.upstream_to_state_table(3), Some(1));
        assert_eq!(mapping.upstream_to_state_table(0), Some(2));
        assert_eq!(mapping.upstream_to_state_table(1), Some(3));
        assert_eq!(mapping.upstream_to_state_table(4), None);
        assert_eq!(mapping.len(), 4);
        assert_eq!(mapping.upstream_columns(), &[2, 3, 0, 1]);
    }
}
