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

use std::collections::HashMap;

#[derive(Clone)]
pub struct StateTableColumnMapping {
    /// index: state table column index, value: upstream column index
    included_upstream_indices: Vec<usize>,
    /// key: upstream column index, value: state table value index
    mapping: HashMap<usize, usize>,
}

impl StateTableColumnMapping {
    /// Creates a new column mapping with the upstream columns included in state table.
    pub fn new(
        included_upstream_indices: Vec<usize>,
        table_value_indices: Option<Vec<usize>>,
    ) -> Self {
        let mapping = table_value_indices
            .unwrap_or_else(|| (0..included_upstream_indices.len()).collect())
            .into_iter()
            .map(|value_idx| included_upstream_indices[value_idx])
            .enumerate()
            .map(|(i, upstream_idx)| (upstream_idx, i))
            .collect();
        Self {
            included_upstream_indices,
            mapping,
        }
    }

    /// Convert upstream chunk column index to state table column index.
    pub fn upstream_to_state_table(&self, idx: usize) -> Option<usize> {
        self.mapping.get(&idx).copied()
    }

    /// Return slice of all upstream columns.
    pub fn upstream_columns(&self) -> &[usize] {
        &self.included_upstream_indices
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_mapping() {
        let mapping = StateTableColumnMapping::new(vec![2, 3, 0, 1], None);
        assert_eq!(mapping.upstream_to_state_table(2), Some(0));
        assert_eq!(mapping.upstream_to_state_table(3), Some(1));
        assert_eq!(mapping.upstream_to_state_table(0), Some(2));
        assert_eq!(mapping.upstream_to_state_table(1), Some(3));
        assert_eq!(mapping.upstream_to_state_table(4), None);
        assert_eq!(mapping.upstream_columns(), &[2, 3, 0, 1]);
    }

    #[test]
    fn test_column_mapping_with_value_indices() {
        let mapping = StateTableColumnMapping::new(vec![2, 3, 0, 1], Some(vec![0, 1, 3]));
        assert_eq!(mapping.upstream_to_state_table(2), Some(0));
        assert_eq!(mapping.upstream_to_state_table(3), Some(1));
        assert_eq!(mapping.upstream_to_state_table(0), None); // not in value indices
        assert_eq!(mapping.upstream_to_state_table(1), Some(2));
        assert_eq!(mapping.upstream_to_state_table(4), None);
        assert_eq!(mapping.upstream_columns(), &[2, 3, 0, 1]);
    }
}
