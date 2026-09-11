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

use std::collections::{BTreeMap, HashMap};

use risingwave_pb::plan_common::cdc_key_ordering::{Column as PbCdcKeyColumn, Comparison};
use risingwave_pb::plan_common::{CdcKeyOrdering as PbCdcKeyOrdering, ExternalTableDesc};
use risingwave_pb::secret::PbSecretRef;

use super::{ColumnDesc, ColumnId, TableId};
use crate::id::SourceId;
use crate::util::iter_util::ZipEqFast;
use crate::util::sort_util::ColumnOrder;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum CdcKeyComparison {
    #[default]
    Native,
    UnsignedInt64,
}

impl CdcKeyComparison {
    pub fn from_protobuf(comparison: Comparison) -> Self {
        match comparison {
            Comparison::Unspecified => Self::Native,
            Comparison::Native => Self::Native,
            Comparison::UnsignedInt64 => Self::UnsignedInt64,
        }
    }

    fn to_protobuf(self) -> Comparison {
        match self {
            Self::Native => Comparison::Native,
            Self::UnsignedInt64 => Comparison::UnsignedInt64,
        }
    }
}

/// Necessary information for compute node to access data in the external database.
/// Compute node will use this information to connect to the external database and scan the table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CdcTableDesc {
    /// Id of the table in RW
    pub table_id: TableId,

    /// Id of the upstream source in sharing cdc mode
    pub source_id: SourceId,

    /// The full name of the table in external database, e.g. `database_name.table_name` in MySQL
    /// and `schema_name.table_name` in the Postgres.
    pub external_table_name: String,
    /// The key used to sort in storage.
    pub pk: Vec<ColumnOrder>,
    /// Comparison semantics for each primary-key column.
    pub pk_comparisons: Vec<CdcKeyComparison>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,

    /// Column indices for primary keys.
    pub stream_key: Vec<usize>,

    /// properties will be passed into the `StreamScanNode`
    pub connect_properties: BTreeMap<String, String>,
    /// Secret refs
    pub secret_refs: BTreeMap<String, PbSecretRef>,
}

impl CdcTableDesc {
    pub fn to_protobuf(&self) -> ExternalTableDesc {
        assert_eq!(self.pk.len(), self.pk_comparisons.len());
        ExternalTableDesc {
            table_id: self.table_id,
            source_id: self.source_id,
            columns: self.columns.iter().map(Into::into).collect(),
            pk: self.pk.iter().map(|column| column.to_protobuf()).collect(),
            pk_ordering: Some(PbCdcKeyOrdering {
                columns: self
                    .pk
                    .iter()
                    .zip_eq_fast(&self.pk_comparisons)
                    .map(|(column_order, comparison)| PbCdcKeyColumn {
                        pk_col_idx: column_order.column_index as _,
                        comparison: comparison.to_protobuf() as _,
                    })
                    .collect(),
            }),
            table_name: self.external_table_name.clone(),
            stream_key: self.stream_key.iter().map(|k| *k as _).collect(),
            connect_properties: self.connect_properties.clone(),
            secret_refs: self.secret_refs.clone(),
        }
    }

    /// Helper function to create a mapping from `column id` to `column index`
    pub fn get_id_to_op_idx_mapping(&self) -> HashMap<ColumnId, usize> {
        ColumnDesc::get_id_to_op_idx_mapping(self.columns.as_slice(), None)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan_common::cdc_key_ordering::Comparison;

    use super::*;
    use crate::util::sort_util::OrderType;

    #[test]
    fn test_cdc_key_comparisons_are_persisted_with_pk_indices() {
        let table_desc = CdcTableDesc {
            table_id: TableId::new(1),
            source_id: SourceId::new(2),
            external_table_name: "orders".to_owned(),
            pk: vec![
                ColumnOrder::new(3, OrderType::ascending()),
                ColumnOrder::new(1, OrderType::ascending()),
            ],
            pk_comparisons: vec![CdcKeyComparison::UnsignedInt64, CdcKeyComparison::Native],
            columns: vec![],
            stream_key: vec![3, 1],
            connect_properties: BTreeMap::new(),
            secret_refs: BTreeMap::new(),
        };

        let protobuf = table_desc.to_protobuf();
        assert_eq!(protobuf.pk.len(), 2);
        assert_eq!(protobuf.pk[0].column_index, 3);
        assert_eq!(protobuf.pk[1].column_index, 1);

        let pk_columns = protobuf.pk_ordering.unwrap().columns;
        assert_eq!(pk_columns.len(), 2);
        assert_eq!(pk_columns[0].pk_col_idx, 3);
        assert_eq!(
            pk_columns[0].get_comparison().unwrap(),
            Comparison::UnsignedInt64
        );
        assert_eq!(pk_columns[1].pk_col_idx, 1);
        assert_eq!(pk_columns[1].get_comparison().unwrap(), Comparison::Native);
    }
}
