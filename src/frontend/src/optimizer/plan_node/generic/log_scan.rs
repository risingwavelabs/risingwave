// Copyright 2024 RisingWave Labs
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
use std::rc::Rc;

use educe::Educe;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{Field, Schema, TableDesc};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_hummock_sdk::HummockVersionId;

use crate::catalog::ColumnId;
use crate::optimizer::optimizer_context::OptimizerContextRef;

const OP_NAME: &str = "op";
const OP_TYPE: DataType = DataType::Varchar;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct LogScan {
    pub table_name: String,
    /// Include `output_col_idx` and `op_column`
    pub output_col_idx: Vec<usize>,
    /// The field is in `output_col_idx`, but it's hidden, like `_row_id`
    pub hidden_col_idx: Vec<usize>,
    /// Descriptor of the table
    pub table_desc: Rc<TableDesc>,
    /// Help `RowSeqLogScan` executor use a better chunk size
    pub chunk_size: Option<u32>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    pub old_epoch: u64,
    pub new_epoch: u64,
    pub version_id: HummockVersionId,
}

impl LogScan {
    // Used for create batch exec, without op
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.table_desc.pk
    }

    fn column_names_with_table_prefix(&self) -> Vec<String> {
        let mut out_column_names: Vec<_> = self
            .output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.table_desc.columns[i].name))
            .collect();
        out_column_names.push(format!("{}.{}", self.table_name, OP_NAME));
        out_column_names
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        let mut out_column_names: Vec<_> = self
            .output_col_idx
            .iter()
            .map(|&i| self.table_desc.columns[i].name.clone())
            .collect();
        out_column_names.push(OP_NAME.to_string());
        out_column_names
    }

    pub(crate) fn column_names_without_hidden(&self) -> Vec<String> {
        let mut out_column_names: Vec<_> = self
            .output_col_idx
            .iter()
            .filter(|&i| !self.hidden_col_idx.contains(i))
            .map(|&i| self.table_desc.columns[i].name.clone())
            .collect();
        out_column_names.push(OP_NAME.to_string());
        out_column_names
    }

    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        let tb_idx_to_op_idx = self
            .output_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_desc
            .distribution_key
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx.get(&tb_idx).cloned())
            .collect()
    }

    /// Create a logical scan node for log table scan
    pub(crate) fn new(
        table_name: String,
        output_col_idx: Vec<usize>,
        hidden_col_idx: Vec<usize>,
        table_desc: Rc<TableDesc>,
        ctx: OptimizerContextRef,
        old_epoch: u64,
        new_epoch: u64,
        version_id: HummockVersionId,
    ) -> Self {
        Self {
            table_name,
            output_col_idx,
            hidden_col_idx,
            table_desc,
            chunk_size: None,
            ctx,
            old_epoch,
            new_epoch,
            version_id,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }

    pub(crate) fn schema(&self) -> Schema {
        let mut fields: Vec<_> = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.table_desc.columns[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        fields.push(Field::with_name(
            OP_TYPE,
            format!("{}.{}", &self.table_name, OP_NAME),
        ));
        Schema { fields }
    }

    pub(crate) fn out_fields(&self) -> FixedBitSet {
        let mut out_fields_vec = self
            .output_col_idx
            .iter()
            .enumerate()
            .filter_map(|(index, idx)| {
                if self.hidden_col_idx.contains(idx) {
                    None
                } else {
                    Some(index)
                }
            })
            .collect_vec();
        // add op column
        out_fields_vec.push(self.output_col_idx.len());
        FixedBitSet::from_iter(out_fields_vec)
    }

    pub(crate) fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}
