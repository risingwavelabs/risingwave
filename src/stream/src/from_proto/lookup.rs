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

use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::plan_common::{OrderType as ProstOrderType, StorageTableDesc};
use risingwave_pb::stream_plan::LookupNode;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
use crate::executor::{LookupExecutor, LookupExecutorParams};

pub struct LookupExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for LookupExecutorBuilder {
    type Node = LookupNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream_manager: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let lookup = node;

        let [stream, arrangement]: [_; 2] = params.input.try_into().unwrap();

        let arrangement_order_rules = lookup
            .get_arrangement_table_info()?
            .arrange_key_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();

        let arrangement_col_descs = lookup
            .get_arrangement_table_info()?
            .column_descs
            .iter()
            .map(ColumnDesc::from)
            .collect();

        let table_desc: &StorageTableDesc = lookup
            .get_arrangement_table_info()?
            .table_desc
            .as_ref()
            .unwrap();

        let table_id = TableId {
            table_id: table_desc.table_id,
        };

        let order_types = table_desc
            .pk
            .iter()
            .map(|desc| OrderType::from_prost(&ProstOrderType::from_i32(desc.order_type).unwrap()))
            .collect_vec();

        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let column_ids = column_descs.iter().map(|x| x.column_id).collect_vec();

        // Use indices based on full table instead of streaming executor output.
        let pk_indices = table_desc.pk.iter().map(|k| k.index as usize).collect_vec();

        let dist_key_indices = table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let distribution = match params.vnode_bitmap {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes: vnodes.into(),
            },
            None => Distribution::fallback(),
        };

        let table_option = TableOption {
            retention_seconds: if table_desc.retention_seconds > 0 {
                Some(table_desc.retention_seconds)
            } else {
                None
            },
        };
        let value_indices = table_desc
            .get_value_indices()
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let prefix_hint_len = table_desc.get_read_prefix_len_hint() as usize;
        let versioned = table_desc.versioned;

        let storage_table = StorageTable::new_partial(
            store,
            table_id,
            column_descs,
            column_ids,
            order_types,
            pk_indices,
            distribution,
            table_option,
            value_indices,
            prefix_hint_len,
            versioned,
        );

        Ok(Box::new(LookupExecutor::new(LookupExecutorParams {
            schema: params.schema,
            arrangement,
            stream,
            arrangement_col_descs,
            arrangement_order_rules,
            pk_indices: params.pk_indices,
            use_current_epoch: lookup.use_current_epoch,
            stream_join_key_indices: lookup.stream_key.iter().map(|x| *x as usize).collect(),
            arrange_join_key_indices: lookup.arrange_key.iter().map(|x| *x as usize).collect(),
            column_mapping: lookup.column_mapping.iter().map(|x| *x as usize).collect(),
            storage_table,
            watermark_epoch: stream_manager.get_watermark_epoch(),
            chunk_size: params.env.config().developer.stream_chunk_size,
        })))
    }
}
