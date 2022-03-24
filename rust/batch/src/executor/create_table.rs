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

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::create_table_node::Info;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::ColumnDesc as ColumnDescProto;
use risingwave_source::SourceManagerRef;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

// TODO: All DDLs should be RPC requests from the meta service. Remove this.
pub struct CreateTableExecutor {
    table_id: TableId,
    source_manager: SourceManagerRef,
    table_columns: Vec<ColumnDescProto>,
    identity: String,

    /// Other info for creating table.
    info: Info,
}

impl CreateTableExecutor {
    pub fn new(
        table_id: TableId,
        source_manager: SourceManagerRef,
        table_columns: Vec<ColumnDescProto>,
        identity: String,
        info: Info,
    ) -> Self {
        Self {
            table_id,
            source_manager,
            table_columns,
            identity,
            info,
        }
    }
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::CreateTable
        )?;

        let table_id = TableId::from(&node.table_ref_id);

        Ok(Box::new(Self {
            table_id,
            source_manager: source.global_batch_env().source_manager_ref(),
            table_columns: node.column_descs.clone(),
            identity: "CreateTableExecutor".to_string(),
            info: node.info.clone().unwrap(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    async fn open(&mut self) -> Result<()> {
        let table_columns = self.table_columns.iter().cloned().map(Into::into).collect();

        match &self.info {
            Info::TableSource(_) => {
                // Create table_v2.
                info!("Create table id:{}", &self.table_id.table_id());

                self.source_manager
                    .create_table_source_v2(&self.table_id, table_columns)?;
            }
            Info::MaterializedView(info) => {
                if info.associated_table_ref_id.is_some() {
                    // Create associated materialized view for table_v2.
                    let associated_table_id = TableId::from(&info.associated_table_ref_id);
                    info!(
                        "create associated materialized view: id={:?}, associated={:?}",
                        self.table_id, associated_table_id
                    );
                    // TODO: there's nothing to do on compute node here for creating associated mv
                } else {
                    // Create normal MV.
                    info!("create materialized view: id={:?}", self.table_id);
                    // TODO: there's nothing to do on compute node here for creating mv
                }
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        panic!("create table executor does not have schema!");
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
