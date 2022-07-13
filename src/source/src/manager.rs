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
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::{ConnectorError, InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::UNIX_SINGULARITY_DATE_EPOCH;
use risingwave_connector::source::ConnectorProperties;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::RowFormatType;

use crate::monitor::SourceMetrics;
use crate::row_id::{RowId, RowIdGenerator};
use crate::table_v2::TableSourceV2;
use crate::{ConnectorSource, SourceFormat, SourceImpl, SourceParserImpl};

pub type SourceRef = Arc<SourceImpl>;

/// The local source manager on the compute node.
#[async_trait]
pub trait SourceManager: Debug + Sync + Send {
    async fn create_source(&self, table_id: &TableId, info: StreamSourceInfo) -> Result<()>;
    fn create_table_source(&self, table_id: &TableId, columns: Vec<ColumnDesc>) -> Result<()>;

    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn drop_source(&self, source_id: &TableId) -> Result<()>;

    /// Clear sources, this is used when failover happens.
    fn clear_sources(&self) -> Result<()>;
}

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub skip_parse: bool,
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            skip_parse: false,
        }
    }
}

/// `SourceDesc` is used to describe a `Source`
#[derive(Clone, Debug)]
pub struct SourceDesc {
    pub source: SourceRef,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,

    // The column index of row ID. By default it's 0, which means the first column is row ID.
    // TODO: change to Option<usize> when pk supported in the future.
    pub row_id_index: usize,
    pub row_id_generator: Arc<Mutex<RowIdGenerator>>,
}

impl SourceDesc {
    pub fn next_row_id(&self) -> RowId {
        self.row_id_generator.as_ref().lock().next()
    }

    pub fn next_row_id_batch(&self, length: usize) -> Vec<RowId> {
        let mut guard = self.row_id_generator.as_ref().lock();
        guard.next_batch(length)
    }
}

pub type SourceManagerRef = Arc<dyn SourceManager>;

#[derive(Debug, Default)]
pub struct MemSourceManager {
    sources: Mutex<HashMap<TableId, SourceDesc>>,
    /// Located worker id.
    worker_id: u32,
    /// local source metrics
    metrics: Arc<SourceMetrics>,
}

#[async_trait]
impl SourceManager for MemSourceManager {
    async fn create_source(&self, source_id: &TableId, info: StreamSourceInfo) -> Result<()> {
        let format = match info.get_row_format()? {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            RowFormatType::Avro => SourceFormat::Avro,
        };

        if format == SourceFormat::Protobuf && info.row_schema_location.is_empty() {
            return Err(RwError::from(ProtocolError(
                "protobuf file location not provided".to_string(),
            )));
        }
        let source_parser_rs =
            SourceParserImpl::create(&format, &info.properties, info.row_schema_location.as_str())
                .await;
        let parser = if let Ok(source_parser) = source_parser_rs {
            source_parser
        } else {
            return Err(source_parser_rs.err().unwrap());
        };

        let columns = info
            .columns
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let c = c.column_desc.as_ref().unwrap().clone();
                SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: DataType::from(&c.column_type.unwrap()),
                    column_id: ColumnId::from(c.column_id),
                    skip_parse: idx as i32 == info.row_id_index,
                }
            })
            .collect::<Vec<SourceColumnDesc>>();

        assert!(
            info.row_id_index >= 0,
            "expected row_id_index >= 0, got {}",
            info.row_id_index
        );
        let row_id_index = info.row_id_index as usize;

        let config = ConnectorProperties::extract(info.properties)
            .map_err(|e| RwError::from(ConnectorError(e.to_string())))?;

        let source = SourceImpl::Connector(ConnectorSource {
            config,
            columns: columns.clone(),
            parser,
        });

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_index,
            row_id_generator: Arc::new(Mutex::new(RowIdGenerator::with_epoch(
                self.worker_id,
                *UNIX_SINGULARITY_DATE_EPOCH,
            ))),
            metrics: self.metrics.clone(),
        };

        let mut tables = self.get_sources()?;
        ensure!(
            !tables.contains_key(source_id),
            "Source id already exists: {:?}",
            source_id
        );
        tables.insert(*source_id, desc);

        Ok(())
    }

    fn create_table_source(&self, table_id: &TableId, columns: Vec<ColumnDesc>) -> Result<()> {
        let mut sources = self.get_sources()?;

        ensure!(
            !sources.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        let source_columns = columns.iter().map(SourceColumnDesc::from).collect();
        let source = SourceImpl::TableV2(TableSourceV2::new(columns));

        // Table sources do not need columns and format
        let desc = SourceDesc {
            source: Arc::new(source),
            columns: source_columns,
            format: SourceFormat::Invalid,
            row_id_index: 0, // always use the first column as row_id
            row_id_generator: Arc::new(Mutex::new(RowIdGenerator::with_epoch(
                self.worker_id,
                *UNIX_SINGULARITY_DATE_EPOCH,
            ))),
            metrics: self.metrics.clone(),
        };

        sources.insert(*table_id, desc);
        Ok(())
    }

    fn get_source(&self, table_id: &TableId) -> Result<SourceDesc> {
        let sources = self.get_sources()?;
        sources.get(table_id).cloned().ok_or_else(|| {
            InternalError(format!("Get source table id not exists: {:?}", table_id)).into()
        })
    }

    fn drop_source(&self, table_id: &TableId) -> Result<()> {
        let mut sources = self.get_sources()?;
        ensure!(
            sources.contains_key(table_id),
            "Source does not exist: {:?}",
            table_id
        );
        sources.remove(table_id);
        Ok(())
    }

    fn clear_sources(&self) -> Result<()> {
        let mut sources = self.get_sources()?;
        sources.clear();
        Ok(())
    }
}

impl MemSourceManager {
    pub fn new(worker_id: u32, metrics: Arc<SourceMetrics>) -> Self {
        MemSourceManager {
            sources: Mutex::new(HashMap::new()),
            worker_id,
            metrics,
        }
    }

    fn get_sources(&self) -> Result<MutexGuard<HashMap<TableId, SourceDesc>>> {
        Ok(self.sources.lock())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::DataType;
    use risingwave_connector::source::kinesis::config::kinesis_demo_properties;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::ColumnCatalog;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use crate::*;

    #[tokio::test]
    #[ignore] // ignored because the test involves aws credentials, remove this line after changing to other
              // connector
    async fn test_source_v2() -> Result<()> {
        let properties = kinesis_demo_properties();
        let source_columns =
            vec![ColumnDesc::unnamed(ColumnId::from(0), DataType::Int64).to_protobuf()];
        let columns = source_columns
            .iter()
            .map(|c| ColumnCatalog {
                column_desc: Some(c.to_owned()),
                is_hidden: false,
            })
            .collect();
        let info = StreamSourceInfo {
            properties,
            row_format: 0,
            row_schema_location: "".to_string(),
            row_id_index: 0,
            pk_column_ids: vec![0],
            columns,
        };
        let source_id = TableId::default();

        let mem_source_manager = MemSourceManager::default();
        let source = mem_source_manager.create_source(&source_id, info).await;

        assert!(source.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_table_source_v2() -> Result<()> {
        let table_id = TableId::default();

        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Decimal),
                Field::unnamed(DataType::Decimal),
            ],
        };

        let table_columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc {
                data_type: f.data_type.clone(),
                column_id: ColumnId::from(i as i32), // use column index as column id
                name: f.name.clone(),
                field_descs: vec![],
                type_name: "".to_string(),
            })
            .collect();

        let _keyspace = Keyspace::table_root(MemoryStateStore::new(), &table_id);

        let mem_source_manager = MemSourceManager::default();
        let res = mem_source_manager.create_table_source(&table_id, table_columns);
        assert!(res.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_ok());

        // drop source
        let drop_source_res = mem_source_manager.drop_source(&table_id);
        assert!(drop_source_res.is_ok());
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_err());

        Ok(())
    }
}
