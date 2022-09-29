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
use risingwave_connector::source::ConnectorProperties;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::RowFormatType;
use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

use crate::monitor::SourceMetrics;
use crate::table::TableSource;
use crate::{ConnectorSource, SourceFormat, SourceImpl, SourceParserImpl};

pub type SourceRef = Arc<SourceImpl>;

/// The local source manager on the compute node.
#[async_trait]
pub trait SourceManager: Debug + Sync + Send {
    async fn create_source(&self, table_id: &TableId, info: StreamSourceInfo) -> Result<()>;
    fn create_table_source(
        &self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
        row_id_index: Option<usize>,
        pk_column_ids: Vec<i32>,
    ) -> Result<()>;

    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn drop_source(&self, source_id: &TableId) -> Result<()>;

    /// Clear sources, this is used when failover happens.
    fn clear_sources(&self) -> Result<()>;

    fn metrics(&self) -> Arc<SourceMetrics>;
    fn msg_buf_size(&self) -> usize;
}

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub fields: Vec<ColumnDesc>,
    /// Now `skip_parse` is used to indicate whether the column is a row id column.
    pub skip_parse: bool,
}

impl SourceColumnDesc {
    /// Create a [`SourceColumnDesc`] without composite types.
    #[track_caller]
    pub fn simple(name: impl Into<String>, data_type: DataType, column_id: ColumnId) -> Self {
        assert!(
            !matches!(data_type, DataType::List { .. } | DataType::Struct(..)),
            "called `SourceColumnDesc::simple` with a composite type."
        );
        Self {
            name: name.into(),
            data_type,
            column_id,
            fields: vec![],
            skip_parse: false,
        }
    }
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            fields: c.field_descs.clone(),
            skip_parse: false,
        }
    }
}

impl From<&SourceColumnDesc> for ColumnDesc {
    fn from(s: &SourceColumnDesc) -> Self {
        ColumnDesc {
            data_type: s.data_type.clone(),
            column_id: s.column_id,
            name: s.name.clone(),
            field_descs: s.fields.clone(),
            type_name: "".to_string(),
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

    // The column index of row ID. If the primary key is specified by users, this will be `None`.
    pub row_id_index: Option<usize>,
    pub pk_column_ids: Vec<i32>,
}

pub type SourceManagerRef = Arc<dyn SourceManager>;

#[derive(Debug)]
pub struct MemSourceManager {
    sources: Mutex<HashMap<TableId, SourceDesc>>,
    /// local source metrics
    metrics: Arc<SourceMetrics>,
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    connector_message_buffer_size: usize,
}

#[async_trait]
impl SourceManager for MemSourceManager {
    async fn create_source(&self, source_id: &TableId, info: StreamSourceInfo) -> Result<()> {
        let format = match info.get_row_format()? {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            RowFormatType::Avro => SourceFormat::Avro,
            RowFormatType::RowUnspecified => unreachable!(),
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

        let mut columns: Vec<_> = info
            .columns
            .into_iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.unwrap())))
            .collect();
        let row_id_index = info.row_id_index.map(|row_id_index| {
            columns[row_id_index.index as usize].skip_parse = true;
            row_id_index.index as usize
        });
        let pk_column_ids = info.pk_column_ids;
        assert!(
            !pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let config = ConnectorProperties::extract(info.properties)
            .map_err(|e| RwError::from(ConnectorError(e.into())))?;

        let source = SourceImpl::Connector(ConnectorSource {
            config,
            columns: columns.clone(),
            parser,
            connector_message_buffer_size: self.connector_message_buffer_size,
        });

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_index,
            pk_column_ids,
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

    fn create_table_source(
        &self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
        row_id_index: Option<usize>,
        pk_column_ids: Vec<i32>,
    ) -> Result<()> {
        let mut sources = self.get_sources()?;

        ensure!(
            !sources.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        assert!(
            !pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let source_columns = columns.iter().map(SourceColumnDesc::from).collect();
        let source = SourceImpl::Table(TableSource::new(columns));

        // Table sources do not need columns and format
        let desc = SourceDesc {
            source: Arc::new(source),
            columns: source_columns,
            format: SourceFormat::Invalid,
            row_id_index,
            pk_column_ids,
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

    fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    fn msg_buf_size(&self) -> usize {
        self.connector_message_buffer_size
    }
}

impl Default for MemSourceManager {
    fn default() -> Self {
        MemSourceManager {
            sources: Default::default(),
            metrics: Default::default(),
            connector_message_buffer_size: 16,
        }
    }
}

impl MemSourceManager {
    pub fn new(metrics: Arc<SourceMetrics>, connector_message_buffer_size: usize) -> Self {
        MemSourceManager {
            sources: Mutex::new(HashMap::new()),
            metrics,
            connector_message_buffer_size,
        }
    }

    fn get_sources(&self) -> Result<MutexGuard<'_, HashMap<TableId, SourceDesc>>> {
        Ok(self.sources.lock())
    }
}

pub struct SourceDescBuilder {
    id: TableId,
    info: ProstSourceInfo,
    mgr: SourceManagerRef,
}

impl SourceDescBuilder {
    pub fn new(id: TableId, info: &ProstSourceInfo, mgr: &SourceManagerRef) -> Self {
        Self {
            id,
            info: info.clone(),
            mgr: mgr.clone(),
        }
    }

    pub async fn build(&self) -> Result<SourceDesc> {
        let Self { id, info, mgr } = self;
        let source_desc = match &info {
            ProstSourceInfo::TableSource(_) => mgr.get_source(id).unwrap(),
            ProstSourceInfo::StreamSource(info) => {
                Self::build_stream_source(mgr, info.clone()).await?
            }
        };
        Ok(source_desc)
    }

    async fn build_stream_source(
        mgr: &SourceManagerRef,
        info: StreamSourceInfo,
    ) -> Result<SourceDesc> {
        let format = match info.get_row_format()? {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            RowFormatType::Avro => SourceFormat::Avro,
            RowFormatType::RowUnspecified => unreachable!(),
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

        let mut columns: Vec<_> = info
            .columns
            .into_iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.unwrap())))
            .collect();
        let row_id_index = info.row_id_index.map(|row_id_index| {
            columns[row_id_index.index as usize].skip_parse = true;
            row_id_index.index as usize
        });
        let pk_column_ids = info.pk_column_ids;
        assert!(
            !pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let config = ConnectorProperties::extract(info.properties)
            .map_err(|e| RwError::from(ConnectorError(e.into())))?;

        let source = SourceImpl::Connector(ConnectorSource {
            config,
            columns: columns.clone(),
            parser,
            connector_message_buffer_size: mgr.msg_buf_size(),
        });

        Ok(SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_index,
            pk_column_ids,
            metrics: mgr.metrics(),
        })
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::DataType;
    use risingwave_connector::source::kinesis::config::kinesis_demo_properties;
    use risingwave_pb::catalog::{ColumnIndex, StreamSourceInfo};
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
            row_id_index: Some(ColumnIndex { index: 0 }),
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
    async fn test_table_source() -> Result<()> {
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
        let row_id_index = None;
        let pk_column_ids = vec![1];

        let _keyspace = Keyspace::table_root(MemoryStateStore::new(), &table_id);

        let mem_source_manager = MemSourceManager::default();
        let res = mem_source_manager.create_table_source(
            &table_id,
            table_columns,
            row_id_index,
            pk_column_ids,
        );
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
