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
use parking_lot::Mutex;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::ErrorCode::{ConnectorError, InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::source::ConnectorProperties;
use risingwave_pb::catalog::{StreamSourceInfo, TableSourceInfo};
use risingwave_pb::plan_common::RowFormatType;
use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

use crate::monitor::SourceMetrics;
use crate::table::TableSource;
use crate::{ConnectorSource, SourceFormat, SourceImpl, SourceParserImpl};

pub type SourceRef = Arc<SourceImpl>;

/// The local source manager on the compute node.
#[async_trait]
pub trait SourceManager: Debug + Sync + Send {
    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn insert_table_source(&self, table_id: &TableId, info: &TableSourceInfo)
        -> Result<SourceDesc>;
    fn try_drop_source(&self, source_id: &TableId);

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

#[derive(Debug, Default)]
struct MemSourceManagerInner {
    sources: HashMap<TableId, SourceDesc>,
    source_actor_num: HashMap<TableId, usize>,
}

#[derive(Debug)]
pub struct MemSourceManager {
    inner: Mutex<MemSourceManagerInner>,
    /// local source metrics
    metrics: Arc<SourceMetrics>,
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    connector_message_buffer_size: usize,
}

#[async_trait]
impl SourceManager for MemSourceManager {
    fn get_source(&self, table_id: &TableId) -> Result<SourceDesc> {
        let inner = self.inner.lock();
        inner.sources.get(table_id).cloned().ok_or_else(|| {
            InternalError(format!("Get source table id not exists: {:?}", table_id)).into()
        })
    }

    fn insert_table_source(
        &self,
        table_id: &TableId,
        info: &TableSourceInfo,
    ) -> Result<SourceDesc> {
        let mut inner = self.inner.lock();
        let actor_num = inner.source_actor_num.entry(*table_id).or_insert(0usize);
        *actor_num += 1;
        let desc = inner.sources.entry(*table_id).or_insert_with(|| {
            let columns: Vec<_> = info
                .columns
                .iter()
                .cloned()
                .map(|c| ColumnDesc::from(c.column_desc.unwrap()))
                .collect();
            let row_id_index = info.row_id_index.as_ref().map(|index| index.index as _);
            let pk_column_ids = info.pk_column_ids.clone();

            // Table sources do not need columns and format
            SourceDesc {
                columns: columns.iter().map(SourceColumnDesc::from).collect(),
                source: Arc::new(SourceImpl::Table(TableSource::new(columns))),
                format: SourceFormat::Invalid,
                row_id_index,
                pk_column_ids,
                metrics: self.metrics.clone(),
            }
        });
        Ok(desc.clone())
    }

    fn try_drop_source(&self, source_id: &TableId) {
        let mut inner = self.inner.lock();
        let actor_num = inner
            .source_actor_num
            .get_mut(source_id)
            .expect("double release in local source manager!");
        *actor_num -= 1;
        if *actor_num == 0 {
            inner.sources.remove(source_id);
        }
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
            inner: Default::default(),
            metrics: Default::default(),
            connector_message_buffer_size: 16,
        }
    }
}

impl MemSourceManager {
    pub fn new(metrics: Arc<SourceMetrics>, connector_message_buffer_size: usize) -> Self {
        MemSourceManager {
            inner: Mutex::new(MemSourceManagerInner::default()),
            metrics,
            connector_message_buffer_size,
        }
    }
}

#[derive(Clone)]
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
        match &info {
            ProstSourceInfo::TableSource(info) => Self::build_table_source(mgr, id, info),
            ProstSourceInfo::StreamSource(info) => Self::build_stream_source(mgr, info).await,
        }
    }

    fn build_table_source(
        mgr: &SourceManagerRef,
        table_id: &TableId,
        info: &TableSourceInfo,
    ) -> Result<SourceDesc> {
        mgr.insert_table_source(table_id, info)
    }

    async fn build_stream_source(
        mgr: &SourceManagerRef,
        info: &StreamSourceInfo,
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
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        let row_id_index = info.row_id_index.as_ref().map(|row_id_index| {
            columns[row_id_index.index as usize].skip_parse = true;
            row_id_index.index as usize
        });
        let pk_column_ids = info.pk_column_ids.clone();
        assert!(
            !pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let config = ConnectorProperties::extract(info.properties.clone())
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
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::DataType;
    use risingwave_connector::source::kinesis::config::kinesis_demo_properties;
    use risingwave_pb::catalog::{ColumnIndex, StreamSourceInfo, TableSourceInfo};
    use risingwave_pb::plan_common::ColumnCatalog;
    use risingwave_pb::stream_plan::source_node::Info;
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

        let mem_source_manager: SourceManagerRef = Arc::new(MemSourceManager::default());
        let source_builder =
            SourceDescBuilder::new(source_id, &Info::StreamSource(info), &mem_source_manager);
        let source = source_builder.build().await;

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

        let info = TableSourceInfo {
            row_id_index: None,
            columns: schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| ColumnCatalog {
                    column_desc: Some(
                        ColumnDesc {
                            data_type: f.data_type.clone(),
                            column_id: ColumnId::from(i as i32), // use column index as column id
                            name: f.name.clone(),
                            field_descs: vec![],
                            type_name: "".to_string(),
                        }
                        .to_protobuf(),
                    ),
                    is_hidden: false,
                })
                .collect(),
            pk_column_ids: vec![1],
            properties: Default::default(),
        };

        let _keyspace = Keyspace::table_root(MemoryStateStore::new(), &table_id);

        let mem_source_manager: SourceManagerRef = Arc::new(MemSourceManager::default());
        let source_builder =
            SourceDescBuilder::new(table_id, &Info::TableSource(info), &mem_source_manager);
        let res = source_builder.build().await;
        assert!(res.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_ok());

        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_err());

        Ok(())
    }
}
