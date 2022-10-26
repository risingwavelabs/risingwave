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
use std::sync::{Arc, Weak};

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::ErrorCode::{ConnectorError, InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_connector::source::ConnectorProperties;
use risingwave_pb::catalog::ColumnIndex as ProstColumnIndex;
use risingwave_pb::plan_common::{ColumnCatalog as ProstColumnCatalog, RowFormatType};
use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

use crate::monitor::SourceMetrics;
use crate::table::TableSource;
use crate::{ConnectorSource, SourceFormat, SourceImpl, SourceParserImpl};

pub type SourceDescRef = Arc<SourceDesc>;
type WeakSourceDescRef = Weak<SourceDesc>;

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
#[derive(Debug)]
pub struct SourceDesc {
    pub source: SourceImpl,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,

    // The column index of row ID. If the primary key is specified by users, this will be `None`.
    pub row_id_index: Option<usize>,
    pub pk_column_ids: Vec<i32>,
}

pub type TableSourceManagerRef = Arc<TableSourceManager>;

#[derive(Debug)]
pub struct TableSourceManager {
    sources: Mutex<HashMap<TableId, WeakSourceDescRef>>,
    /// local source metrics
    metrics: Arc<SourceMetrics>,
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    connector_message_buffer_size: usize,
}

impl TableSourceManager {
    pub fn get_source(&self, source_id: &TableId) -> Result<SourceDescRef> {
        let sources = self.sources.lock();
        sources
            .get(source_id)
            .and_then(|weak_ref| weak_ref.upgrade())
            .ok_or_else(|| {
                InternalError(format!("Get source table id not exists: {:?}", source_id)).into()
            })
    }

    pub fn insert_source(
        &self,
        source_id: TableId,
        row_id_index: Option<ProstColumnIndex>,
        columns: Vec<ProstColumnCatalog>,
        pk_column_ids: Vec<i32>,
    ) -> Result<SourceDescRef> {
        let mut sources = self.sources.lock();
        sources.drain_filter(|_, weak_ref| weak_ref.strong_count() == 0);
        if let Some(strong_ref) = sources
            .get(&source_id)
            .and_then(|weak_ref| weak_ref.upgrade())
        {
            Ok(strong_ref)
        } else {
            let columns = columns
                .iter()
                .map(|c| ColumnDesc::from(c.column_desc.as_ref().unwrap()))
                .collect_vec();
            let row_id_index = row_id_index.map(|index| index.index as _);

            // Table sources do not need columns and format
            let strong_ref = Arc::new(SourceDesc {
                columns: columns.iter().map(SourceColumnDesc::from).collect(),
                source: SourceImpl::Table(TableSource::new(columns)),
                format: SourceFormat::Invalid,
                row_id_index,
                pk_column_ids,
                metrics: self.metrics.clone(),
            });
            sources.insert(source_id, Arc::downgrade(&strong_ref));
            Ok(strong_ref)
        }
    }

    /// For recovery, clear all sources' weak references.
    pub fn clear_sources(&self) {
        self.sources.lock().clear()
    }

    fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    fn msg_buf_size(&self) -> usize {
        self.connector_message_buffer_size
    }
}

impl Default for TableSourceManager {
    fn default() -> Self {
        TableSourceManager {
            sources: Default::default(),
            metrics: Default::default(),
            connector_message_buffer_size: 16,
        }
    }
}

impl TableSourceManager {
    pub fn new(metrics: Arc<SourceMetrics>, connector_message_buffer_size: usize) -> Self {
        TableSourceManager {
            sources: Mutex::new(HashMap::new()),
            metrics,
            connector_message_buffer_size,
        }
    }
}

#[derive(Clone)]
pub struct SourceDescBuilder {
    source_id: TableId,
    row_id_index: Option<ProstColumnIndex>,
    columns: Vec<ProstColumnCatalog>,
    pk_column_ids: Vec<i32>,
    properties: HashMap<String, String>,
    info: ProstSourceInfo,
    source_manager: TableSourceManagerRef,
}

impl SourceDescBuilder {
    pub fn new(
        source_id: TableId,
        row_id_index: Option<ProstColumnIndex>,
        columns: Vec<ProstColumnCatalog>,
        pk_column_ids: Vec<i32>,
        properties: HashMap<String, String>,
        info: ProstSourceInfo,
        source_manager: TableSourceManagerRef,
    ) -> Self {
        Self {
            source_id,
            row_id_index,
            columns,
            pk_column_ids,
            properties,
            info,
            source_manager,
        }
    }

    pub async fn build(&self) -> Result<SourceDescRef> {
        match &self.info {
            ProstSourceInfo::TableSource(_) => self.build_table_source(),
            ProstSourceInfo::StreamSource(_) => self.build_stream_source().await,
        }
    }

    fn build_table_source(&self) -> Result<SourceDescRef> {
        self.source_manager.insert_source(
            self.source_id,
            self.row_id_index.clone(),
            self.columns.clone(),
            self.pk_column_ids.clone(),
        )
    }

    async fn build_stream_source(&self) -> Result<SourceDescRef> {
        let info = try_match_expand!(&self.info, ProstSourceInfo::StreamSource).unwrap();
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
            SourceParserImpl::create(&format, &self.properties, info.row_schema_location.as_str())
                .await;
        let parser = if let Ok(source_parser) = source_parser_rs {
            source_parser
        } else {
            return Err(source_parser_rs.err().unwrap());
        };

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        let row_id_index = self.row_id_index.as_ref().map(|row_id_index| {
            columns[row_id_index.index as usize].skip_parse = true;
            row_id_index.index as usize
        });
        assert!(
            !self.pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let config = ConnectorProperties::extract(self.properties.clone())
            .map_err(|e| RwError::from(ConnectorError(e.into())))?;

        let source = SourceImpl::Connector(ConnectorSource {
            config,
            columns: columns.clone(),
            parser,
            connector_message_buffer_size: self.source_manager.msg_buf_size(),
        });

        Ok(Arc::new(SourceDesc {
            source,
            format,
            columns,
            row_id_index,
            pk_column_ids: self.pk_column_ids.clone(),
            metrics: self.source_manager.metrics(),
        }))
    }
}

pub mod test_utils {
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId};
    use risingwave_pb::catalog::{ColumnIndex, TableSourceInfo};
    use risingwave_pb::plan_common::ColumnCatalog;
    use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

    use crate::{SourceDescBuilder, TableSourceManagerRef};

    pub fn create_table_source_desc_builder(
        schema: &Schema,
        source_id: TableId,
        row_id_index: Option<u64>,
        pk_column_ids: Vec<i32>,
        source_manager: TableSourceManagerRef,
    ) -> SourceDescBuilder {
        let row_id_index = row_id_index.map(|index| ColumnIndex { index });
        let columns = schema
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
            .collect();
        let info = ProstSourceInfo::TableSource(TableSourceInfo {});
        SourceDescBuilder {
            source_id,
            row_id_index,
            columns,
            pk_column_ids,
            properties: Default::default(),
            info,
            source_manager,
        }
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
        let row_id_index = Some(ColumnIndex { index: 0 });
        let pk_column_ids = vec![0];
        let info = StreamSourceInfo {
            row_format: 0,
            row_schema_location: "".to_string(),
        };
        let source_id = TableId::default();

        let mem_source_manager: TableSourceManagerRef = Arc::new(TableSourceManager::default());
        let source_builder = SourceDescBuilder::new(
            source_id,
            row_id_index,
            columns,
            pk_column_ids,
            properties,
            Info::StreamSource(info),
            mem_source_manager,
        );
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

        let columns = schema
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
            .collect();
        let pk_column_ids = vec![1];
        let info = TableSourceInfo {};

        let _keyspace = Keyspace::table_root(MemoryStateStore::new(), &table_id);

        let mem_source_manager: TableSourceManagerRef = Arc::new(TableSourceManager::default());
        let mut source_builder = SourceDescBuilder::new(
            table_id,
            None,
            columns,
            pk_column_ids,
            Default::default(),
            Info::TableSource(info),
            mem_source_manager.clone(),
        );
        let res = source_builder.build().await;
        assert!(res.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_ok());

        // drop all replicas of TableId(0)
        drop(res);
        drop(get_source_res);
        // failed to get_source
        let result = mem_source_manager.get_source(&table_id);
        assert!(result.is_err());

        source_builder.source_id = TableId::new(1u32);
        let _new_source = source_builder.build().await;

        assert_eq!(mem_source_manager.sources.lock().len(), 1);
        mem_source_manager.clear_sources();
        assert!(mem_source_manager.sources.lock().is_empty());

        Ok(())
    }
}
