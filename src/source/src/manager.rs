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
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::base::SourceReader;
use risingwave_connector::new_connector;
use risingwave_pb::catalog::{RowFormatType, StreamSourceInfo};

use crate::connector_source::ConnectorSource;
use crate::table_v2::TableSourceV2;
use crate::{
    DebeziumJsonParser, HighLevelKafkaSource, JSONParser, ProtobufParser, SourceConfig,
    SourceFormat, SourceImpl, SourceParser,
};

pub type SourceRef = Arc<SourceImpl>;

// the same key is defined in `src/batch/src/executor/create_source.rs`, remove in batch if
// necessary
const UPSTREAM_SOURCE_KEY: &str = "connector";
const KINESIS_SOURCE: &str = "kinesis";
const KAFKA_SOURCE: &str = "kafka";

const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
const PROTOBUF_TEMP_LOCAL_FILENAME: &str = "rw.proto";
const PROTOBUF_FILE_URL_SCHEME: &str = "file";

#[async_trait]
pub trait SourceManager: Debug + Sync + Send {
    async fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Arc<dyn SourceParser + Send + Sync>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: Option<usize>,
    ) -> Result<()>;
    async fn create_source_v2(&self, table_id: &TableId, info: StreamSourceInfo) -> Result<()>;
    fn create_table_source_v2(&self, table_id: &TableId, columns: Vec<ColumnDesc>) -> Result<()>;

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
    pub row_id_index: Option<usize>,
}

pub type SourceManagerRef = Arc<dyn SourceManager>;

#[derive(Debug)]
pub struct MemSourceManager {
    sources: Mutex<HashMap<TableId, SourceDesc>>,
}

fn get_properties<'a>(properties: &'a HashMap<String, String>, key: &'a str) -> Result<&'a str> {
    Ok(properties
        .get(key)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "property {} not found",
                UPSTREAM_SOURCE_KEY
            )))
        })?
        .as_str())
}

#[async_trait]
impl SourceManager for MemSourceManager {
    async fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Arc<dyn SourceParser + Send + Sync>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: Option<usize>,
    ) -> Result<()> {
        let source = match config {
            SourceConfig::Kafka(config) => SourceImpl::HighLevelKafka(HighLevelKafkaSource::new(
                config.clone(),
                Arc::new(columns.clone()),
                parser.clone(),
            )),
            SourceConfig::Connector(config) => {
                let split_reader: Arc<tokio::sync::Mutex<Box<dyn SourceReader + Send + Sync>>> =
                    Arc::new(tokio::sync::Mutex::new(
                        new_connector(config.clone(), None)
                            .await
                            .map_err(|e| RwError::from(InternalError(e.to_string())))?,
                    ));
                SourceImpl::Connector(ConnectorSource {
                    parser: parser.clone(),
                    reader: split_reader,
                    column_descs: columns.clone(),
                })
            }
        };

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_index,
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

    async fn create_source_v2(&self, source_id: &TableId, info: StreamSourceInfo) -> Result<()> {
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

        let parser =
            build_source_parser(&format, &info.properties, info.row_schema_location.as_str())?;

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
        let row_id_index = Some(info.row_id_index as usize);

        let config = match get_properties(&info.properties, UPSTREAM_SOURCE_KEY)? {
            // TODO support more connector here
            KINESIS_SOURCE => Ok(SourceConfig::Connector(info.properties.clone())),
            KAFKA_SOURCE => Ok(SourceConfig::Connector(info.properties.clone())),
            other => Err(RwError::from(ProtocolError(format!(
                "source type {} not supported",
                other
            )))),
        }?;

        let source =
            match config {
                SourceConfig::Kafka(config) => SourceImpl::HighLevelKafka(
                    HighLevelKafkaSource::new(config, Arc::new(columns.clone()), parser.clone()),
                ),
                SourceConfig::Connector(config) => {
                    let split_reader: Arc<tokio::sync::Mutex<Box<dyn SourceReader + Send + Sync>>> =
                        Arc::new(tokio::sync::Mutex::new(
                            new_connector(config.clone(), None)
                                .await
                                .map_err(|e| RwError::from(InternalError(e.to_string())))?,
                        ));
                    SourceImpl::Connector(ConnectorSource {
                        parser: parser.clone(),
                        reader: split_reader,
                        column_descs: columns.clone(),
                    })
                }
            };

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_index,
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

    fn create_table_source_v2(&self, table_id: &TableId, columns: Vec<ColumnDesc>) -> Result<()> {
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
            row_id_index: Some(0), // always use the first column as row_id
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
    pub fn new() -> Self {
        MemSourceManager {
            sources: Mutex::new(HashMap::new()),
        }
    }

    fn get_sources(&self) -> Result<MutexGuard<HashMap<TableId, SourceDesc>>> {
        Ok(self.sources.lock())
    }
}

impl Default for MemSourceManager {
    fn default() -> Self {
        Self::new()
    }
}

fn build_source_parser(
    format: &SourceFormat,
    properties: &HashMap<String, String>,
    schema_location: &str,
) -> Result<Arc<dyn SourceParser + Send + Sync>> {
    let parser: Arc<dyn SourceParser + Send + Sync> = match format {
        SourceFormat::Json => {
            let parser: Arc<dyn SourceParser + Send + Sync> = Arc::new(JSONParser {});
            Ok(parser)
        }
        SourceFormat::Protobuf => {
            let message_name = properties.get(PROTOBUF_MESSAGE_KEY).ok_or_else(|| {
                RwError::from(ProtocolError(format!(
                    "{} not found in properties",
                    PROTOBUF_MESSAGE_KEY
                )))
            })?;

            let parser: Arc<dyn SourceParser + Send + Sync> =
                Arc::new(ProtobufParser::new(schema_location, message_name)?);

            Ok(parser)
        }
        SourceFormat::DebeziumJson => {
            let parser: Arc<dyn SourceParser + Send + Sync> = Arc::new(DebeziumJsonParser {});
            Ok(parser)
        }
        _ => Err(RwError::from(InternalError(
            "format not support".to_string(),
        ))),
    }?;

    Ok(parser)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::DataType;
    use risingwave_connector::kinesis::config::kinesis_demo_properties;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan::ColumnCatalog;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use crate::*;

    const KAFKA_TOPIC_KEY: &str = "kafka.topic";
    const KAFKA_BOOTSTRAP_SERVERS_KEY: &str = "kafka.bootstrap.servers";

    #[tokio::test]
    async fn test_source() -> Result<()> {
        // init
        let table_id = TableId::default();
        let format = SourceFormat::Json;
        let parser = Arc::new(JSONParser {});

        let config = SourceConfig::Kafka(HighLevelKafkaSourceConfig {
            bootstrap_servers: KAFKA_BOOTSTRAP_SERVERS_KEY
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            topic: KAFKA_TOPIC_KEY.to_string(),
            properties: Default::default(),
        });

        let columns = vec![ColumnDesc::unnamed(ColumnId::from(0), DataType::Int64)];
        let source_columns = columns
            .iter()
            .map(|c| SourceColumnDesc {
                name: "123".to_string(),
                data_type: c.data_type.clone(),
                column_id: c.column_id,
                skip_parse: false,
            })
            .collect();

        // create source
        let mem_source_manager = MemSourceManager::new();
        let new_source = mem_source_manager
            .create_source(&table_id, format, parser, &config, source_columns, Some(0))
            .await;
        assert!(new_source.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id)?;
        assert_eq!(get_source_res.columns[0].name, "123");

        // drop source
        let drop_source_res = mem_source_manager.drop_source(&table_id);
        assert!(drop_source_res.is_ok());

        let get_source_res = mem_source_manager.get_source(&table_id);

        assert!(get_source_res.is_err());

        Ok(())
    }

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

        let mem_source_manager = MemSourceManager::new();
        let source = mem_source_manager.create_source_v2(&source_id, info).await;

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

        let mem_source_manager = MemSourceManager::new();
        let res = mem_source_manager.create_table_source_v2(&table_id, table_columns);
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
