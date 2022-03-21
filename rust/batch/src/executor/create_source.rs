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
//
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::RowFormatType;
use risingwave_source::parser::JSONParser;
use risingwave_source::{
    DebeziumJsonParser, HighLevelKafkaSourceConfig, ProtobufParser, SourceColumnDesc, SourceConfig,
    SourceFormat, SourceManagerRef, SourceParser,
};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

const UPSTREAM_SOURCE_KEY: &str = "upstream.source";
const KAFKA_SOURCE: &str = "kafka";

const KAFKA_TOPIC_KEY: &str = "kafka.topic";
const KAFKA_BOOTSTRAP_SERVERS_KEY: &str = "kafka.bootstrap.servers";

const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
const PROTOBUF_TEMP_LOCAL_FILENAME: &str = "rw.proto";
const PROTOBUF_FILE_URL_SCHEME: &str = "file";

// TODO: All DDLs should be RPC requests from the meta service. Remove this.
pub(super) struct CreateSourceExecutor {
    table_id: TableId,
    config: SourceConfig,
    format: SourceFormat,
    parser: Option<Arc<dyn SourceParser + Send + Sync>>,
    columns: Vec<SourceColumnDesc>,
    source_manager: SourceManagerRef,
    properties: HashMap<String, String>,
    schema_location: String,
    schema: Schema,
    row_id_index: Option<usize>,
    identity: String,
}

macro_rules! get_from_properties {
    ($node_type:expr, $source:expr) => {
        $node_type.get($source).ok_or_else(|| {
            RwError::from(ProtocolError(format!("property {} not found", $source)))
        })?
    };
}

impl CreateSourceExecutor {
    fn extract_kafka_config(properties: &HashMap<String, String>) -> Result<SourceConfig> {
        Ok(SourceConfig::Kafka(HighLevelKafkaSourceConfig {
            bootstrap_servers: get_from_properties!(properties, KAFKA_BOOTSTRAP_SERVERS_KEY)
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            topic: get_from_properties!(properties, KAFKA_TOPIC_KEY).clone(),
            properties: Default::default(),
        }))
    }
}

impl BoxedExecutorBuilder for CreateSourceExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::CreateSource
        )?;

        let table_id = TableId::from(&node.table_ref_id);
        let info = node.get_info().unwrap();

        let row_id_index = info.get_row_id_index();

        let columns = node
            .get_column_descs()
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                Ok(SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: DataType::from(c.get_column_type()?),
                    column_id: ColumnId::from(c.column_id),
                    skip_parse: idx as i32 == row_id_index,
                })
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()?;

        let format = match info.get_row_format().unwrap() {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::Avro => SourceFormat::Avro,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
        };

        let properties = info.get_properties();

        let schema_location = info.get_row_schema_location();

        if format == SourceFormat::Protobuf && schema_location.is_empty() {
            return Err(RwError::from(ProtocolError(
                "protobuf file location not provided".to_string(),
            )));
        }

        let config = match get_from_properties!(properties, UPSTREAM_SOURCE_KEY).as_str() {
            KAFKA_SOURCE => CreateSourceExecutor::extract_kafka_config(properties),
            other => Err(RwError::from(ProtocolError(format!(
                "source type {} not supported",
                other
            )))),
        }?;

        let row_id_index = if row_id_index >= 0 {
            Some(row_id_index as usize)
        } else {
            None
        };

        Ok(Box::new(Self {
            table_id,
            config,
            format,
            source_manager: source.global_batch_env().source_manager_ref(),
            columns,
            schema: Schema { fields: vec![] },
            properties: properties.clone(),
            schema_location: schema_location.clone(),
            parser: None,
            row_id_index,
            identity: "CreateSourceExecutor".to_string(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateSourceExecutor {
    async fn open(&mut self) -> Result<()> {
        let parser: Arc<dyn SourceParser + Send + Sync> = match self.format {
            SourceFormat::Json => {
                let parser: Arc<dyn SourceParser> = Arc::new(JSONParser {});
                Ok(parser)
            }
            SourceFormat::Protobuf => {
                let message_name = self.properties.get(PROTOBUF_MESSAGE_KEY).ok_or_else(|| {
                    RwError::from(ProtocolError(format!(
                        "{} not found in properties",
                        PROTOBUF_MESSAGE_KEY
                    )))
                })?;

                let parser: Arc<dyn SourceParser> = Arc::new(ProtobufParser::new(
                    self.schema_location.as_str(),
                    message_name,
                )?);

                Ok(parser)
            }
            SourceFormat::DebeziumJson => {
                let parser: Arc<dyn SourceParser> = Arc::new(DebeziumJsonParser {});
                Ok(parser)
            }
            _ => Err(RwError::from(InternalError(
                "format not support".to_string(),
            ))),
        }?;

        self.parser = Some(parser);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        self.source_manager
            .create_source(
                &self.table_id,
                self.format.clone(),
                self.parser.clone().unwrap(),
                &self.config,
                self.columns.clone(),
                self.row_id_index,
            )
            .await?;

        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
