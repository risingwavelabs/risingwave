use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataTypeKind;
use risingwave_pb::plan::create_source_node::RowFormatType;
use risingwave_pb::plan::plan_node::NodeBody;
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

pub(super) struct CreateSourceExecutor {
    table_id: TableId,
    config: SourceConfig,
    format: SourceFormat,
    parser: Option<Arc<dyn SourceParser>>,
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
        let node = try_match_expand!(source.plan_node().get_node_body(), NodeBody::CreateSource)?;

        let table_id = TableId::from(&node.table_ref_id);

        let row_id_index = node.get_row_id_index();

        let columns = node
            .get_column_descs()
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                Ok(SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: DataTypeKind::from(c.get_column_type()),
                    column_id: c.column_id,
                    skip_parse: idx as i32 == row_id_index,
                    is_primary: c.is_primary,
                })
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()?;

        let format = match node.get_format() {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::Avro => SourceFormat::Avro,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
        };

        let properties = node.get_properties();

        let schema_location = node.get_schema_location();

        if let SourceFormat::Protobuf = format {
            if schema_location.is_empty() {
                return Err(RwError::from(ProtocolError(
                    "protobuf file location not provided".to_string(),
                )));
            }
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
            source_manager: source.global_task_env().source_manager_ref(),
            columns,
            schema: Schema { fields: vec![] },
            properties: properties.clone(),
            schema_location: schema_location.clone(),
            parser: None,
            row_id_index,
            identity: format!("CreateSourceExecutor{:?}", source.task_id),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateSourceExecutor {
    async fn open(&mut self) -> Result<()> {
        let parser: Arc<dyn SourceParser> = match self.format {
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
        self.source_manager.create_source(
            &self.table_id,
            self.format.clone(),
            self.parser.clone().unwrap(),
            &self.config,
            self.columns.clone(),
            self.row_id_index,
        )?;

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
