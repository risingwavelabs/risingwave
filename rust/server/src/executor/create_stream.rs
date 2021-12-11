use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::{InternalError, ProstError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::build_from_prost;
use risingwave_pb::plan::create_stream_node::RowFormatType;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::CreateStreamNode;
use risingwave_source::parser::JSONParser;
use risingwave_source::{
    HighLevelKafkaSourceConfig, ProtobufParser, SourceColumnDesc, SourceConfig, SourceFormat,
    SourceManagerRef, SourceParser,
};
use tempfile::Builder;
use tokio::fs;
use url::Url;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

const UPSTREAM_SOURCE_KEY: &str = "upstream.source";
const KAFKA_SOURCE: &str = "kafka";

const KAFKA_TOPIC_KEY: &str = "kafka.topic";
const KAFKA_BOOTSTRAP_SERVERS_KEY: &str = "kafka.bootstrap.servers";

const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
const PROTOBUF_TEMP_LOCAL_FILENAME: &str = "rw.proto";
const PROTOBUF_FILE_URL_SCHEME: &str = "file";

pub(super) struct CreateStreamExecutor {
    table_id: TableId,
    config: SourceConfig,
    format: SourceFormat,
    parser: Option<Arc<dyn SourceParser>>,
    columns: Vec<SourceColumnDesc>,
    source_manager: SourceManagerRef,
    properties: HashMap<String, String>,
    schema_location: String,
    schema: Schema,
    row_id_index: usize,
}

macro_rules! get_from_properties {
    ($node_type:expr, $source:expr) => {
        $node_type.get($source).ok_or_else(|| {
            RwError::from(ProtocolError(format!("property {} not found", $source)))
        })?
    };
}

impl CreateStreamExecutor {
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

impl BoxedExecutorBuilder for CreateStreamExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::CreateStream);

        let node = CreateStreamNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from(&node.table_ref_id);

        let row_id_index = node.get_row_id_index() as usize;

        let columns = node
            .get_column_descs()
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                Ok(SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: build_from_prost(c.get_column_type())?,
                    column_id: c.column_id,
                    skip_parse: idx == row_id_index,
                })
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()?;

        let format = match node.get_format() {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::Avro => SourceFormat::Avro,
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
            KAFKA_SOURCE => CreateStreamExecutor::extract_kafka_config(properties),
            other => Err(RwError::from(ProtocolError(format!(
                "source type {} not supported",
                other
            )))),
        }?;

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
        }))
    }
}

impl CreateStreamExecutor {
    async fn extract_protobuf_parser(
        schema_location: &str,
        message_name: &str,
    ) -> Result<ProtobufParser> {
        let location = schema_location;
        let url = Url::parse(location).map_err(|e| RwError::from(InternalError(e.to_string())))?;

        match url.scheme() {
            PROTOBUF_FILE_URL_SCHEME => {
                let path = url.to_file_path().map_err(|_| {
                    RwError::from(InternalError(format!("path {} illegal", location)))
                })?;

                if path.is_dir() {
                    return Err(RwError::from(ProtocolError(
                        "schema file location is dir".to_string(),
                    )));
                }

                let temp_dir = Builder::new()
                    .rand_bytes(10)
                    .tempdir()
                    .map_err(|e| RwError::from(InternalError(e.to_string())))?;

                let target = temp_dir.path().join(PROTOBUF_TEMP_LOCAL_FILENAME);

                fs::copy(path, target.as_path())
                    .await
                    .map_err(|e| RwError::from(InternalError(e.to_string())))?;

                let dirname = temp_dir.path().to_str().ok_or_else(|| {
                    RwError::from(InternalError(
                        "convert temp path dir to str failed".to_string(),
                    ))
                })?;

                let filename = target.to_str().ok_or_else(|| {
                    RwError::from(InternalError(
                        "convert temp path filename to str failed".to_string(),
                    ))
                })?;

                ProtobufParser::new(
                    &[dirname.to_string()],
                    &[filename.to_string()],
                    message_name,
                )
            }
            scheme => Err(RwError::from(ProtocolError(format!(
                "path scheme {} is not supported now",
                scheme
            )))),
        }
    }
}

#[async_trait::async_trait]
impl Executor for CreateStreamExecutor {
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

                let parser: Arc<dyn SourceParser> = Arc::new(
                    Self::extract_protobuf_parser(self.schema_location.as_str(), message_name)
                        .await?,
                );

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
}
