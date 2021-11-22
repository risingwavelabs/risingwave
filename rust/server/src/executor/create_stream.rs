use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::{InternalError, ProstError, ProtocolError};
use risingwave_common::error::Result;
use risingwave_common::error::RwError;
use risingwave_common::types::build_from_prost;
use risingwave_pb::plan::create_stream_node::RowFormatType;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::CreateStreamNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder};
use crate::source::parser::JSONParser;
use crate::source::SourceFormat;
use crate::source::{
    HighLevelKafkaSourceConfig, SourceColumnDesc, SourceConfig, SourceManagerRef, SourceParser,
};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct CreateStreamExecutor {
    table_id: TableId,
    config: SourceConfig,
    format: SourceFormat,
    parser: Option<Arc<dyn SourceParser>>,
    columns: Vec<SourceColumnDesc>,
    source_manager: SourceManagerRef,
    properties: HashMap<String, String>,
    schema: Schema,
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
            bootstrap_servers: get_from_properties!(properties, "kafka_bootstrap_servers")
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            topic: get_from_properties!(properties, "kafka_topic").clone(),
            properties: Default::default(),
        }))
    }
}

impl BoxedExecutorBuilder for CreateStreamExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::CreateStream);

        let node = CreateStreamNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            node.to_proto::<risingwave_proto::plan::CreateStreamNode>()
                .get_table_ref_id(),
        )
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let columns = node
            .get_column_descs()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Ok(SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: build_from_prost(c.get_column_type())?,
                    // todo, column id should be passed from ColumnDesc
                    column_id: i as i32,
                })
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()?;

        let format = match node.get_format() {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::Avro => SourceFormat::Avro,
        };

        let properties = node.get_properties();

        let config = match get_from_properties!(properties, "upstream_source").as_str() {
            "kafka" => CreateStreamExecutor::extract_kafka_config(properties),
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
            parser: None,
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateStreamExecutor {
    async fn open(&mut self) -> Result<()> {
        let parser: Arc<dyn SourceParser> = match self.format {
            SourceFormat::Json => Ok(Arc::new(JSONParser {})),
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
