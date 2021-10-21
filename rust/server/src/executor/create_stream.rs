use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError, ProtocolError};
use crate::error::Result;
use crate::error::RwError;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::source::{FileSourceConfig, KafkaSourceConfig, SourceFormat};
use crate::source::{SourceColumnDesc, SourceConfig, SourceManagerRef};
use crate::types::build_from_proto;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{
    CreateStreamNode, CreateStreamNode_RowFormatType, PlanNode_PlanNodeType,
};
use std::collections::HashMap;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct CreateStreamExecutor {
    table_id: TableId,
    config: SourceConfig,
    format: SourceFormat,
    columns: Vec<SourceColumnDesc>,
    source_manager: SourceManagerRef,
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
        Ok(SourceConfig::Kafka(KafkaSourceConfig {
            bootstrap_servers: get_from_properties!(properties, "kafka_bootstrap_servers")
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            topic: get_from_properties!(properties, "kafka_topic").clone(),
            properties: Default::default(),
        }))
    }
    fn extract_file_config(properties: &HashMap<String, String>) -> Result<SourceConfig> {
        Ok(SourceConfig::File(FileSourceConfig {
            filename: get_from_properties!(properties, "local_file_path").clone(),
        }))
    }
}

impl BoxedExecutorBuilder for CreateStreamExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::CREATE_STREAM);

        let node = CreateStreamNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let columns = node
            .get_column_descs()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Ok(SourceColumnDesc {
                    name: c.name.clone(),
                    data_type: build_from_proto(c.get_column_type())?,
                    // todo, column id should be passed from ColumnDesc
                    column_id: i as i32,
                })
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()?;

        let format = match node.get_format() {
            CreateStreamNode_RowFormatType::JSON => SourceFormat::Json,
            CreateStreamNode_RowFormatType::PROTOBUF => SourceFormat::Protobuf,
            CreateStreamNode_RowFormatType::AVRO => SourceFormat::Avro,
        };

        let properties = node.get_properties();

        let config = match get_from_properties!(properties, "upstream_source").as_str() {
            "kafka" => CreateStreamExecutor::extract_kafka_config(properties),
            "file" => CreateStreamExecutor::extract_file_config(properties),
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
        }))
    }
}

impl Executor for CreateStreamExecutor {
    fn init(&mut self) -> Result<()> {
        info!("create stream executor initing!");
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        self.source_manager.create_source(
            &self.table_id,
            self.format.clone(),
            &self.config,
            self.columns.clone(),
        )?;

        Ok(ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("create stream executor cleaned!");
        Ok(())
    }
}
