// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::Result;
use risingwave_connector::parser::SpecificParserConfig;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_connector::source::{SourceColumnDesc, SourceFormat};
use risingwave_connector::ConnectorParams;
use risingwave_pb::catalog::PbStreamSourceInfo;
use risingwave_pb::plan_common::{PbColumnCatalog, PbRowFormatType};

use crate::connector_source::ConnectorSource;
use crate::fs_connector_source::FsConnectorSource;

pub const DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 16;

/// `SourceDesc` describes a stream source.
#[derive(Debug)]
pub struct SourceDesc {
    pub source: ConnectorSource,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

/// `FsSourceDesc` describes a stream source.
#[derive(Debug)]
pub struct FsSourceDesc {
    pub source: FsConnectorSource,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

#[derive(Clone)]
pub struct SourceDescBuilder {
    columns: Vec<PbColumnCatalog>,
    metrics: Arc<SourceMetrics>,
    row_id_index: Option<usize>,
    properties: HashMap<String, String>,
    source_info: PbStreamSourceInfo,
    connector_params: ConnectorParams,
    connector_message_buffer_size: usize,
}

impl SourceDescBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        columns: Vec<PbColumnCatalog>,
        metrics: Arc<SourceMetrics>,
        row_id_index: Option<usize>,
        properties: HashMap<String, String>,
        source_info: PbStreamSourceInfo,
        connector_params: ConnectorParams,
        connector_message_buffer_size: usize,
    ) -> Self {
        Self {
            columns,
            metrics,
            row_id_index,
            properties,
            source_info,
            connector_params,
            connector_message_buffer_size,
        }
    }

    fn column_catalogs_to_source_column_descs(&self) -> Vec<SourceColumnDesc> {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        if let Some(row_id_index) = self.row_id_index {
            columns[row_id_index].is_row_id = true;
        }
        columns
    }

    pub async fn build(self) -> Result<SourceDesc> {
        let format = match self.source_info.get_row_format()? {
            PbRowFormatType::Json => SourceFormat::Json,
            PbRowFormatType::Protobuf => SourceFormat::Protobuf,
            PbRowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            PbRowFormatType::Avro => SourceFormat::Avro,
            PbRowFormatType::Maxwell => SourceFormat::Maxwell,
            PbRowFormatType::CanalJson => SourceFormat::CanalJson,
            PbRowFormatType::Native => SourceFormat::Native,
            PbRowFormatType::DebeziumAvro => SourceFormat::DebeziumAvro,
            PbRowFormatType::UpsertJson => SourceFormat::UpsertJson,
            PbRowFormatType::UpsertAvro => SourceFormat::UpsertAvro,
            PbRowFormatType::DebeziumMongoJson => SourceFormat::DebeziumMongoJson,
            _ => unreachable!(),
        };

        if format == SourceFormat::Protobuf && self.source_info.row_schema_location.is_empty() {
            return Err(ProtocolError("protobuf file location not provided".to_string()).into());
        }

        let columns = self.column_catalogs_to_source_column_descs();

        let psrser_config =
            SpecificParserConfig::new(format, &self.source_info, &self.properties).await?;

        let source = ConnectorSource::new(
            self.properties,
            columns.clone(),
            self.connector_params.connector_rpc_endpoint,
            self.connector_message_buffer_size,
            psrser_config,
        )?;

        Ok(SourceDesc {
            source,
            format,
            columns,
            metrics: self.metrics,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    pub async fn build_fs_source_desc(&self) -> Result<FsSourceDesc> {
        let format = match self.source_info.get_row_format()? {
            PbRowFormatType::Csv => SourceFormat::Csv,
            PbRowFormatType::Json => SourceFormat::Json,
            _ => unreachable!(),
        };

        let columns = self.column_catalogs_to_source_column_descs();

        let parser_config =
            SpecificParserConfig::new(format, &self.source_info, &self.properties).await?;

        let source = FsConnectorSource::new(
            self.properties.clone(),
            columns.clone(),
            self.connector_params.connector_rpc_endpoint.clone(),
            parser_config,
        )?;

        Ok(FsSourceDesc {
            source,
            format,
            columns,
            metrics: self.metrics.clone(),
        })
    }
}

pub mod test_utils {
    use std::collections::HashMap;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::ColumnCatalog;

    use super::{SourceDescBuilder, DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE};

    pub fn create_source_desc_builder(
        schema: &Schema,
        row_id_index: Option<usize>,
        source_info: StreamSourceInfo,
        properties: HashMap<String, String>,
    ) -> SourceDescBuilder {
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
                        generated_column: None,
                        is_from_key: false,
                    }
                    .to_protobuf(),
                ),
                is_hidden: false,
            })
            .collect();
        SourceDescBuilder {
            columns,
            metrics: Default::default(),
            row_id_index,
            properties,
            source_info,
            connector_params: Default::default(),
            connector_message_buffer_size: DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
        }
    }
}
