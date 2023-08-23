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
use risingwave_common::error::{Result, RwError};
use risingwave_connector::parser::SpecificParserConfig;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_connector::source::{SourceColumnDesc, SourceEncode, SourceFormat, SourceStruct};
use risingwave_connector::ConnectorParams;
use risingwave_pb::catalog::PbStreamSourceInfo;
use risingwave_pb::plan_common::{PbColumnCatalog, PbEncodeType, PbFormatType, RowFormatType};

use crate::connector_source::ConnectorSource;
use crate::fs_connector_source::FsConnectorSource;

pub const DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 16;

/// `SourceDesc` describes a stream source.
#[derive(Debug)]
pub struct SourceDesc {
    pub source: ConnectorSource,
    pub source_struct: SourceStruct,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

/// `FsSourceDesc` describes a stream source.
#[derive(Debug)]
pub struct FsSourceDesc {
    pub source: FsConnectorSource,
    pub source_struct: SourceStruct,
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
    pk_indices: Vec<usize>,
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
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            columns,
            metrics,
            row_id_index,
            properties,
            source_info,
            connector_params,
            connector_message_buffer_size,
            pk_indices,
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
        for pk_index in &self.pk_indices {
            columns[*pk_index].is_pk = true;
        }
        columns
    }

    pub fn build(self) -> Result<SourceDesc> {
        let columns = self.column_catalogs_to_source_column_descs();

        let source_struct = extract_source_struct(&self.source_info)?;
        let psrser_config =
            SpecificParserConfig::new(source_struct, &self.source_info, &self.properties)?;

        let source = ConnectorSource::new(
            self.properties,
            columns.clone(),
            self.connector_message_buffer_size,
            psrser_config,
        )?;

        Ok(SourceDesc {
            source,
            source_struct,
            columns,
            metrics: self.metrics,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    pub fn build_fs_source_desc(&self) -> Result<FsSourceDesc> {
        let source_struct = extract_source_struct(&self.source_info)?;
        match (source_struct.format, source_struct.encode) {
            (SourceFormat::Plain, SourceEncode::Csv | SourceEncode::Json) => {}
            (format, encode) => {
                return Err(RwError::from(ProtocolError(format!(
                    "Unsupported combination of format {:?} and encode {:?}",
                    format, encode
                ))));
            }
        }

        let columns = self.column_catalogs_to_source_column_descs();

        let parser_config =
            SpecificParserConfig::new(source_struct, &self.source_info, &self.properties)?;

        let source = FsConnectorSource::new(
            self.properties.clone(),
            columns.clone(),
            self.connector_params
                .connector_client
                .as_ref()
                .map(|client| client.endpoint().clone()),
            parser_config,
        )?;

        Ok(FsSourceDesc {
            source,
            source_struct,
            columns,
            metrics: self.metrics.clone(),
        })
    }
}

// Only return valid (format, encode)
pub fn extract_source_struct(info: &PbStreamSourceInfo) -> Result<SourceStruct> {
    // old version meta.
    if let Ok(format) = info.get_row_format() {
        let (format, encode) = match format {
            RowFormatType::Json => (SourceFormat::Plain, SourceEncode::Json),
            RowFormatType::Protobuf => (SourceFormat::Plain, SourceEncode::Protobuf),
            RowFormatType::DebeziumJson => (SourceFormat::Debezium, SourceEncode::Json),
            RowFormatType::Avro => (SourceFormat::Plain, SourceEncode::Avro),
            RowFormatType::Maxwell => (SourceFormat::Maxwell, SourceEncode::Json),
            RowFormatType::CanalJson => (SourceFormat::Canal, SourceEncode::Json),
            RowFormatType::Csv => (SourceFormat::Plain, SourceEncode::Csv),
            RowFormatType::Native => (SourceFormat::Native, SourceEncode::Native),
            RowFormatType::DebeziumAvro => (SourceFormat::Debezium, SourceEncode::Avro),
            RowFormatType::UpsertJson => (SourceFormat::Upsert, SourceEncode::Json),
            RowFormatType::UpsertAvro => (SourceFormat::Upsert, SourceEncode::Avro),
            RowFormatType::DebeziumMongoJson => (SourceFormat::DebeziumMongo, SourceEncode::Json),
            RowFormatType::Bytes => (SourceFormat::Plain, SourceEncode::Bytes),
            RowFormatType::RowUnspecified => unreachable!(),
        };
        return Ok(SourceStruct::new(format, encode));
    }
    let source_format = info.get_format()?;
    let source_encode = info.get_row_encode()?;
    let (format, encode) = match (source_format, source_encode) {
        (PbFormatType::Plain, PbEncodeType::Json) => (SourceFormat::Plain, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Protobuf) => {
            (SourceFormat::Plain, SourceEncode::Protobuf)
        }
        (PbFormatType::Debezium, PbEncodeType::Json) => {
            (SourceFormat::Debezium, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Avro) => (SourceFormat::Plain, SourceEncode::Avro),
        (PbFormatType::Maxwell, PbEncodeType::Json) => (SourceFormat::Maxwell, SourceEncode::Json),
        (PbFormatType::Canal, PbEncodeType::Json) => (SourceFormat::Canal, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Csv) => (SourceFormat::Plain, SourceEncode::Csv),
        (PbFormatType::Native, PbEncodeType::Native) => {
            (SourceFormat::Native, SourceEncode::Native)
        }
        (PbFormatType::Debezium, PbEncodeType::Avro) => {
            (SourceFormat::Debezium, SourceEncode::Avro)
        }
        (PbFormatType::Upsert, PbEncodeType::Json) => (SourceFormat::Upsert, SourceEncode::Json),
        (PbFormatType::Upsert, PbEncodeType::Avro) => (SourceFormat::Upsert, SourceEncode::Avro),
        (PbFormatType::DebeziumMongo, PbEncodeType::Json) => {
            (SourceFormat::DebeziumMongo, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Bytes) => (SourceFormat::Plain, SourceEncode::Bytes),
        (format, encode) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unsupported combination of format {:?} and encode {:?}",
                format, encode
            ))));
        }
    };
    Ok(SourceStruct::new(format, encode))
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
        pk_indices: Vec<usize>,
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
                        generated_or_default_column: None,
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
            pk_indices,
        }
    }
}
