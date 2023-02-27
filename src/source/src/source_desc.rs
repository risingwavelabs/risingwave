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
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, StreamSourceInfo as ProstStreamSourceInfo,
};
use risingwave_pb::plan_common::{
    ColumnCatalog as ProstColumnCatalog, RowFormatType as ProstRowFormatType,
};

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
    pub pk_column_ids: Vec<i32>,
}

/// `FsSourceDesc` describes a stream source.
#[derive(Debug)]
pub struct FsSourceDesc {
    pub source: FsConnectorSource,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
    pub pk_column_ids: Vec<i32>,
}

#[derive(Clone)]
pub struct SourceDescBuilder {
    columns: Vec<ProstColumnCatalog>,
    metrics: Arc<SourceMetrics>,
    pk_column_ids: Vec<i32>,
    row_id_index: Option<ProstColumnIndex>,
    properties: HashMap<String, String>,
    source_info: ProstStreamSourceInfo,
    connector_params: ConnectorParams,
    connector_message_buffer_size: usize,
}

impl SourceDescBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        columns: Vec<ProstColumnCatalog>,
        metrics: Arc<SourceMetrics>,
        pk_column_ids: Vec<i32>,
        row_id_index: Option<ProstColumnIndex>,
        properties: HashMap<String, String>,
        source_info: ProstStreamSourceInfo,
        connector_params: ConnectorParams,
        connector_message_buffer_size: usize,
    ) -> Self {
        Self {
            columns,
            metrics,
            pk_column_ids,
            row_id_index,
            properties,
            source_info,
            connector_params,
            connector_message_buffer_size,
        }
    }

    pub async fn build(self) -> Result<SourceDesc> {
        let format = match self.source_info.get_row_format()? {
            ProstRowFormatType::Json => SourceFormat::Json,
            ProstRowFormatType::Protobuf => SourceFormat::Protobuf,
            ProstRowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            ProstRowFormatType::Avro => SourceFormat::Avro,
            ProstRowFormatType::Maxwell => SourceFormat::Maxwell,
            ProstRowFormatType::CanalJson => SourceFormat::CanalJson,
            ProstRowFormatType::Native => SourceFormat::Native,
            ProstRowFormatType::DebeziumAvro => SourceFormat::DebeziumAvro,
            ProstRowFormatType::UpsertJson => SourceFormat::UpsertJson,
            ProstRowFormatType::UpsertAvro => SourceFormat::UpsertAvro,
            _ => unreachable!(),
        };

        if format == SourceFormat::Protobuf && self.source_info.row_schema_location.is_empty() {
            return Err(ProtocolError("protobuf file location not provided".to_string()).into());
        }

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        if let Some(row_id_index) = self.row_id_index.as_ref() {
            columns[row_id_index.index as usize].is_row_id = true;
        }
        assert!(
            !self.pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

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
            pk_column_ids: self.pk_column_ids,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    pub async fn build_fs_source_desc(&self) -> Result<FsSourceDesc> {
        let format = match self.source_info.get_row_format()? {
            ProstRowFormatType::Csv => SourceFormat::Csv,
            _ => unreachable!(),
        };

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();

        if let Some(row_id_index) = self.row_id_index.as_ref() {
            columns[row_id_index.index as usize].is_row_id = true;
        }

        assert!(
            !self.pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

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
            pk_column_ids: self.pk_column_ids.clone(),
        })
    }
}

pub mod test_utils {
    use std::collections::HashMap;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
    use risingwave_pb::catalog::{ColumnIndex, StreamSourceInfo};
    use risingwave_pb::plan_common::ColumnCatalog;

    use super::{SourceDescBuilder, DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE};

    pub fn create_source_desc_builder(
        schema: &Schema,
        pk_column_ids: Vec<i32>,
        row_id_index: Option<u64>,
        source_info: StreamSourceInfo,
        properties: HashMap<String, String>,
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
        SourceDescBuilder {
            columns,
            metrics: Default::default(),
            pk_column_ids,
            row_id_index,
            properties,
            source_info,
            connector_params: Default::default(),
            connector_message_buffer_size: DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
        }
    }
}
