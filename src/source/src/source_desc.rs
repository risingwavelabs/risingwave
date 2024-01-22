// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::additional_columns::common_compatible_column_vec;
use risingwave_connector::parser::{EncodingProperties, ProtocolProperties, SpecificParserConfig};
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_connector::source::{
    get_connector_compatible_additional_columns, SourceColumnDesc, SourceColumnType,
    UPSTREAM_SOURCE_KEY,
};
use risingwave_connector::ConnectorParams;
use risingwave_pb::catalog::PbStreamSourceInfo;
use risingwave_pb::plan_common::{AdditionalColumnType, PbColumnCatalog};

use crate::connector_source::ConnectorSource;
use crate::fs_connector_source::FsConnectorSource;

pub const DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 16;

/// `SourceDesc` describes a stream source.
#[derive(Debug, Clone)]
pub struct SourceDesc {
    pub source: ConnectorSource,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

/// `FsSourceDesc` describes a stream source.
#[derive(Debug)]
pub struct FsSourceDesc {
    pub source: FsConnectorSource,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

#[derive(Clone)]
pub struct SourceDescBuilder {
    columns: Vec<PbColumnCatalog>,
    metrics: Arc<SourceMetrics>,
    row_id_index: Option<usize>,
    with_properties: HashMap<String, String>,
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
        with_properties: HashMap<String, String>,
        source_info: PbStreamSourceInfo,
        connector_params: ConnectorParams,
        connector_message_buffer_size: usize,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            columns,
            metrics,
            row_id_index,
            with_properties,
            source_info,
            connector_params,
            connector_message_buffer_size,
            pk_indices,
        }
    }

    pub fn column_catalogs_to_source_column_descs(&self) -> Vec<SourceColumnDesc> {
        let mut columns_exist = [false; 2];
        let mut last_column_id = self
            .columns
            .iter()
            .map(|c| c.column_desc.as_ref().unwrap().column_id.into())
            .max()
            .unwrap_or(ColumnId::placeholder());
        let connector_name = self
            .with_properties
            .get(UPSTREAM_SOURCE_KEY)
            .map(|s| s.to_lowercase())
            .unwrap();

        let additional_columns: Vec<_> = {
            let col_list = get_connector_compatible_additional_columns(&connector_name)
                .unwrap_or(common_compatible_column_vec());
            ["partition", "file", "offset"]
                .into_iter()
                .filter_map(|key_name| {
                    let col_name = format!("_rw_{}_{}", connector_name, key_name);
                    col_list.iter().find_map(|(n, f)| {
                        if key_name == *n {
                            last_column_id = last_column_id.next();
                            Some(f(last_column_id, &col_name).to_protobuf())
                        } else {
                            None
                        }
                    })
                })
                .collect()
        };

        debug_assert_eq!(additional_columns.len(), 2);

        for col in &self.columns {
            match col
                .column_desc
                .as_ref()
                .unwrap()
                .get_additional_column_type()
            {
                Ok(AdditionalColumnType::Partition | AdditionalColumnType::Filename) => {
                    columns_exist[0] = true;
                }
                Ok(AdditionalColumnType::Offset) => {
                    columns_exist[1] = true;
                }
                _ => (),
            }
        }

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();

        for (existed, c) in columns_exist.iter().zip_eq_fast(&additional_columns) {
            if !existed {
                columns.push(SourceColumnDesc::hidden_addition_col_from_column_desc(
                    &ColumnDesc::from(c.column_desc.as_ref().unwrap()),
                ));
            }
        }

        if let Some(row_id_index) = self.row_id_index {
            columns[row_id_index].column_type = SourceColumnType::RowId;
        }
        for pk_index in &self.pk_indices {
            columns[*pk_index].is_pk = true;
        }
        columns
    }

    pub fn build(self) -> Result<SourceDesc> {
        let columns = self.column_catalogs_to_source_column_descs();

        let psrser_config = SpecificParserConfig::new(&self.source_info, &self.with_properties)?;

        let source = ConnectorSource::new(
            self.with_properties,
            columns.clone(),
            self.connector_message_buffer_size,
            psrser_config,
        )?;

        Ok(SourceDesc {
            source,
            columns,
            metrics: self.metrics,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    pub fn build_fs_source_desc(&self) -> Result<FsSourceDesc> {
        let parser_config = SpecificParserConfig::new(&self.source_info, &self.with_properties)?;

        match (
            &parser_config.protocol_config,
            &parser_config.encoding_config,
        ) {
            (
                ProtocolProperties::Plain,
                EncodingProperties::Csv(_) | EncodingProperties::Json(_),
            ) => {}
            (format, encode) => {
                return Err(RwError::from(ProtocolError(format!(
                    "Unsupported combination of format {:?} and encode {:?}",
                    format, encode
                ))));
            }
        }

        let columns = self.column_catalogs_to_source_column_descs();

        let source = FsConnectorSource::new(
            self.with_properties.clone(),
            columns.clone(),
            self.connector_params
                .connector_client
                .as_ref()
                .map(|client| client.endpoint().clone()),
            parser_config,
        )?;

        Ok(FsSourceDesc {
            source,
            columns,
            metrics: self.metrics.clone(),
        })
    }
}

pub mod test_utils {
    use std::collections::HashMap;

    use risingwave_common::catalog::{ColumnDesc, Schema};
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::ColumnCatalog;

    use super::{SourceDescBuilder, DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE};

    pub fn create_source_desc_builder(
        schema: &Schema,
        row_id_index: Option<usize>,
        source_info: StreamSourceInfo,
        with_properties: HashMap<String, String>,
        pk_indices: Vec<usize>,
    ) -> SourceDescBuilder {
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnCatalog {
                column_desc: Some(
                    ColumnDesc::named(
                        f.name.clone(),
                        (i as i32).into(), // use column index as column id
                        f.data_type.clone(),
                    )
                    .to_protobuf(),
                ),
                is_hidden: false,
            })
            .collect();
        SourceDescBuilder {
            columns,
            metrics: Default::default(),
            row_id_index,
            with_properties,
            source_info,
            connector_params: Default::default(),
            connector_message_buffer_size: DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
            pk_indices,
        }
    }
}
