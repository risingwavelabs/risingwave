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

use risingwave_common::bail;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::PbStreamSourceInfo;
use risingwave_pb::plan_common::PbColumnCatalog;

#[expect(deprecated)]
use super::fs_reader::LegacyFsSourceReader;
use super::reader::SourceReader;
use crate::error::ConnectorResult;
use crate::parser::additional_columns::{
    derive_pulsar_message_id_data_column, source_add_partition_offset_cols,
};
use crate::parser::{EncodingProperties, ProtocolProperties, SpecificParserConfig};
use crate::source::monitor::SourceMetrics;
use crate::source::{SourceColumnDesc, SourceColumnType, UPSTREAM_SOURCE_KEY};
use crate::{WithOptionsSecResolved, WithPropertiesExt};

pub const DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 16;

/// `SourceDesc` describes a stream source.
#[derive(Debug, Clone)]
pub struct SourceDesc {
    pub source_info: PbStreamSourceInfo,
    pub source: SourceReader,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

impl SourceDesc {
    pub fn update_reader(
        &mut self,
        props_plaintext: HashMap<String, String>,
    ) -> ConnectorResult<()> {
        let props_wrapper =
            WithOptionsSecResolved::without_secrets(props_plaintext.into_iter().collect());
        let parser_config = SpecificParserConfig::new(&self.source_info, &props_wrapper)?;

        let reader = SourceReader::new(
            props_wrapper,
            self.columns.clone(),
            self.source.connector_message_buffer_size,
            parser_config,
        )?;
        self.source = reader;
        Ok(())
    }
}

/// `FsSourceDesc` describes a stream source.
#[deprecated = "will be replaced by new fs source (list + fetch)"]
#[expect(deprecated)]
#[derive(Debug)]
pub struct LegacyFsSourceDesc {
    pub source: LegacyFsSourceReader,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
}

#[derive(Clone)]
pub struct SourceDescBuilder {
    columns: Vec<ColumnCatalog>,
    metrics: Arc<SourceMetrics>,
    row_id_index: Option<usize>,
    with_properties: WithOptionsSecResolved,
    source_info: PbStreamSourceInfo,
    connector_message_buffer_size: usize,
    pk_indices: Vec<usize>,
}

impl SourceDescBuilder {
    pub fn new(
        columns: Vec<PbColumnCatalog>,
        metrics: Arc<SourceMetrics>,
        row_id_index: Option<usize>,
        with_properties: WithOptionsSecResolved,
        source_info: PbStreamSourceInfo,
        connector_message_buffer_size: usize,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            columns: columns.into_iter().map(ColumnCatalog::from).collect(),
            metrics,
            row_id_index,
            with_properties,
            source_info,
            connector_message_buffer_size,
            pk_indices,
        }
    }

    /// This function builds `SourceColumnDesc` from `ColumnCatalog`, and handle the creation
    /// of hidden columns like partition/file, offset that are not specified by user.
    pub fn column_catalogs_to_source_column_descs(&self) -> Vec<SourceColumnDesc> {
        let connector_name = self
            .with_properties
            .get(UPSTREAM_SOURCE_KEY)
            .map(|s| s.to_lowercase())
            .unwrap();
        let (columns_exist, additional_columns) = {
            let (mut columns_exist, mut additional_columns) =
                source_add_partition_offset_cols(&self.columns, &connector_name, false);

            // add `message_id_data` column for pulsar source, which is used for ack message
            if self.with_properties.is_pulsar_connector() {
                derive_pulsar_message_id_data_column(
                    &connector_name,
                    &mut columns_exist,
                    &mut additional_columns,
                );
            }
            (columns_exist, additional_columns)
        };

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| {
                let mut desc = SourceColumnDesc::from(&c.column_desc);
                desc.is_hidden_addition_col = c.is_hidden;
                desc
            })
            .collect();

        // currently iceberg uses other columns. See `extract_iceberg_columns`
        // TODO: unify logic.
        if connector_name != "iceberg" {
            for (existed, c) in columns_exist.iter().zip_eq_fast(&additional_columns) {
                if !existed {
                    columns.push(SourceColumnDesc::hidden_addition_col_from_column_desc(c));
                }
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

    pub fn build(self) -> ConnectorResult<SourceDesc> {
        let columns = self.column_catalogs_to_source_column_descs();

        let parser_config = SpecificParserConfig::new(&self.source_info, &self.with_properties)?;

        let source = SourceReader::new(
            self.with_properties,
            columns.clone(),
            self.connector_message_buffer_size,
            parser_config,
        )?;

        Ok(SourceDesc {
            source,
            columns,
            metrics: self.metrics,
            source_info: self.source_info,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    #[deprecated = "will be replaced by new fs source (list + fetch)"]
    #[expect(deprecated)]
    pub fn build_fs_source_desc(&self) -> ConnectorResult<LegacyFsSourceDesc> {
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
                bail!(
                    "Unsupported combination of format {:?} and encode {:?}",
                    format,
                    encode,
                );
            }
        }

        let columns = self.column_catalogs_to_source_column_descs();

        let source = LegacyFsSourceReader::new(
            self.with_properties.clone(),
            columns.clone(),
            parser_config,
        )?;

        Ok(LegacyFsSourceDesc {
            source,
            columns,
            metrics: self.metrics.clone(),
        })
    }

    pub fn with_properties(&self) -> WithOptionsSecResolved {
        self.with_properties.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;
    use risingwave_pb::plan_common::additional_column::ColumnType;

    use super::*;
    use crate::parser::additional_columns::build_additional_column_desc;

    #[test]
    fn preserves_hidden_additional_columns_from_catalog() {
        let user_column =
            ColumnCatalog::visible(ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32));
        let hidden_partition = ColumnCatalog::hidden(
            build_additional_column_desc(
                ColumnId::new(2),
                "kafka",
                "partition",
                None,
                None,
                None,
                false,
                false,
            )
            .unwrap(),
        );
        let hidden_offset = ColumnCatalog::hidden(
            build_additional_column_desc(
                ColumnId::new(3),
                "kafka",
                "offset",
                None,
                None,
                None,
                false,
                false,
            )
            .unwrap(),
        );

        let builder = SourceDescBuilder::new(
            vec![
                user_column.to_protobuf(),
                hidden_partition.to_protobuf(),
                hidden_offset.to_protobuf(),
            ],
            Arc::new(SourceMetrics::default()),
            None,
            WithOptionsSecResolved::without_secrets(BTreeMap::from([(
                UPSTREAM_SOURCE_KEY.to_owned(),
                "kafka".to_owned(),
            )])),
            PbStreamSourceInfo::default(),
            DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
            vec![],
        );

        let columns = builder.column_catalogs_to_source_column_descs();
        let hidden_partition = columns
            .iter()
            .find(|desc| {
                matches!(
                    desc.additional_column.column_type,
                    Some(ColumnType::Partition(_))
                )
            })
            .unwrap();
        let hidden_offset = columns
            .iter()
            .find(|desc| {
                matches!(
                    desc.additional_column.column_type,
                    Some(ColumnType::Offset(_))
                )
            })
            .unwrap();

        assert!(hidden_partition.is_hidden_addition_col);
        assert!(!hidden_partition.is_visible());
        assert!(hidden_offset.is_hidden_addition_col);
        assert!(!hidden_offset.is_visible());
    }
}

pub mod test_utils {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Schema};
    use risingwave_pb::catalog::StreamSourceInfo;

    use super::{DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE, SourceDescBuilder};

    pub fn create_source_desc_builder(
        schema: &Schema,
        row_id_index: Option<usize>,
        source_info: StreamSourceInfo,
        with_properties: BTreeMap<String, String>,
        pk_indices: Vec<usize>,
    ) -> SourceDescBuilder {
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnCatalog::visible(ColumnDesc::named(
                    f.name.clone(),
                    (i as i32).into(), // use column index as column id
                    f.data_type.clone(),
                ))
            })
            .collect();
        let options_with_secret = crate::WithOptionsSecResolved::without_secrets(with_properties);
        SourceDescBuilder {
            columns,
            metrics: Default::default(),
            row_id_index,
            with_properties: options_with_secret,
            source_info,
            connector_message_buffer_size: DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
            pk_indices,
        }
    }
}
