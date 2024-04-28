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

pub mod enumerator;
pub mod external;
pub mod jni_source;
pub mod source;
pub mod split;

use std::collections::HashMap;
use std::marker::PhantomData;

pub use enumerator::*;
use itertools::Itertools;
use risingwave_pb::catalog::PbSource;
use risingwave_pb::connector_service::{PbSourceType, PbTableSchema, SourceType, TableSchema};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::ExternalTableDesc;
use simd_json::prelude::ArrayTrait;
pub use source::*;

use crate::error::ConnectorResult;
use crate::source::{SourceProperties, SplitImpl, TryFromHashmap};
use crate::{for_all_classified_sources, impl_cdc_source_type};

pub const CDC_CONNECTOR_NAME_SUFFIX: &str = "-cdc";
pub const CDC_SNAPSHOT_MODE_KEY: &str = "debezium.snapshot.mode";
pub const CDC_SNAPSHOT_BACKFILL: &str = "rw_cdc_backfill";
pub const CDC_SHARING_MODE_KEY: &str = "rw.sharing.mode.enable";
// User can set snapshot='false' to disable cdc backfill
pub const CDC_BACKFILL_ENABLE_KEY: &str = "snapshot";
// We enable transaction for shared cdc source by default
pub const CDC_TRANSACTIONAL_KEY: &str = "transactional";

pub const MYSQL_CDC_CONNECTOR: &str = Mysql::CDC_CONNECTOR_NAME;
pub const POSTGRES_CDC_CONNECTOR: &str = Postgres::CDC_CONNECTOR_NAME;
pub const CITUS_CDC_CONNECTOR: &str = Citus::CDC_CONNECTOR_NAME;
pub const MONGODB_CDC_CONNECTOR: &str = Mongodb::CDC_CONNECTOR_NAME;

pub trait CdcSourceTypeTrait: Send + Sync + Clone + 'static {
    const CDC_CONNECTOR_NAME: &'static str;
    fn source_type() -> CdcSourceType;
}

for_all_classified_sources!(impl_cdc_source_type);

impl<'a> From<&'a str> for CdcSourceType {
    fn from(name: &'a str) -> Self {
        match name {
            MYSQL_CDC_CONNECTOR => CdcSourceType::Mysql,
            POSTGRES_CDC_CONNECTOR => CdcSourceType::Postgres,
            CITUS_CDC_CONNECTOR => CdcSourceType::Citus,
            MONGODB_CDC_CONNECTOR => CdcSourceType::Mongodb,
            _ => CdcSourceType::Unspecified,
        }
    }
}

impl CdcSourceType {
    pub fn as_str_name(&self) -> &str {
        match self {
            CdcSourceType::Mysql => "MySQL",
            CdcSourceType::Postgres => "Postgres",
            CdcSourceType::Citus => "Citus",
            CdcSourceType::Mongodb => "MongoDB",
            CdcSourceType::Unspecified => "Unspecified",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CdcProperties<T: CdcSourceTypeTrait> {
    /// Properties specified in the WITH clause by user
    pub properties: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: TableSchema,

    /// Whether it is created by a cdc source job
    pub is_cdc_source_job: bool,

    /// For validation purpose, mark if the table is a backfill cdc table
    pub is_backfill_table: bool,

    pub _phantom: PhantomData<T>,
}

impl<T: CdcSourceTypeTrait> TryFromHashmap for CdcProperties<T> {
    fn try_from_hashmap(
        properties: HashMap<String, String>,
        _deny_unknown_fields: bool,
    ) -> ConnectorResult<Self> {
        let is_share_source = properties
            .get(CDC_SHARING_MODE_KEY)
            .is_some_and(|v| v == "true");
        Ok(CdcProperties {
            properties,
            table_schema: Default::default(),
            // TODO(siyuan): use serde to deserialize input hashmap
            is_cdc_source_job: is_share_source,
            is_backfill_table: false,
            _phantom: PhantomData,
        })
    }
}

impl<T: CdcSourceTypeTrait> SourceProperties for CdcProperties<T>
where
    DebeziumCdcSplit<T>: TryFrom<SplitImpl, Error = crate::error::ConnectorError> + Into<SplitImpl>,
    DebeziumSplitEnumerator<T>: ListCdcSplits<CdcSourceType = T>,
{
    type Split = DebeziumCdcSplit<T>;
    type SplitEnumerator = DebeziumSplitEnumerator<T>;
    type SplitReader = CdcSplitReader<T>;

    const SOURCE_NAME: &'static str = T::CDC_CONNECTOR_NAME;

    fn init_from_pb_source(&mut self, source: &PbSource) {
        let pk_indices = source
            .pk_column_ids
            .iter()
            .map(|&id| {
                source
                    .columns
                    .iter()
                    .position(|col| col.column_desc.as_ref().unwrap().column_id == id)
                    .unwrap() as u32
            })
            .collect_vec();

        let table_schema = PbTableSchema {
            columns: source
                .columns
                .iter()
                .flat_map(|col| &col.column_desc)
                .filter(|col| {
                    !matches!(
                        col.generated_or_default_column,
                        Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
                    )
                })
                .cloned()
                .collect(),
            pk_indices,
        };
        self.table_schema = table_schema;
        if let Some(info) = source.info.as_ref() {
            self.is_cdc_source_job = info.is_shared();
        }
    }

    fn init_from_pb_cdc_table_desc(&mut self, table_desc: &ExternalTableDesc) {
        let properties: HashMap<String, String> =
            table_desc.connect_properties.clone().into_iter().collect();

        let table_schema = TableSchema {
            columns: table_desc
                .columns
                .iter()
                .filter(|col| {
                    !matches!(
                        col.generated_or_default_column,
                        Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
                    )
                })
                .cloned()
                .collect(),
            pk_indices: table_desc.stream_key.clone(),
        };

        self.properties = properties;
        self.table_schema = table_schema;
        self.is_cdc_source_job = false;
        self.is_backfill_table = true;
    }
}

impl<T: CdcSourceTypeTrait> crate::source::UnknownFields for CdcProperties<T> {
    fn unknown_fields(&self) -> HashMap<String, String> {
        // FIXME: CDC does not handle unknown fields yet
        HashMap::new()
    }
}

impl<T: CdcSourceTypeTrait> CdcProperties<T> {
    pub fn get_source_type_pb(&self) -> SourceType {
        SourceType::from(T::source_type())
    }
}
