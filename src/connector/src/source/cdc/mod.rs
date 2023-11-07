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

pub mod enumerator;
pub mod source;
pub mod split;
use std::collections::HashMap;
use std::marker::PhantomData;

pub use enumerator::*;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_pb::catalog::PbSource;
use risingwave_pb::connector_service::{PbSourceType, PbTableSchema, SourceType, TableSchema};
pub use source::*;
pub use split::*;

use crate::source::{SourceProperties, SplitImpl, TryFromHashmap};
use crate::{for_all_classified_sources, impl_cdc_source_type};

pub const CDC_CONNECTOR_NAME_SUFFIX: &str = "-cdc";
pub const CDC_SNAPSHOT_MODE_KEY: &str = "debezium.snapshot.mode";
pub const CDC_SNAPSHOT_BACKFILL: &str = "rw_cdc_backfill";
pub const CDC_SHARING_MODE_KEY: &str = "rw.sharing.mode.enable";

pub const MYSQL_CDC_CONNECTOR: &str = Mysql::CDC_CONNECTOR_NAME;
pub const POSTGRES_CDC_CONNECTOR: &str = Postgres::CDC_CONNECTOR_NAME;
pub const CITUS_CDC_CONNECTOR: &str = Citus::CDC_CONNECTOR_NAME;

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
            CdcSourceType::Unspecified => "Unspecified",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CdcProperties<T: CdcSourceTypeTrait> {
    /// Properties specified in the WITH clause by user
    pub props: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: TableSchema,

    pub _phantom: PhantomData<T>,
}

impl<T: CdcSourceTypeTrait> TryFromHashmap for CdcProperties<T> {
    fn try_from_hashmap(props: HashMap<String, String>) -> anyhow::Result<Self> {
        Ok(CdcProperties {
            props,
            table_schema: Default::default(),
            _phantom: PhantomData,
        })
    }
}

impl<T: CdcSourceTypeTrait> SourceProperties for CdcProperties<T>
where
    DebeziumCdcSplit<T>: TryFrom<SplitImpl, Error = anyhow::Error> + Into<SplitImpl>,
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
                .cloned()
                .collect(),
            pk_indices,
        };
        self.table_schema = table_schema;
    }
}

impl<T: CdcSourceTypeTrait> CdcProperties<T> {
    pub fn get_source_type_pb(&self) -> SourceType {
        SourceType::from(T::source_type())
    }

    pub fn schema(&self) -> Schema {
        Schema {
            fields: self
                .table_schema
                .columns
                .iter()
                .map(ColumnDesc::from)
                .map(Field::from)
                .collect(),
        }
    }
}
