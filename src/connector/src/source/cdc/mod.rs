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
use paste::paste;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_pb::connector_service::{PbSourceType, PbTableSchema, SourceType, TableSchema};
pub use source::*;
pub use split::*;

use crate::impl_cdc_source_type;
use crate::source::ConnectorProperties;

pub const CDC_CONNECTOR_NAME_SUFFIX: &str = "-cdc";
pub const CDC_SNAPSHOT_MODE_KEY: &str = "debezium.snapshot.mode";
pub const CDC_LATEST_OFFSET_MODE: &str = "rw_cdc_backfill";

pub const MYSQL_CDC_CONNECTOR: &str = Mysql::CDC_CONNECTOR_NAME;
pub const POSTGRES_CDC_CONNECTOR: &str = Postgres::CDC_CONNECTOR_NAME;
pub const CITUS_CDC_CONNECTOR: &str = Citus::CDC_CONNECTOR_NAME;

pub trait CdcSourceTypeTrait: Send + Sync + Clone + 'static {
    const CDC_CONNECTOR_NAME: &'static str;
    fn source_type() -> CdcSourceType;
}

impl_cdc_source_type!({ Mysql, "mysql" }, { Postgres, "postgres" }, { Citus, "citus" });

#[derive(Clone, Debug, Default)]
pub struct CdcProperties<T: CdcSourceTypeTrait> {
    /// Properties specified in the WITH clause by user
    pub props: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: TableSchema,

    pub _phantom: PhantomData<T>,
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
