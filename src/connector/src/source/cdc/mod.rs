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
use risingwave_pb::connector_service::{PbSourceType, SourceType, TableSchema};
pub use source::*;
pub use split::*;

pub const MYSQL_CDC_CONNECTOR: &str = Mysql::CDC_CONNECTOR_NAME;
pub const POSTGRES_CDC_CONNECTOR: &str = Postgres::CDC_CONNECTOR_NAME;
pub const CITUS_CDC_CONNECTOR: &str = Citus::CDC_CONNECTOR_NAME;

pub trait CdcSourceTypeTrait: Send + Sync + Clone + 'static {
    const CDC_CONNECTOR_NAME: &'static str;
    fn source_type() -> CdcSourceType;
}

macro_rules! impl_cdc_source_type {
    ($({$source_type:ident, $name:expr }),*) => {
        $(
            paste!{
                #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
                pub struct $source_type;
                impl CdcSourceTypeTrait for $source_type {
                    const CDC_CONNECTOR_NAME: &'static str = concat!($name, "-cdc");
                    fn source_type() -> CdcSourceType {
                        CdcSourceType::$source_type
                    }
                }

                pub type [< $source_type DebeziumSplitEnumerator >] = DebeziumSplitEnumerator<$source_type>;
            }
        )*

        pub enum CdcSourceType {
            $(
                $source_type,
            )*
        }

        impl From<PbSourceType> for CdcSourceType {
            fn from(value: PbSourceType) -> Self {
                match value {
                    PbSourceType::Unspecified => unreachable!(),
                    $(
                        PbSourceType::$source_type => CdcSourceType::$source_type,
                    )*
                }
            }
        }

        impl From<CdcSourceType> for PbSourceType {
            fn from(this: CdcSourceType) -> PbSourceType {
                match this {
                    $(
                        CdcSourceType::$source_type => PbSourceType::$source_type,
                    )*
                }
            }
        }
    }
}

impl_cdc_source_type!({ Mysql, "mysql" }, { Postgres, "postgres" }, { Citus, "citus" });

#[derive(Clone, Debug, Default)]
pub struct CdcProperties<T: CdcSourceTypeTrait> {
    /// Properties specified in the WITH clause by user
    pub props: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: Option<TableSchema>,

    pub _phantom: PhantomData<T>,
}

impl<T: CdcSourceTypeTrait> CdcProperties<T> {
    pub fn get_source_type_pb(&self) -> SourceType {
        SourceType::from(T::source_type())
    }
}
