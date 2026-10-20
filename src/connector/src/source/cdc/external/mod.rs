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

pub mod mock_external_table;

use std::collections::{BTreeMap, HashMap};

use futures::stream::BoxStream;
use risingwave_common::bail;
use risingwave_common::catalog::{CdcKeyComparison, ColumnDesc, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;
use risingwave_pb::secret::PbSecretRef;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::WithPropertiesExt;
use crate::error::ConnectorResult;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcExternalTableHandler, CdcSerializedNames, CdcSourceType,
    parse_debezium_offset,
};

/// Maps a registry connector to its `PbCdcTableType` variant of the same name.
// TODO(oracle): `PbCdcTableType` has no Oracle value yet; remove the `Oracle` arm once the proto
// adds `CDC_TABLE_TYPE_ORACLE`.
macro_rules! pb_cdc_table_type {
    (Oracle) => {
        PbCdcTableType::Unspecified
    };
    ($cdc_source_type:ident) => {
        PbCdcTableType::$cdc_source_type
    };
}

/// Generates [`ExternalCdcTableType`] with one variant per CDC connector in the registry plus the
/// hardcoded `Unspecified` and `Mock`, together with `from_properties` and the `PbCdcTableType`
/// conversions.
macro_rules! impl_external_cdc_table_type {
    ({$({$cdc_source_type:ident}),*}, {$($_ignore:tt),*}) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum ExternalCdcTableType {
            Unspecified,
            Mock,
            $(
                $cdc_source_type,
            )*
        }

        impl ExternalCdcTableType {
            pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
                let connector = with_properties.get_connector().unwrap_or_default();
                match CdcSourceType::from(connector.as_str()) {
                    $(
                        CdcSourceType::$cdc_source_type => Self::$cdc_source_type,
                    )*
                    CdcSourceType::Unspecified => Self::Unspecified,
                }
            }

            /// The registry connector behind this table type, for `dispatch_cdc_source_type!`.
            /// `Mock` has no connector and maps to `Unspecified`.
            pub fn cdc_source_type(&self) -> CdcSourceType {
                match self {
                    $(
                        Self::$cdc_source_type => CdcSourceType::$cdc_source_type,
                    )*
                    Self::Unspecified | Self::Mock => CdcSourceType::Unspecified,
                }
            }
        }

        impl From<ExternalCdcTableType> for PbCdcTableType {
            fn from(cdc_table_type: ExternalCdcTableType) -> Self {
                match cdc_table_type {
                    $(
                        ExternalCdcTableType::$cdc_source_type => pb_cdc_table_type!($cdc_source_type),
                    )*
                    ExternalCdcTableType::Unspecified | ExternalCdcTableType::Mock => Self::Unspecified,
                }
            }
        }

        impl From<PbCdcTableType> for ExternalCdcTableType {
            fn from(cdc_table_type: PbCdcTableType) -> Self {
                #[allow(unreachable_patterns)]
                match cdc_table_type {
                    PbCdcTableType::Unspecified => Self::Unspecified,
                    $(
                        pb_cdc_table_type!($cdc_source_type) => Self::$cdc_source_type,
                    )*
                }
            }
        }
    };
}

crate::for_all_classified_sources!(impl_external_cdc_table_type);

/// Generates [`CdcOffset`] with one variant per CDC connector in the registry, holding that
/// connector's `CdcDebeziumOffsetInterpreter::Offset`, and `get_cdc_offset_parser`. Serde is
/// hand-written because the persisted tag of each variant comes from `CdcSerializedNames`, which
/// `#[serde(rename)]` cannot read.
macro_rules! impl_cdc_offset {
    ({$({$cdc_source_type:ident}),*}, {$($_ignore:tt),*}) => {
        /// Upstream position of a CDC table, compared during backfill and persisted in the
        /// backfill state table as `{"<variant>": <offset>}`.
        #[derive(Debug, Clone, PartialEq, PartialOrd)]
        pub enum CdcOffset {
            $(
                $cdc_source_type(<crate::source::cdc::$cdc_source_type as CdcDebeziumOffsetInterpreter>::Offset),
            )*
        }

        const CDC_OFFSET_VARIANTS: &[&str] = &[
            $(
                <crate::source::cdc::$cdc_source_type as CdcSerializedNames>::CDC_OFFSET_VARIANT,
            )*
        ];

        impl Serialize for CdcOffset {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $(
                        Self::$cdc_source_type(offset) => {
                            let variant = <crate::source::cdc::$cdc_source_type as CdcSerializedNames>::CDC_OFFSET_VARIANT;
                            let index = CDC_OFFSET_VARIANTS.iter().position(|v| *v == variant).unwrap() as u32;
                            serializer.serialize_newtype_variant("CdcOffset", index, variant, offset)
                        }
                    )*
                }
            }
        }

        impl<'de> Deserialize<'de> for CdcOffset {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct CdcOffsetVisitor;

                impl<'de> serde::de::Visitor<'de> for CdcOffsetVisitor {
                    type Value = CdcOffset;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        formatter.write_str("enum CdcOffset")
                    }

                    fn visit_enum<A: serde::de::EnumAccess<'de>>(
                        self,
                        data: A,
                    ) -> Result<Self::Value, A::Error> {
                        use serde::de::VariantAccess;
                        let (variant, access): (String, A::Variant) = data.variant()?;
                        match variant.as_str() {
                            $(
                                variant if variant == <crate::source::cdc::$cdc_source_type as CdcSerializedNames>::CDC_OFFSET_VARIANT => {
                                    Ok(CdcOffset::$cdc_source_type(access.newtype_variant()?))
                                }
                            )*
                            unknown => Err(serde::de::Error::unknown_variant(unknown, CDC_OFFSET_VARIANTS)),
                        }
                    }
                }

                deserializer.deserialize_enum("CdcOffset", CDC_OFFSET_VARIANTS, CdcOffsetVisitor)
            }
        }

        impl ExternalCdcTableType {
            pub fn get_cdc_offset_parser(&self) -> ConnectorResult<CdcOffsetParseFunc> {
                match self {
                    $(
                        Self::$cdc_source_type => Ok(Box::new(|offset| {
                            Ok(CdcOffset::$cdc_source_type(
                                parse_debezium_offset::<crate::source::cdc::$cdc_source_type>(offset)?,
                            ))
                        })),
                    )*
                    Self::Mock => Ok(MockExternalTableReader::get_cdc_offset_parser()),
                    Self::Unspecified => bail!("invalid external table type: {:?}", *self),
                }
            }
        }
    };
}

crate::for_all_classified_sources!(impl_cdc_offset);

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

/// The resume position Debezium reports with every event. The envelope is the same for every
/// connector; `sourceOffset` is connector-specific, so `S` is the connector's own struct, named
/// by `CdcDebeziumOffsetInterpreter::SourceOffset`.
///
/// Example for Postgres:
/// ```json
/// {
///     "sourcePartition": { "server": "RW_CDC_1004" },
///     "sourceOffset": {
///         "last_snapshot_record": false,
///         "lsn": 29973552,
///         "txId": 1046,
///         "ts_usec": 1670826189008456,
///         "snapshot": true
///     },
///     "isHeartbeat": false
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumOffset<S> {
    #[serde(rename = "sourcePartition")]
    pub source_partition: HashMap<String, String>,
    #[serde(rename = "sourceOffset")]
    pub source_offset: S,
    #[serde(rename = "isHeartbeat")]
    pub is_heartbeat: bool,
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader: Sized {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    // Currently, MySQL cdc uses a connection pool to manage connections to MySQL, and other CDC processes do not require the disconnect step for now.

    async fn disconnect(self) -> ConnectorResult<()> {
        Ok(())
    }

    /// Comparison semantics of the given primary key columns, recovered from the live reader for
    /// stream graphs that predate persisted comparisons.
    fn pk_column_comparisons(&self, pk_names: &[String]) -> ConnectorResult<Vec<CdcKeyComparison>> {
        Ok(vec![CdcKeyComparison::Native; pk_names.len()])
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>>;

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

pub struct CdcTableSnapshotSplitOption {
    pub backfill_num_rows_per_split: u64,
    pub backfill_as_even_splits: bool,
    pub backfill_split_pk_column_index: u32,
}

/// Schema of an upstream table as discovered by `CdcExternalTableHandler::connect_external_table`
/// for `CREATE TABLE ... FROM <source>` without explicit columns.
pub trait ExternalTable: Send + Sync {
    fn column_descs(&self) -> &[ColumnDesc];

    fn pk_names(&self) -> &[String];

    /// Comparison semantics of the given primary key columns, in that order, from the metadata
    /// loaded on connect.
    fn pk_column_comparisons(&self, pk_names: &[String]) -> ConnectorResult<Vec<CdcKeyComparison>> {
        Ok(vec![CdcKeyComparison::Native; pk_names.len()])
    }
}

/// Generates [`ExternalTableReaderImpl`] with one variant per CDC connector in the registry,
/// holding that connector's `CdcExternalTableHandler::TableReader`, plus the hardcoded `Mock`,
/// together with its [`ExternalTableReader`] delegation and
/// [`ExternalCdcTableType::create_table_reader`].
macro_rules! impl_external_table_reader_impl {
    ({$({$cdc_source_type:ident}),*}, {$($_ignore:tt),*}) => {
        pub enum ExternalTableReaderImpl {
            $(
                $cdc_source_type(<crate::source::cdc::$cdc_source_type as CdcExternalTableHandler>::TableReader),
            )*
            Mock(MockExternalTableReader),
        }

        impl ExternalTableReader for ExternalTableReaderImpl {
            async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => reader.current_cdc_offset().await,
                    )*
                    Self::Mock(reader) => reader.current_cdc_offset().await,
                }
            }

            async fn disconnect(self) -> ConnectorResult<()> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => reader.disconnect().await,
                    )*
                    Self::Mock(reader) => reader.disconnect().await,
                }
            }

            fn pk_column_comparisons(
                &self,
                pk_names: &[String],
            ) -> ConnectorResult<Vec<CdcKeyComparison>> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => reader.pk_column_comparisons(pk_names),
                    )*
                    Self::Mock(reader) => reader.pk_column_comparisons(pk_names),
                }
            }

            fn snapshot_read(
                &self,
                table_name: SchemaTableName,
                start_pk: Option<OwnedRow>,
                primary_keys: Vec<String>,
                limit: u32,
            ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => {
                            reader.snapshot_read(table_name, start_pk, primary_keys, limit)
                        }
                    )*
                    Self::Mock(reader) => reader.snapshot_read(table_name, start_pk, primary_keys, limit),
                }
            }

            fn get_parallel_cdc_splits(
                &self,
                options: CdcTableSnapshotSplitOption,
            ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => reader.get_parallel_cdc_splits(options),
                    )*
                    Self::Mock(reader) => reader.get_parallel_cdc_splits(options),
                }
            }

            fn split_snapshot_read(
                &self,
                table_name: SchemaTableName,
                left: OwnedRow,
                right: OwnedRow,
                split_columns: Vec<Field>,
            ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
                match self {
                    $(
                        Self::$cdc_source_type(reader) => {
                            reader.split_snapshot_read(table_name, left, right, split_columns)
                        }
                    )*
                    Self::Mock(reader) => reader.split_snapshot_read(table_name, left, right, split_columns),
                }
            }
        }

        impl ExternalTableReaderImpl {
            pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
                match self {
                    $(
                        Self::$cdc_source_type(_) => Box::new(|offset| {
                            Ok(CdcOffset::$cdc_source_type(
                                parse_debezium_offset::<crate::source::cdc::$cdc_source_type>(offset)?,
                            ))
                        }),
                    )*
                    Self::Mock(_) => MockExternalTableReader::get_cdc_offset_parser(),
                }
            }
        }

        impl ExternalCdcTableType {
            pub async fn create_table_reader(
                &self,
                config: ExternalTableConfig,
                schema: Schema,
                pk_indices: Vec<usize>,
                schema_table_name: SchemaTableName,
                table_id: u32,
            ) -> ConnectorResult<ExternalTableReaderImpl> {
                match self {
                    $(
                        Self::$cdc_source_type => Ok(ExternalTableReaderImpl::$cdc_source_type(
                            <crate::source::cdc::$cdc_source_type as CdcExternalTableHandler>::create_table_reader(
                                config,
                                schema,
                                pk_indices,
                                schema_table_name,
                                table_id,
                            )
                            .await?,
                        )),
                    )*
                    Self::Mock => Ok(ExternalTableReaderImpl::Mock(MockExternalTableReader::new())),
                    Self::Unspecified => bail!("invalid external table type: {:?}", *self),
                }
            }
        }
    };
}

crate::for_all_classified_sources!(impl_external_table_reader_impl);

/// Connection options of the upstream table, from the CDC table's WITH options. The named
/// fields are what every connector needs; the connector-specific rest is `specific`, kept as the
/// raw options until a connector parses them into its own struct with [`Self::with_specific`].
#[derive(Debug, Default, Clone, Deserialize)]
pub struct ExternalTableConfig<S = BTreeMap<String, String>> {
    pub connector: String,

    #[serde(rename = "hostname")]
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    #[serde(rename = "database.name")]
    pub database: String,
    #[serde(rename = "schema.name", default = "Default::default")]
    pub schema: String,
    #[serde(rename = "table.name")]
    pub table: String,

    #[serde(flatten)]
    pub specific: S,
}

impl ExternalTableConfig {
    pub fn try_from_btreemap(
        connect_properties: BTreeMap<String, String>,
        secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<Self> {
        let options_with_secret =
            LocalSecretManager::global().fill_secrets(connect_properties, secret_refs)?;
        let json_value = serde_json::to_value(options_with_secret)?;
        let config = serde_json::from_value::<ExternalTableConfig>(json_value)?;
        Ok(config)
    }

    /// Parses the connector-specific options into the connector's own struct `S`.
    pub fn with_specific<S: DeserializeOwned>(self) -> ConnectorResult<ExternalTableConfig<S>> {
        let specific = serde_json::from_value(serde_json::to_value(self.specific)?)?;
        Ok(ExternalTableConfig {
            connector: self.connector,
            host: self.host,
            port: self.port,
            username: self.username,
            password: self.password,
            database: self.database,
            schema: self.schema,
            table: self.table,
            specific,
        })
    }
}

pub const CDC_TABLE_SPLIT_ID_START: i64 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::cdc::mysql::MysqlOffset;
    use crate::source::cdc::postgres::PostgresOffset;
    use crate::source::cdc::sql_server::SqlServerOffset;

    #[test]
    fn test_cdc_offset_persisted_variants() {
        let mut variants = CDC_OFFSET_VARIANTS.to_vec();
        variants.sort_unstable();
        assert_eq!(
            variants,
            [
                "Citus",
                "Mongodb",
                "MySql",
                "Oracle",
                "Postgres",
                "SqlServer"
            ]
        );

        // JSON as written into the CDC backfill state table before `CdcOffset` was generated.
        for (persisted, expected) in [
            (
                r#"{"MySql":{"filename":"binlog.000001","position":4}}"#,
                CdcOffset::Mysql(MysqlOffset::new("binlog.000001".to_owned(), 4)),
            ),
            (
                r#"{"Postgres":{"txid":1046,"lsn":29973552,"lsn_commit":null,"lsn_proc":null}}"#,
                CdcOffset::Postgres(PostgresOffset {
                    txid: 1046,
                    lsn: 29973552,
                    lsn_commit: None,
                    lsn_proc: None,
                }),
            ),
            (
                r#"{"SqlServer":{"change_lsn":"00000027:00000ac0:0001","commit_lsn":"00000027:00000ac0:0002"}}"#,
                CdcOffset::SqlServer(SqlServerOffset {
                    change_lsn: "00000027:00000ac0:0001".to_owned(),
                    commit_lsn: "00000027:00000ac0:0002".to_owned(),
                }),
            ),
        ] {
            let restored: CdcOffset = serde_json::from_str(persisted).unwrap();
            assert_eq!(restored, expected);
            assert_eq!(serde_json::to_string(&restored).unwrap(), persisted);
        }

        assert!(serde_json::from_str::<CdcOffset>(r#"{"Oracle":{}}"#).is_err());
    }
}
