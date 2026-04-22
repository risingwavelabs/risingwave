// Copyright 2026 RisingWave Labs
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
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::{Context, anyhow};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    NullOrder, SortDirection, SortField, SortOrder, TableProperties, Transform,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableCreation};
use itertools::Itertools;
use regex::Regex;
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    self, DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
    Schema as ArrowSchema,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, Schema};
use url::Url;

use super::{IcebergConfig, PARTITION_DATA_ID_START, SinkError};
use crate::sink::{Result, SinkParam};

static ORDER_KEY_COLUMN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid order key regex"));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergOrderKeyField {
    pub column: String,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

impl IcebergOrderKeyField {
    fn default_null_order(direction: SortDirection) -> NullOrder {
        match direction {
            SortDirection::Ascending => NullOrder::First,
            SortDirection::Descending => NullOrder::Last,
        }
    }
}

pub(super) async fn create_and_validate_table_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<Table> {
    if config.create_table_if_not_exists {
        create_table_if_not_exists_impl(config, param).await?;
    }

    let table = config
        .load_table()
        .await
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    let sink_schema = param.schema();
    let iceberg_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    try_matches_arrow_schema(&sink_schema, &iceberg_arrow_schema)
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    Ok(table)
}

pub(super) async fn create_table_if_not_exists_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<()> {
    let catalog = config.create_catalog().await?;
    let namespace = if let Some(database_name) = config.table.database_name() {
        let namespace = NamespaceIdent::new(database_name.to_owned());
        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
        {
            catalog
                .create_namespace(&namespace, HashMap::default())
                .await
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context("failed to create iceberg namespace")?;
        }
        namespace
    } else {
        bail!("database name must be set if you want to create table")
    };

    let table_id = config
        .full_table_name()
        .context("Unable to parse table name")?;
    if !catalog
        .table_exists(&table_id)
        .await
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
    {
        let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
        // convert risingwave schema -> arrow schema -> iceberg schema
        let arrow_fields = param
            .columns
            .iter()
            .map(|column| {
                Ok(iceberg_create_table_arrow_convert
                    .to_arrow_field(&column.name, &column.data_type)
                    .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                    .context(format!(
                        "failed to convert {}: {} to arrow type",
                        &column.name, &column.data_type
                    ))?)
            })
            .collect::<Result<Vec<ArrowField>>>()?;
        let arrow_schema = arrow_schema_iceberg::Schema::new(arrow_fields);
        let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))
            .context("failed to convert arrow schema to iceberg schema")?;

        let location = {
            let mut names = namespace.clone().inner();
            names.push(config.table.table_name().to_owned());
            match &config.common.warehouse_path {
                Some(warehouse_path) => {
                    let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                    // BigLake catalog federation uses bq:// prefix for BigQuery-managed Iceberg tables
                    let is_bq_catalog_federation = warehouse_path.starts_with("bq://");
                    let url = Url::parse(warehouse_path);
                    if url.is_err() || is_s3_tables || is_bq_catalog_federation {
                        // For rest catalog, the warehouse_path could be a warehouse name.
                        // In this case, we should specify the location when creating a table.
                        if config.common.catalog_type() == "rest"
                            || config.common.catalog_type() == "rest_rust"
                        {
                            None
                        } else {
                            bail!(format!("Invalid warehouse path: {}", warehouse_path))
                        }
                    } else if warehouse_path.ends_with('/') {
                        Some(format!("{}{}", warehouse_path, names.join("/")))
                    } else {
                        Some(format!("{}/{}", warehouse_path, names.join("/")))
                    }
                }
                None => None,
            }
        };

        let partition_spec = match &config.partition_by {
            Some(partition_by) => {
                let mut partition_fields = Vec::<UnboundPartitionField>::new();
                for (i, (column, transform)) in parse_partition_by_exprs(partition_by.clone())?
                    .into_iter()
                    .enumerate()
                {
                    match iceberg_schema.field_id_by_name(&column) {
                        Some(id) => partition_fields.push(
                            UnboundPartitionField::builder()
                                .source_id(id)
                                .transform(transform)
                                .name(format!("_p_{}", column))
                                .field_id(PARTITION_DATA_ID_START + i as i32)
                                .build(),
                        ),
                        None => bail!(format!(
                            "Partition source column does not exist in schema: {}",
                            column
                        )),
                    };
                }
                Some(
                    UnboundPartitionSpec::builder()
                        .with_spec_id(0)
                        .add_partition_fields(partition_fields)
                        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                        .context("failed to add partition columns")?
                        .build(),
                )
            }
            None => None,
        };

        let sort_order = match &config.order_key {
            Some(order_key) => Some(build_sort_order(order_key, &iceberg_schema)?),
            None => None,
        };

        // Put format-version into table properties, because catalog like jdbc extract format-version from table properties.
        let properties = HashMap::from([(
            TableProperties::PROPERTY_FORMAT_VERSION.to_owned(),
            (config.format_version as u8).to_string(),
        )]);

        let table_creation_builder = TableCreation::builder()
            .name(config.table.table_name().to_owned())
            .schema(iceberg_schema)
            .format_version(config.table_format_version())
            .properties(properties);

        let table_creation = match (location, partition_spec, sort_order) {
            (Some(location), Some(partition_spec), Some(sort_order)) => table_creation_builder
                .location(location)
                .partition_spec(partition_spec)
                .sort_order(sort_order)
                .build(),
            (Some(location), Some(partition_spec), None) => table_creation_builder
                .location(location)
                .partition_spec(partition_spec)
                .build(),
            (Some(location), None, Some(sort_order)) => table_creation_builder
                .location(location)
                .sort_order(sort_order)
                .build(),
            (Some(location), None, None) => table_creation_builder.location(location).build(),
            (None, Some(partition_spec), Some(sort_order)) => table_creation_builder
                .partition_spec(partition_spec)
                .sort_order(sort_order)
                .build(),
            (None, Some(partition_spec), None) => table_creation_builder
                .partition_spec(partition_spec)
                .build(),
            (None, None, Some(sort_order)) => table_creation_builder.sort_order(sort_order).build(),
            (None, None, None) => table_creation_builder.build(),
        };

        catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))
            .context("failed to create iceberg table")?;
    }
    Ok(())
}

pub async fn sync_iceberg_table_comments(
    config: &IcebergConfig,
    table_comment: Option<&str>,
    columns: &[ColumnDesc],
) -> Result<()> {
    let column_comments = columns
        .iter()
        .map(|column| (column.name.clone(), column.description.clone()))
        .collect();
    config
        .common
        .sync_table_comments(
            &config.table,
            &config.java_catalog_props,
            table_comment,
            &column_comments,
        )
        .await
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))
}

const MAP_KEY: &str = "key";
const MAP_VALUE: &str = "value";

fn get_fields<'a>(
    our_field_type: &'a risingwave_common::types::DataType,
    data_type: &ArrowDataType,
    schema_fields: &mut HashMap<&'a str, &'a risingwave_common::types::DataType>,
) -> Option<ArrowFields> {
    match data_type {
        ArrowDataType::Struct(fields) => {
            match our_field_type {
                risingwave_common::types::DataType::Struct(struct_fields) => {
                    struct_fields.iter().for_each(|(name, data_type)| {
                        let res = schema_fields.insert(name, data_type);
                        // This assert is to make sure there is no duplicate field name in the schema.
                        assert!(res.is_none())
                    });
                }
                risingwave_common::types::DataType::Map(map_fields) => {
                    schema_fields.insert(MAP_KEY, map_fields.key());
                    schema_fields.insert(MAP_VALUE, map_fields.value());
                }
                risingwave_common::types::DataType::List(list) => {
                    list.elem()
                        .as_struct()
                        .iter()
                        .for_each(|(name, data_type)| {
                            let res = schema_fields.insert(name, data_type);
                            // This assert is to make sure there is no duplicate field name in the schema.
                            assert!(res.is_none())
                        });
                }
                _ => {}
            };
            Some(fields.clone())
        }
        ArrowDataType::List(field) | ArrowDataType::Map(field, _) => {
            get_fields(our_field_type, field.data_type(), schema_fields)
        }
        _ => None, // not a supported complex type and unlikely to show up
    }
}

fn check_compatibility(
    schema_fields: HashMap<&str, &risingwave_common::types::DataType>,
    fields: &ArrowFields,
) -> anyhow::Result<bool> {
    for arrow_field in fields {
        let our_field_type = schema_fields
            .get(arrow_field.name().as_str())
            .ok_or_else(|| anyhow!("Field {} not found in our schema", arrow_field.name()))?;

        // Iceberg source should be able to read iceberg decimal type.
        let converted_arrow_data_type = IcebergArrowConvert
            .to_arrow_field("", our_field_type)
            .map_err(|e| anyhow!(e))?
            .data_type()
            .clone();

        let compatible = match (&converted_arrow_data_type, arrow_field.data_type()) {
            (ArrowDataType::Decimal128(_, _), ArrowDataType::Decimal128(_, _)) => true,
            (ArrowDataType::Binary, ArrowDataType::LargeBinary) => true,
            (ArrowDataType::LargeBinary, ArrowDataType::Binary) => true,
            (ArrowDataType::List(_), ArrowDataType::List(field))
            | (ArrowDataType::Map(_, _), ArrowDataType::Map(field, _)) => {
                let mut schema_fields = HashMap::new();
                get_fields(our_field_type, field.data_type(), &mut schema_fields)
                    .is_none_or(|fields| check_compatibility(schema_fields, &fields).unwrap())
            }
            // validate nested structs
            (ArrowDataType::Struct(_), ArrowDataType::Struct(fields)) => {
                let mut schema_fields = HashMap::new();
                our_field_type
                    .as_struct()
                    .iter()
                    .for_each(|(name, data_type)| {
                        let res = schema_fields.insert(name, data_type);
                        // This assert is to make sure there is no duplicate field name in the schema.
                        assert!(res.is_none())
                    });
                check_compatibility(schema_fields, fields)?
            }
            // cases where left != right (metadata, field name mismatch)
            //
            // all nested types: in iceberg `field_id` will always be present, but RW doesn't have it:
            // {"PARQUET:field_id": ".."}
            //
            // map: The standard name in arrow is "entries", "key", "value".
            // in iceberg-rs, it's called "key_value"
            (left, right) => left.equals_datatype(right),
        };
        if !compatible {
            bail!(
                "field {}'s type is incompatible\nRisingWave converted data type: {}\niceberg's data type: {}",
                arrow_field.name(),
                converted_arrow_data_type,
                arrow_field.data_type()
            );
        }
    }
    Ok(true)
}

/// Try to match our schema with iceberg schema.
pub fn try_matches_arrow_schema(rw_schema: &Schema, arrow_schema: &ArrowSchema) -> Result<()> {
    if rw_schema.fields.len() != arrow_schema.fields().len() {
        bail!(
            "Schema length mismatch, risingwave is {}, and iceberg is {}",
            rw_schema.fields.len(),
            arrow_schema.fields.len()
        );
    }

    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(field.name.as_str(), &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });

    check_compatibility(schema_fields, &arrow_schema.fields)?;
    Ok(())
}

pub fn parse_partition_by_exprs(
    expr: String,
) -> std::result::Result<Vec<(String, Transform)>, anyhow::Error> {
    // captures column, transform(column), transform(n,column), transform(n, column)
    let re = Regex::new(r"(?<transform>\w+)(\(((?<n>\d+)?(?:,|(,\s)))?(?<field>\w+)\))?").unwrap();
    if !re.is_match(&expr) {
        bail!(format!(
            "Invalid partition fields: {}\nHINT: Supported formats are column, transform(column), transform(n,column), transform(n, column)",
            expr
        ))
    }
    let caps = re.captures_iter(&expr);

    let mut partition_columns = vec![];

    for mat in caps {
        let (column, transform) = if mat.name("n").is_none() && mat.name("field").is_none() {
            (&mat["transform"], Transform::Identity)
        } else {
            let mut func = mat["transform"].to_owned();
            if func == "bucket" || func == "truncate" {
                let n = &mat
                    .name("n")
                    .ok_or_else(|| anyhow!("The `n` must be set with `bucket` and `truncate`"))?
                    .as_str();
                func = format!("{func}[{n}]");
            }
            (
                &mat["field"],
                Transform::from_str(&func)
                    .with_context(|| format!("invalid transform function {}", func))?,
            )
        };
        partition_columns.push((column.to_owned(), transform));
    }
    Ok(partition_columns)
}

pub fn parse_order_key_exprs(
    expr: String,
) -> std::result::Result<Vec<IcebergOrderKeyField>, anyhow::Error> {
    let mut order_keys = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();

    for raw_item in expr.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            bail!("Invalid order key: empty item in `{expr}`");
        }

        let tokens = item.split_whitespace().collect_vec();
        if tokens.is_empty() {
            bail!("Invalid order key item `{item}`");
        }
        if tokens.len() > 4 {
            bail!(
                "Invalid order key item `{item}`\nHINT: Supported format is `column [asc|desc] [nulls first|last]`"
            );
        }

        let column = tokens[0];
        if !ORDER_KEY_COLUMN_RE.is_match(column) {
            bail!(
                "Invalid order key column `{column}`\nHINT: Only plain column names are supported in order_key"
            );
        }
        if !seen_columns.insert(column.to_ascii_lowercase()) {
            bail!("Duplicate column `{column}` in order_key");
        }

        let mut direction = SortDirection::Ascending;
        let mut null_order = None;
        let mut idx = 1;
        while idx < tokens.len() {
            match tokens[idx].to_ascii_lowercase().as_str() {
                "asc" => {
                    direction = SortDirection::Ascending;
                    idx += 1;
                }
                "desc" => {
                    direction = SortDirection::Descending;
                    idx += 1;
                }
                "nulls" => {
                    let order = tokens.get(idx + 1).ok_or_else(|| {
                        anyhow!(
                            "Invalid order key item `{item}`: `NULLS` must be followed by `FIRST` or `LAST`"
                        )
                    })?;
                    null_order = Some(match order.to_ascii_lowercase().as_str() {
                        "first" => NullOrder::First,
                        "last" => NullOrder::Last,
                        _ => bail!(
                            "Invalid order key item `{item}`\nHINT: `NULLS` must be followed by `FIRST` or `LAST`"
                        ),
                    });
                    idx += 2;
                }
                token => {
                    bail!(
                        "Invalid order key token `{token}` in `{item}`\nHINT: Supported format is `column [asc|desc] [nulls first|last]`"
                    );
                }
            }
        }

        order_keys.push(IcebergOrderKeyField {
            column: column.to_owned(),
            direction,
            null_order: null_order
                .unwrap_or_else(|| IcebergOrderKeyField::default_null_order(direction)),
        });
    }

    if order_keys.is_empty() {
        bail!("order_key must not be empty");
    }

    Ok(order_keys)
}

pub fn validate_order_key_columns<'a>(
    order_key: &str,
    columns: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<Vec<IcebergOrderKeyField>, anyhow::Error> {
    let parsed = parse_order_key_exprs(order_key.to_owned())?;
    let columns = columns
        .into_iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for item in &parsed {
        if item.column.starts_with('_') {
            bail!(
                "System column `{}` is not allowed in order_key",
                item.column
            );
        }
        if !columns.contains(&item.column.to_ascii_lowercase()) {
            bail!("Order key column does not exist in schema: {}", item.column);
        }
    }
    Ok(parsed)
}

fn build_sort_order(order_key: &str, schema: &iceberg::spec::Schema) -> Result<SortOrder> {
    let order_fields = validate_order_key_columns(
        order_key,
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str()),
    )?;
    let mut builder = SortOrder::builder();
    for field in order_fields {
        let source_id = schema.field_id_by_name(&field.column).ok_or_else(|| {
            anyhow!(
                "Order key column does not exist in schema: {}",
                field.column
            )
        })?;
        builder.with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(Transform::Identity)
                .direction(field.direction)
                .null_order(field.null_order)
                .build(),
        );
    }
    builder
        .build(schema)
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
}
