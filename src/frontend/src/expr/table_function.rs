// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use mysql_async::consts::ColumnType as MySqlColumnType;
use mysql_async::prelude::*;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, ScalarImpl, StructType};
use risingwave_connector::source::iceberg::{
    extract_bucket_and_file_name, get_parquet_fields, list_data_directory, new_azblob_operator,
    new_gcs_operator, new_s3_operator, FileScanBackend,
};
pub use risingwave_pb::expr::table_function::PbType as TableFunctionType;
use risingwave_pb::expr::PbTableFunction;
use thiserror_ext::AsReport;
use tokio_postgres::types::Type as TokioPgType;

use super::{infer_type, Expr, ExprImpl, ExprRewriter, Literal, RwResult};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::ErrorCode::BindError;
use crate::utils::FRONTEND_RUNTIME;

const INLINE_ARG_LEN: usize = 6;
const CDC_SOURCE_ARG_LEN: usize = 2;

/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
///
/// See also [`TableFunction`](risingwave_expr::table_function::TableFunction) trait in expr crate
/// and [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem).
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct TableFunction {
    pub args: Vec<ExprImpl>,
    pub return_type: DataType,
    pub function_type: TableFunctionType,
    /// Catalog of user defined table function.
    pub user_defined: Option<Arc<FunctionCatalog>>,
}

impl TableFunction {
    /// Create a `TableFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(func_type: TableFunctionType, mut args: Vec<ExprImpl>) -> RwResult<Self> {
        let return_type = infer_type(func_type.into(), &mut args)?;
        Ok(TableFunction {
            args,
            return_type,
            function_type: func_type,
            user_defined: None,
        })
    }

    /// Create a user-defined `TableFunction`.
    pub fn new_user_defined(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        let FunctionKind::Table = &catalog.kind else {
            panic!("not a table function");
        };
        TableFunction {
            args,
            return_type: catalog.return_type.clone(),
            function_type: TableFunctionType::UserDefined,
            user_defined: Some(catalog),
        }
    }

    /// A special table function which would be transformed into `LogicalFileScan` by `TableFunctionToFileScanRule` in the optimizer.
    /// select * from `file_scan`('parquet', 's3', region, ak, sk, location)
    pub fn new_file_scan(mut args: Vec<ExprImpl>) -> RwResult<Self> {
        let return_type = {
            // arguments:
            // file format e.g. parquet
            // storage type e.g. s3, gcs, azblob
            // For s3: file_scan('parquet', 's3', s3_region, s3_access_key, s3_secret_key, file_location_or_directory)
            // For gcs: file_scan('parquet', 'gcs', credential, file_location_or_directory)
            // For azblob: file_scan('parquet', 'azblob', endpoint, account_name, account_key, file_location)
            let mut eval_args: Vec<String> = vec![];
            for arg in &args {
                if arg.return_type() != DataType::Varchar {
                    return Err(BindError(
                        "file_scan function only accepts string arguments".to_owned(),
                    )
                    .into());
                }
                match arg.try_fold_const() {
                    Some(Ok(value)) => {
                        if value.is_none() {
                            return Err(BindError(
                                "file_scan function does not accept null arguments".to_owned(),
                            )
                            .into());
                        }
                        match value {
                            Some(ScalarImpl::Utf8(s)) => {
                                eval_args.push(s.to_string());
                            }
                            _ => {
                                return Err(BindError(
                                    "file_scan function only accepts string arguments".to_owned(),
                                )
                                .into())
                            }
                        }
                    }
                    Some(Err(err)) => {
                        return Err(err);
                    }
                    None => {
                        return Err(BindError(
                            "file_scan function only accepts constant arguments".to_owned(),
                        )
                        .into());
                    }
                }
            }

            if (eval_args.len() != 4 && eval_args.len() != 6)
                || (eval_args.len() == 4 && !"gcs".eq_ignore_ascii_case(&eval_args[1]))
                || (eval_args.len() == 6
                    && !"s3".eq_ignore_ascii_case(&eval_args[1])
                    && !"azblob".eq_ignore_ascii_case(&eval_args[1]))
            {
                return Err(BindError(
                "file_scan function supports three backends: s3, gcs, and azblob. Their formats are as follows: \n
                    file_scan('parquet', 's3', s3_region, s3_access_key, s3_secret_key, file_location) \n
                    file_scan('parquet', 'gcs', credential, service_account, file_location) \n
                    file_scan('parquet', 'azblob', endpoint, account_name, account_key, file_location)"
                        .to_owned(),
                )
                .into());
            }
            if !"parquet".eq_ignore_ascii_case(&eval_args[0]) {
                return Err(BindError(
                    "file_scan function only accepts 'parquet' as file format".to_owned(),
                )
                .into());
            }

            if !"s3".eq_ignore_ascii_case(&eval_args[1])
                && !"gcs".eq_ignore_ascii_case(&eval_args[1])
                && !"azblob".eq_ignore_ascii_case(&eval_args[1])
            {
                return Err(BindError(
                    "file_scan function only accepts 's3', 'gcs' or 'azblob' as storage type"
                        .to_owned(),
                )
                .into());
            }

            #[cfg(madsim)]
            return Err(crate::error::ErrorCode::BindError(
                "file_scan can't be used in the madsim mode".to_string(),
            )
            .into());

            #[cfg(not(madsim))]
            {
                let (file_scan_backend, input_file_location) =
                    if "s3".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::S3, eval_args[5].clone())
                    } else if "gcs".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::Gcs, eval_args[3].clone())
                    } else if "azblob".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::Azblob, eval_args[5].clone())
                    } else {
                        unreachable!();
                    };
                let op = match file_scan_backend {
                    FileScanBackend::S3 => {
                        let (bucket, _) = extract_bucket_and_file_name(
                            &eval_args[5].clone(),
                            &file_scan_backend,
                        )?;

                        let (s3_region, s3_endpoint) = match eval_args[2].starts_with("http") {
                            true => ("us-east-1".to_owned(), eval_args[2].clone()), /* for minio, hard code region as not used but needed. */
                            false => (
                                eval_args[2].clone(),
                                format!("https://{}.s3.{}.amazonaws.com", bucket, eval_args[2],),
                            ),
                        };
                        new_s3_operator(
                            s3_region.clone(),
                            eval_args[3].clone(),
                            eval_args[4].clone(),
                            bucket.clone(),
                            s3_endpoint.clone(),
                        )?
                    }
                    FileScanBackend::Gcs => {
                        let (bucket, _) =
                            extract_bucket_and_file_name(&input_file_location, &file_scan_backend)?;

                        new_gcs_operator(eval_args[2].clone(), bucket.clone())?
                    }
                    FileScanBackend::Azblob => {
                        let (bucket, _) =
                            extract_bucket_and_file_name(&input_file_location, &file_scan_backend)?;

                        new_azblob_operator(
                            eval_args[2].clone(),
                            eval_args[3].clone(),
                            eval_args[4].clone(),
                            bucket.clone(),
                        )?
                    }
                };
                let files = if input_file_location.ends_with('/') {
                    let files = tokio::task::block_in_place(|| {
                        FRONTEND_RUNTIME.block_on(async {
                            let files = list_data_directory(
                                op.clone(),
                                input_file_location.clone(),
                                &file_scan_backend,
                            )
                            .await?;

                            Ok::<Vec<String>, anyhow::Error>(files)
                        })
                    })?;
                    if files.is_empty() {
                        return Err(BindError(
                            "file_scan function only accepts non-empty directory".to_owned(),
                        )
                        .into());
                    }

                    Some(files)
                } else {
                    None
                };
                let schema = tokio::task::block_in_place(|| {
                    FRONTEND_RUNTIME.block_on(async {
                        let location = match files.as_ref() {
                            Some(files) => files[0].clone(),
                            None => input_file_location.clone(),
                        };
                        let (_, file_name) =
                            extract_bucket_and_file_name(&location, &file_scan_backend)?;

                        let fields = get_parquet_fields(op, file_name).await?;

                        let mut rw_types = vec![];
                        for field in &fields {
                            rw_types.push((
                                field.name().to_string(),
                                IcebergArrowConvert.type_from_field(field)?,
                            ));
                        }

                        Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                            StructType::new(rw_types),
                        ))
                    })
                })?;

                if let Some(files) = files {
                    // if the file location is a directory, we need to remove the last argument and add all files in the directory as arguments
                    match file_scan_backend {
                        FileScanBackend::S3 => args.remove(5),
                        FileScanBackend::Gcs => args.remove(3),
                        FileScanBackend::Azblob => args.remove(5),
                    };
                    for file in files {
                        args.push(ExprImpl::Literal(Box::new(Literal::new(
                            Some(ScalarImpl::Utf8(file.into())),
                            DataType::Varchar,
                        ))));
                    }
                }

                schema
            }
        };

        Ok(TableFunction {
            args,
            return_type,
            function_type: TableFunctionType::FileScan,
            user_defined: None,
        })
    }

    fn handle_postgres_or_mysql_query_args(
        catalog_reader: &CatalogReadGuard,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        args: Vec<ExprImpl>,
        expect_connector_name: &str,
    ) -> RwResult<Vec<ExprImpl>> {
        let cast_args = match args.len() {
            INLINE_ARG_LEN => {
                let mut cast_args = Vec::with_capacity(INLINE_ARG_LEN);
                for arg in args {
                    let arg = arg.cast_implicit(DataType::Varchar)?;
                    cast_args.push(arg);
                }
                cast_args
            }
            CDC_SOURCE_ARG_LEN => {
                let source_name = expr_impl_to_string_fn(&args[0])?;
                let source_catalog = catalog_reader
                    .get_source_by_name(db_name, schema_path, &source_name)?
                    .0;
                if !source_catalog
                    .connector_name()
                    .eq_ignore_ascii_case(expect_connector_name)
                {
                    return Err(BindError(format!("TVF function only accepts `mysql-cdc` and `postgres-cdc` source. Expected: {}, but got: {}", expect_connector_name, source_catalog.connector_name())).into());
                }

                let (props, secret_refs) = source_catalog.with_properties.clone().into_parts();
                let secret_resolved =
                    LocalSecretManager::global().fill_secrets(props, secret_refs)?;

                vec![
                    ExprImpl::literal_varchar(secret_resolved["hostname"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["port"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["username"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["password"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["database.name"].clone()),
                    args.get(1)
                        .unwrap()
                        .clone()
                        .cast_implicit(DataType::Varchar)?,
                ]
            }
            _ => {
                return Err(BindError("postgres_query function and mysql_query function accept either 2 arguments: (cdc_source_name varchar, query varchar) or 6 arguments: (hostname varchar, port varchar, username varchar, password varchar, database_name varchar, query varchar)".to_owned()).into());
            }
        };

        Ok(cast_args)
    }

    pub fn new_postgres_query(
        catalog_reader: &CatalogReadGuard,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        args: Vec<ExprImpl>,
    ) -> RwResult<Self> {
        let args = Self::handle_postgres_or_mysql_query_args(
            catalog_reader,
            db_name,
            schema_path,
            args,
            "postgres-cdc",
        )?;
        let evaled_args = args
            .iter()
            .map(expr_impl_to_string_fn)
            .collect::<RwResult<Vec<_>>>()?;

        #[cfg(madsim)]
        {
            return Err(crate::error::ErrorCode::BindError(
                "postgres_query can't be used in the madsim mode".to_string(),
            )
            .into());
        }

        #[cfg(not(madsim))]
        {
            let schema = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(async {
                    let (client, connection) = tokio_postgres::connect(
                        format!(
                            "host={} port={} user={} password={} dbname={}",
                            evaled_args[0],
                            evaled_args[1],
                            evaled_args[2],
                            evaled_args[3],
                            evaled_args[4]
                        )
                        .as_str(),
                        tokio_postgres::NoTls,
                    )
                    .await?;

                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!(
                                "mysql_query_executor: connection error: {:?}",
                                e.as_report()
                            );
                        }
                    });

                    let statement = client.prepare(evaled_args[5].as_str()).await?;

                    let mut rw_types = vec![];
                    for column in statement.columns() {
                        let name = column.name().to_owned();
                        let data_type = match *column.type_() {
                            TokioPgType::BOOL => DataType::Boolean,
                            TokioPgType::INT2 => DataType::Int16,
                            TokioPgType::INT4 => DataType::Int32,
                            TokioPgType::INT8 => DataType::Int64,
                            TokioPgType::FLOAT4 => DataType::Float32,
                            TokioPgType::FLOAT8 => DataType::Float64,
                            TokioPgType::NUMERIC => DataType::Decimal,
                            TokioPgType::DATE => DataType::Date,
                            TokioPgType::TIME => DataType::Time,
                            TokioPgType::TIMESTAMP => DataType::Timestamp,
                            TokioPgType::TIMESTAMPTZ => DataType::Timestamptz,
                            TokioPgType::TEXT | TokioPgType::VARCHAR => DataType::Varchar,
                            TokioPgType::INTERVAL => DataType::Interval,
                            TokioPgType::JSONB => DataType::Jsonb,
                            TokioPgType::BYTEA => DataType::Bytea,
                            _ => {
                                return Err(crate::error::ErrorCode::BindError(format!(
                                    "unsupported column type: {}",
                                    column.type_()
                                ))
                                .into());
                            }
                        };
                        rw_types.push((name, data_type));
                    }
                    Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                        StructType::new(rw_types),
                    ))
                })
            })?;

            Ok(TableFunction {
                args,
                return_type: schema,
                function_type: TableFunctionType::PostgresQuery,
                user_defined: None,
            })
        }
    }

    pub fn new_mysql_query(
        catalog_reader: &CatalogReadGuard,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        args: Vec<ExprImpl>,
    ) -> RwResult<Self> {
        let args = Self::handle_postgres_or_mysql_query_args(
            catalog_reader,
            db_name,
            schema_path,
            args,
            "mysql-cdc",
        )?;
        let evaled_args = args
            .iter()
            .map(expr_impl_to_string_fn)
            .collect::<RwResult<Vec<_>>>()?;

        #[cfg(madsim)]
        {
            return Err(crate::error::ErrorCode::BindError(
                "postgres_query can't be used in the madsim mode".to_string(),
            )
            .into());
        }

        #[cfg(not(madsim))]
        {
            let schema = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(async {
                    let database_opts: mysql_async::Opts = {
                        let port = evaled_args[1]
                            .parse::<u16>()
                            .context("failed to parse port")?;
                        mysql_async::OptsBuilder::default()
                            .ip_or_hostname(evaled_args[0].clone())
                            .tcp_port(port)
                            .user(Some(evaled_args[2].clone()))
                            .pass(Some(evaled_args[3].clone()))
                            .db_name(Some(evaled_args[4].clone()))
                            .into()
                    };

                    let pool = mysql_async::Pool::new(database_opts);
                    let mut conn = pool
                        .get_conn()
                        .await
                        .context("failed to connect to mysql in binder")?;

                    let query = evaled_args[5].clone();
                    let statement = conn
                        .prep(query)
                        .await
                        .context("failed to prepare mysql_query in binder")?;

                    let mut rw_types = vec![];
                    #[allow(clippy::never_loop)]
                    for column in statement.columns() {
                        let name = column.name_str().to_string();
                        let data_type = match column.column_type() {
                            // Boolean types
                            MySqlColumnType::MYSQL_TYPE_BIT if column.column_length() == 1 => {
                                DataType::Boolean
                            }

                            // Numeric types
                            // NOTE(kwannoel): Although `bool/boolean` is a synonym of TINY(1) in MySQL,
                            // we treat it as Int16 here. It is better to be straightforward in our conversion.
                            MySqlColumnType::MYSQL_TYPE_TINY => DataType::Int16,
                            MySqlColumnType::MYSQL_TYPE_SHORT => DataType::Int16,
                            MySqlColumnType::MYSQL_TYPE_INT24 => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_LONG => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_LONGLONG => DataType::Int64,
                            MySqlColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
                            MySqlColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
                            MySqlColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Decimal,
                            MySqlColumnType::MYSQL_TYPE_DECIMAL => DataType::Decimal,

                            // Date time types
                            MySqlColumnType::MYSQL_TYPE_YEAR => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_DATE => DataType::Date,
                            MySqlColumnType::MYSQL_TYPE_NEWDATE => DataType::Date,
                            MySqlColumnType::MYSQL_TYPE_TIME => DataType::Time,
                            MySqlColumnType::MYSQL_TYPE_TIME2 => DataType::Time,
                            MySqlColumnType::MYSQL_TYPE_DATETIME => DataType::Timestamp,
                            MySqlColumnType::MYSQL_TYPE_DATETIME2 => DataType::Timestamp,
                            MySqlColumnType::MYSQL_TYPE_TIMESTAMP => DataType::Timestamptz,
                            MySqlColumnType::MYSQL_TYPE_TIMESTAMP2 => DataType::Timestamptz,

                            // String types
                            MySqlColumnType::MYSQL_TYPE_VARCHAR
                            | MySqlColumnType::MYSQL_TYPE_STRING
                            | MySqlColumnType::MYSQL_TYPE_VAR_STRING => DataType::Varchar,

                            // JSON types
                            MySqlColumnType::MYSQL_TYPE_JSON => DataType::Jsonb,

                            // Binary types
                            MySqlColumnType::MYSQL_TYPE_BIT
                            | MySqlColumnType::MYSQL_TYPE_BLOB
                            | MySqlColumnType::MYSQL_TYPE_TINY_BLOB
                            | MySqlColumnType::MYSQL_TYPE_MEDIUM_BLOB
                            | MySqlColumnType::MYSQL_TYPE_LONG_BLOB => DataType::Bytea,

                            MySqlColumnType::MYSQL_TYPE_UNKNOWN
                            | MySqlColumnType::MYSQL_TYPE_TYPED_ARRAY
                            | MySqlColumnType::MYSQL_TYPE_ENUM
                            | MySqlColumnType::MYSQL_TYPE_SET
                            | MySqlColumnType::MYSQL_TYPE_GEOMETRY
                            | MySqlColumnType::MYSQL_TYPE_NULL => {
                                return Err(crate::error::ErrorCode::BindError(format!(
                                    "unsupported column type: {:?}",
                                    column.column_type()
                                ))
                                .into());
                            }
                        };
                        rw_types.push((name, data_type));
                    }
                    Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                        StructType::new(rw_types),
                    ))
                })
            })?;

            Ok(TableFunction {
                args,
                return_type: schema,
                function_type: TableFunctionType::MysqlQuery,
                user_defined: None,
            })
        }
    }

    pub fn to_protobuf(&self) -> PbTableFunction {
        PbTableFunction {
            function_type: self.function_type as i32,
            args: self.args.iter().map(|c| c.to_expr_proto()).collect_vec(),
            return_type: Some(self.return_type.to_protobuf()),
            udf: self.user_defined.as_ref().map(|c| c.as_ref().into()),
        }
    }

    /// Get the name of the table function.
    pub fn name(&self) -> String {
        match self.function_type {
            TableFunctionType::UserDefined => self.user_defined.as_ref().unwrap().name.clone(),
            t => t.as_str_name().to_lowercase(),
        }
    }

    pub fn rewrite(self, rewriter: &mut impl ExprRewriter) -> Self {
        Self {
            args: self
                .args
                .into_iter()
                .map(|e| rewriter.rewrite_expr(e))
                .collect(),
            ..self
        }
    }
}

impl std::fmt::Debug for TableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("FunctionCall")
                .field("function_type", &self.function_type)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .finish()
        } else {
            let func_name = format!("{:?}", self.function_type);
            let mut builder = f.debug_tuple(&func_name);
            self.args.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}

impl Expr for TableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("Table function should not be converted to ExprNode")
    }
}

fn expr_impl_to_string_fn(arg: &ExprImpl) -> RwResult<String> {
    match arg.try_fold_const() {
        Some(Ok(value)) => {
            let Some(scalar) = value else {
                return Err(BindError(
                    "postgres_query function and mysql_query function do not accept null arguments"
                        .to_owned(),
                )
                .into());
            };
            Ok(scalar.into_utf8().to_string())
        }
        Some(Err(err)) => Err(err),
        None => Err(BindError(
            "postgres_query function and mysql_query function only accept constant arguments"
                .to_owned(),
        )
        .into()),
    }
}
