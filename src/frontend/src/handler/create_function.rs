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

use anyhow::Context;
use either::Either;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::StructType;
use risingwave_expr::sig::{CreateOptions, UdfKind};
use risingwave_pb::catalog::PbFunction;
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};

use super::*;
use crate::{Binder, bind_data_type};

/// Non-SQL UDFs exchange data via Arrow, which does not support VARIANT yet.
pub(crate) fn reject_variant_in_udf_signature(
    return_type: &risingwave_common::types::DataType,
    arg_types: &[risingwave_common::types::DataType],
    kind: &str,
) -> Result<()> {
    if return_type.contains_variant() || arg_types.iter().any(|t| t.contains_variant()) {
        return Err(ErrorCode::NotSupported(
            format!("VARIANT type in {kind} signature"),
            "VARIANT is not supported in UDFs yet".to_owned(),
        )
        .into());
    }
    Ok(())
}

pub async fn handle_create_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    if_not_exists: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    returns: Option<CreateFunctionReturns>,
    params: CreateFunctionBody,
    with_options: CreateFunctionWithOptions,
) -> Result<RwPgResponse> {
    if or_replace {
        bail_not_implemented!("CREATE OR REPLACE FUNCTION");
    }
    if temporary {
        bail_not_implemented!("CREATE TEMPORARY FUNCTION");
    }

    let is_immutable = matches!(&params.behavior, Some(FunctionBehavior::Immutable));
    let udf_config = handler_args.session.env().udf_config();

    // e.g., `language [ python / javascript / ...etc]`
    let language = match params.language {
        Some(lang) => {
            let lang = lang.real_value().to_lowercase();
            match &*lang {
                "java" => lang, // only support external UDF for Java
                "python" if udf_config.enable_embedded_python_udf => lang,
                "javascript" if udf_config.enable_embedded_javascript_udf => lang,
                "rust" | "wasm" if udf_config.enable_embedded_wasm_udf => lang,
                "python" | "javascript" | "rust" | "wasm" => {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "{} UDF is not enabled in configuration",
                        lang
                    ))
                    .into());
                }
                _ => {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "language {} is not supported",
                        lang
                    ))
                    .into());
                }
            }
        }
        // Empty language is acceptable since we only require the external server implements the
        // correct protocol.
        None => "".to_owned(),
    };

    let runtime = match params.runtime {
        Some(_) => {
            return Err(ErrorCode::InvalidParameterValue(
                "runtime selection is currently not supported".to_owned(),
            )
            .into());
        }
        None => None,
    };

    let always_retry_on_network_error = with_options
        .always_retry_on_network_error
        .unwrap_or_default();
    let skip_materializing_eval_result = with_options
        .skip_materializing_eval_result
        .unwrap_or_default();
    if skip_materializing_eval_result && !always_retry_on_network_error {
        return Err(ErrorCode::InvalidParameterValue(
            "`always_retry_on_network_error` must be true when `skip_materializing_eval_result` is true"
                .to_owned(),
        )
        .into());
    }
    if skip_materializing_eval_result && !is_immutable {
        return Err(ErrorCode::InvalidParameterValue(
            "`IMMUTABLE` must be specified when `skip_materializing_eval_result` is true"
                .to_owned(),
        )
        .into());
    }

    let return_type;
    let kind = match returns {
        Some(CreateFunctionReturns::Value(data_type)) => {
            return_type = bind_data_type(&data_type)?;
            Kind::Scalar(ScalarFunction {})
        }
        Some(CreateFunctionReturns::Table(columns)) => {
            if columns.len() == 1 {
                // return type is the original type for single column
                return_type = bind_data_type(&columns[0].data_type)?;
            } else {
                // return type is a struct for multiple columns
                let it = columns
                    .into_iter()
                    .map(|c| bind_data_type(&c.data_type).map(|ty| (c.name.real_value(), ty)));
                let fields = it.try_collect::<_, Vec<_>, _>()?;
                return_type = StructType::new(fields).into();
            }
            Kind::Table(TableFunction {})
        }
        None => {
            return Err(ErrorCode::InvalidParameterValue(
                "return type must be specified".to_owned(),
            )
            .into());
        }
    };

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_names.push(arg.name.map_or("".to_owned(), |n| n.real_value()));
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    reject_variant_in_udf_signature(&return_type, &arg_types, "function")?;

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = &session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if the function exists in the catalog
    if let Either::Right(resp) = session.check_function_name_duplicated(
        StatementType::CREATE_FUNCTION,
        name,
        &arg_types,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let link = match &params.using {
        Some(CreateFunctionUsing::Link(l)) => Some(l.as_str()),
        _ => None,
    };
    let base64_decoded = match &params.using {
        Some(CreateFunctionUsing::Base64(encoded)) => {
            use base64::prelude::{BASE64_STANDARD, Engine};
            let bytes = BASE64_STANDARD
                .decode(encoded)
                .context("invalid base64 encoding")?;
            Some(bytes)
        }
        _ => None,
    };

    let create_fn =
        risingwave_expr::sig::find_udf_impl(&language, runtime.as_deref(), link)?.create_fn;
    let output = create_fn(CreateOptions {
        kind: match kind {
            Kind::Scalar(_) => UdfKind::Scalar,
            Kind::Table(_) => UdfKind::Table,
            Kind::Aggregate(_) => unreachable!(),
        },
        name: &function_name,
        arg_names: &arg_names,
        arg_types: &arg_types,
        return_type: &return_type,
        as_: params.as_.as_ref().map(|s| s.as_str()),
        using_link: link,
        using_base64_decoded: base64_decoded.as_deref(),
    })?;

    let function = PbFunction {
        id: FunctionId::placeholder(),
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_names,
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        runtime,
        name_in_runtime: Some(output.name_in_runtime),
        link: link.map(|s| s.to_owned()),
        body: output.body,
        compressed_binary: output.compressed_binary,
        owner: session.user_id(),
        always_retry_on_network_error,
        skip_materializing_eval_result,
        is_async: with_options.r#async,
        is_batched: with_options.batch,
        created_at_epoch: None,
        created_at_cluster_version: None,
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;
    use risingwave_expr::sig::{CreateFunctionOutput, UDF_IMPLS, UdfImplDescriptor};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[linkme::distributed_slice(UDF_IMPLS)]
    static TEST_UDF: UdfImplDescriptor = UdfImplDescriptor {
        match_fn: |language, runtime, link| {
            language.is_empty() && runtime.is_none() && link.is_none()
        },
        create_fn: |opts| {
            Ok(CreateFunctionOutput {
                name_in_runtime: opts.name.to_owned(),
                body: opts.as_.map(ToOwned::to_owned),
                compressed_binary: None,
            })
        },
        build_fn: |_| unreachable!("the planner test does not execute the UDF"),
    };

    /// Verifies option dependencies, catalog propagation, recursive purity, and top-level
    /// materialization for a regular scalar UDF.
    #[tokio::test]
    async fn test_skip_materializing_eval_result() {
        let frontend = LocalFrontend::new(Default::default()).await;

        frontend.run_sql("create table t(v int)").await.unwrap();

        // Isolate the retry validation by providing IMMUTABLE.
        let error = frontend
            .run_sql(
                r#"create function rejected_without_retry(v int)
                   returns int immutable
                   with (skip_materializing_eval_result = true)"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(
                "`always_retry_on_network_error` must be true when `skip_materializing_eval_result` is true"
            ),
            "{error}"
        );

        // Isolate the determinism validation by enabling retries.
        let error = frontend
            .run_sql(
                r#"create function rejected_without_immutable(v int)
                   returns int
                   with (
                       skip_materializing_eval_result = true,
                       always_retry_on_network_error = true
                   )"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(
                "`IMMUTABLE` must be specified when `skip_materializing_eval_result` is true"
            ),
            "{error}"
        );

        frontend
            .run_sql(
                r#"create function identity_without_stored_result(v int)
                   returns int immutable
                   with (
                       skip_materializing_eval_result = true,
                       always_retry_on_network_error = true
                   )"#,
            )
            .await
            .unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (function, _) = catalog_reader
            .get_function_by_name_args(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(DEFAULT_SCHEMA_NAME),
                "identity_without_stored_result",
                &[DataType::Int32],
            )
            .unwrap();
        // The validated option must survive catalog creation so expression planning sees the
        // same materialization policy that was specified in CREATE FUNCTION.
        assert!(function.skip_materializing_eval_result);
        drop(catalog_reader);

        // An opted-out immutable UDF with only pure arguments is recursively pure, so its project
        // needs no StreamMaterializedExprs state table.
        let plan = frontend
            .get_explain_output(
                "explain create materialized view mv as \
                 select identity_without_stored_result(v) as v from t",
            )
            .await;
        assert!(plan.contains("StreamProject"), "{plan}");
        assert!(!plan.contains("StreamMaterializedExprs"), "{plan}");

        // An opted-out UDF does not hide an impure descendant. Recursive purity marks the complete
        // projected expression as impure, so the planner materializes the top-level result.
        let plan = frontend
            .get_explain_output(
                "explain create materialized view mv_random as \
                 select identity_without_stored_result(random()::int) as v from t",
            )
            .await;
        let materialized_line = plan
            .lines()
            .find(|line| line.contains("StreamMaterializedExprs"))
            .expect("the complete impure project expression should be materialized");
        // Both names on the same operator line prove that it stores the complete outer expression,
        // with RANDOM still nested inside it, rather than storing nested descendants separately.
        assert!(materialized_line.contains("Random"), "{plan}");
        assert!(
            materialized_line.contains("identity_without_stored_result"),
            "{plan}"
        );
    }

    /// Verifies that an UPSERT project does not materialize impure computed expressions. Project
    /// stream keys are always direct input references, including hidden references appended by
    /// stream-plan rewriting, so computed impure expressions are necessarily non-key columns.
    #[tokio::test]
    async fn test_upsert_project_skips_impure_expr_materialization() {
        let frontend = LocalFrontend::new(Default::default()).await;

        frontend
            .run_sql("create table upsert_input(id int primary key, v int)")
            .await
            .unwrap();
        frontend
            .run_sql("create table upsert_output(v int, id int primary key)")
            .await
            .unwrap();
        frontend
            .run_sql("create table computed_key_output(key int primary key, v int)")
            .await
            .unwrap();
        frontend
            .run_sql(
                r#"create function identity_without_stored_result(v int)
                   returns int immutable
                   with (
                       skip_materializing_eval_result = true,
                       always_retry_on_network_error = true
                   )"#,
            )
            .await
            .unwrap();
        frontend
            .run_sql(
                r#"create function identity_with_stored_result(v int)
                   returns int immutable
                   with (always_retry_on_network_error = true)"#,
            )
            .await
            .unwrap();

        // No matter the UDF is marked or not, the project on an UPSERT stream does not materialize
        // them.
        let plan = frontend
            .get_explain_output(
                "explain create sink skipped_sink into upsert_output as \
                 select identity_without_stored_result(v) as v, id \
                 from upsert_input with (snapshot = 'false')",
            )
            .await;
        assert!(plan.contains("StreamProject"), "{plan}");
        assert!(!plan.contains("StreamMaterializedExprs"), "{plan}");

        let plan = frontend
            .get_explain_output(
                "explain create sink unmarked_sink into upsert_output as \
                 select identity_with_stored_result(v) as v, id \
                 from upsert_input with (snapshot = 'false')",
            )
            .await;
        assert!(plan.contains("StreamProject"), "{plan}");
        assert!(!plan.contains("StreamMaterializedExprs"), "{plan}");

        // Skipping UPSERT expression materialization is safe because a sink cannot use a computed
        // impure output as its downstream primary key. Such a key does not match the one derived
        // from the internal stream and is rejected later by sink planning.
        let error = frontend
            .run_sql(
                "create sink computed_key_sink into computed_key_output as \
                 select identity_with_stored_result(id) as key, v \
                 from upsert_input with (snapshot = 'false')",
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(
                "the downstream primary key must be the same as or a subset of the one derived from the stream"
            ),
            "{error}"
        );
    }
}
