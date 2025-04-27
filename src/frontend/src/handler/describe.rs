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

use std::cmp::max;
use std::fmt::Display;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pretty_xmlish::{Pretty, PrettyConfig};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_common::types::{DataType, Fields};
use risingwave_expr::bail;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_sqlparser::ast::{DescribeKind, ObjectName, display_comma_separated};

use super::explain::ExplainRow;
use super::show::ShowColumnRow;
use super::{RwPgResponse, fields_to_descriptors};
use crate::binder::{Binder, Relation};
use crate::catalog::CatalogError;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponseBuilderExt};

pub fn handle_describe(handler_args: HandlerArgs, object_name: ObjectName) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let mut binder = Binder::new_for_system(&session);

    Binder::validate_cross_db_reference(&session.database(), &object_name)?;
    let not_found_err =
        CatalogError::NotFound("table, source, sink or view", object_name.to_string());

    // Vec<ColumnCatalog>, Vec<ColumnDesc>, Vec<ColumnDesc>, Vec<Arc<IndexCatalog>>, String, Option<String>
    let (columns, pk_columns, dist_columns, indices, relname, description) = if let Ok(relation) =
        binder.bind_relation_by_name(object_name.clone(), None, None, false)
    {
        match relation {
            Relation::Source(s) => {
                let pk_column_catalogs = s
                    .catalog
                    .pk_col_ids
                    .iter()
                    .map(|&column_id| {
                        s.catalog
                            .columns
                            .iter()
                            .filter(|x| x.column_id() == column_id)
                            .map(|x| x.column_desc.clone())
                            .exactly_one()
                            .unwrap()
                    })
                    .collect_vec();
                (
                    s.catalog.columns,
                    pk_column_catalogs,
                    vec![],
                    vec![],
                    s.catalog.name,
                    None, // Description
                )
            }
            Relation::BaseTable(t) => {
                let pk_column_catalogs = t
                    .table_catalog
                    .pk()
                    .iter()
                    .map(|x| t.table_catalog.columns[x.column_index].column_desc.clone())
                    .collect_vec();
                let dist_columns = t
                    .table_catalog
                    .distribution_key()
                    .iter()
                    .map(|idx| t.table_catalog.columns[*idx].column_desc.clone())
                    .collect_vec();
                (
                    t.table_catalog.columns.clone(),
                    pk_column_catalogs,
                    dist_columns,
                    t.table_indexes,
                    t.table_catalog.name.clone(),
                    t.table_catalog.description.clone(),
                )
            }
            Relation::SystemTable(t) => {
                let pk_column_catalogs = t
                    .sys_table_catalog
                    .pk
                    .iter()
                    .map(|idx| t.sys_table_catalog.columns[*idx].column_desc.clone())
                    .collect_vec();
                (
                    t.sys_table_catalog.columns.clone(),
                    pk_column_catalogs,
                    vec![],
                    vec![],
                    t.sys_table_catalog.name.clone(),
                    None, // Description
                )
            }
            Relation::Share(_) => {
                if let Ok(view) = binder.bind_view_by_name(object_name.clone()) {
                    let columns = view
                        .view_catalog
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| ColumnCatalog {
                            column_desc: ColumnDesc::from_field_with_column_id(field, idx as _),
                            is_hidden: false,
                        })
                        .collect();
                    (
                        columns,
                        vec![],
                        vec![],
                        vec![],
                        view.view_catalog.name.clone(),
                        None,
                    )
                } else {
                    return Err(not_found_err.into());
                }
            }
            _ => {
                return Err(not_found_err.into());
            }
        }
    } else if let Ok(sink) = binder.bind_sink_by_name(object_name.clone()) {
        let columns = sink.sink_catalog.full_columns().to_vec();
        let pk_columns = sink
            .sink_catalog
            .downstream_pk_indices()
            .into_iter()
            .map(|idx| columns[idx].column_desc.clone())
            .collect_vec();
        let dist_columns = sink
            .sink_catalog
            .distribution_key
            .iter()
            .map(|idx| columns[*idx].column_desc.clone())
            .collect_vec();
        (
            columns,
            pk_columns,
            dist_columns,
            vec![],
            sink.sink_catalog.name.clone(),
            None,
        )
    } else {
        return Err(not_found_err.into());
    };

    // Convert all column descs to rows
    let mut rows = columns
        .into_iter()
        .flat_map(ShowColumnRow::from_catalog)
        .collect_vec();

    fn concat<T>(display_elems: impl IntoIterator<Item = T>) -> String
    where
        T: Display,
    {
        format!(
            "{}",
            display_comma_separated(&display_elems.into_iter().collect::<Vec<_>>())
        )
    }

    // Convert primary key to rows
    if !pk_columns.is_empty() {
        rows.push(ShowColumnRow {
            name: "primary key".into(),
            r#type: concat(pk_columns.iter().map(|x| &x.name)),
            is_hidden: None,
            description: None,
        });
    }

    // Convert distribution keys to rows
    if !dist_columns.is_empty() {
        rows.push(ShowColumnRow {
            name: "distribution key".into(),
            r#type: concat(dist_columns.iter().map(|x| &x.name)),
            is_hidden: None,
            description: None,
        });
    }

    // Convert all indexes to rows
    rows.extend(indices.iter().map(|index| {
        let index_display = index.display();

        ShowColumnRow {
            name: index.name.clone(),
            r#type: if index_display.include_columns.is_empty() {
                format!(
                    "index({}) distributed by({})",
                    display_comma_separated(&index_display.index_columns_with_ordering),
                    display_comma_separated(&index_display.distributed_by_columns),
                )
            } else {
                format!(
                    "index({}) include({}) distributed by({})",
                    display_comma_separated(&index_display.index_columns_with_ordering),
                    display_comma_separated(&index_display.include_columns),
                    display_comma_separated(&index_display.distributed_by_columns),
                )
            },
            is_hidden: None,
            // TODO: index description
            description: None,
        }
    }));

    rows.push(ShowColumnRow {
        name: "table description".into(),
        r#type: relname,
        is_hidden: None,
        description,
    });

    // TODO: table name and description as title of response
    // TODO: recover the original user statement
    Ok(PgResponse::builder(StatementType::DESCRIBE)
        .rows(rows)
        .into())
}

pub fn infer_describe(kind: &DescribeKind) -> Vec<PgFieldDescriptor> {
    match kind {
        DescribeKind::Fragments | DescribeKind::Fragment => vec![PgFieldDescriptor::new(
            "Fragments".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )],
        DescribeKind::Plain => fields_to_descriptors(ShowColumnRow::fields()),
    }
}
pub async fn handle_describe_fragments(
    handler_args: HandlerArgs,
    object_name: ObjectName,
    options: Option<Vec<risingwave_sqlparser::ast::Ident>>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let job_id = {
        let mut binder = Binder::new_for_system(&session);

        Binder::validate_cross_db_reference(&session.database(), &object_name)?;
        let not_found_err = CatalogError::NotFound("stream job", object_name.to_string());

        if let Ok(relation) = binder.bind_relation_by_name(object_name.clone(), None, None, false) {
            match relation {
                Relation::Source(s) => {
                    if s.is_shared() {
                        s.catalog.id
                    } else {
                        bail!(ErrorCode::NotSupported(
                            "non shared source has no fragments to describe".to_owned(),
                            "Use `DESCRIBE` instead.".to_owned(),
                        ));
                    }
                }
                Relation::BaseTable(t) => t.table_catalog.id.table_id,
                Relation::SystemTable(_t) => {
                    bail!(ErrorCode::NotSupported(
                        "system table has no fragments to describe".to_owned(),
                        "Use `DESCRIBE` instead.".to_owned(),
                    ));
                }
                Relation::Share(_s) => {
                    bail!(ErrorCode::NotSupported(
                        "view has no fragments to describe".to_owned(),
                        "Use `DESCRIBE` instead.".to_owned(),
                    ));
                }
                _ => {
                    // Other relation types (Subquery, Join, etc.) are not directly describable.
                    return Err(not_found_err.into());
                }
            }
        } else if let Ok(sink) = binder.bind_sink_by_name(object_name.clone()) {
            sink.sink_catalog.id.sink_id
        } else {
            return Err(not_found_err.into());
        }
    };

    let meta_client = session.env().meta_client();
    let fragments = &meta_client.list_table_fragments(&[job_id]).await?[&job_id];
    let verbose = options.as_ref().map(|opts| opts.iter().any(|opt| opt.real_value() == "VERBOSE"));
    let res = generate_fragments_string(fragments, verbose)?;
pub async fn handle_describe_fragment(
    handler_args: HandlerArgs,
    fragment_id_name: ObjectName,
    options: Option<Vec<risingwave_sqlparser::ast::Ident>>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    
    let fragment_id_str = fragment_id_name.to_string();
    let fragment_id = fragment_id_str.parse::<u32>().map_err(|_| {
        ErrorCode::InvalidInputSyntax(format!(
            "fragment ID must be a positive integer, got: {}", 
            fragment_id_str
        ))
    })?;
    
    let meta_client = session.env().meta_client();
    let fragment_info = meta_client.get_fragment_by_id(fragment_id).await?;
    
    match fragment_info {
        Some(info) => {
            let verbose = options.as_ref().map(|opts| opts.iter().any(|opt| opt.real_value() == "VERBOSE"));
            let res = generate_fragments_string(&info, verbose)?;
            Ok(res)
        }
        None => {
            Err(ErrorCode::ItemNotFound(
                format!("fragment {}", fragment_id)
            ).into())
        }
    }
}

    Ok(res)
}

/// The implementation largely copied from `crate::utils::stream_graph_formatter::StreamGraphFormatter`.
/// The input is different, so we need separate implementation.
fn generate_fragments_string(fragments: &TableFragmentInfo, verbose: Option<bool>) -> Result<RwPgResponse> {
    let verbose = verbose.unwrap_or(true);
    let mut config = PrettyConfig {
        need_boundaries: false,
        width: 80,
        ..Default::default()
    };

    let mut max_width = 80;

    let mut blocks = vec![];
    for fragment in fragments.fragments.iter().sorted_by_key(|f| f.id) {
        let mut res = String::new();
        let actor_ids = fragment.actors.iter().map(|a| a.id).format(",");
        
        let relation_name = String::new();
        
        res.push_str(&format!("Fragment {}{} (Actor {})\n", fragment.id, relation_name, actor_ids));
        let node = &fragment.actors[0].node;
        let node = explain_node(node.as_ref().unwrap(), verbose);
        let width = config.unicode(&mut res, &node);
        max_width = max(width, max_width);
        config.width = max_width;
        blocks.push(res);
        blocks.push("".to_owned());
    }

    let rows = blocks.iter().map(|b| ExplainRow {
        query_plan: b.into(),
    });
    Ok(PgResponse::builder(StatementType::DESCRIBE)
        .rows(rows)
        .into())
}

fn explain_node<'a>(node: &StreamNode, verbose: bool) -> Pretty<'a> {
    // TODO: we need extra edge information to get which fragment MergeExecutor connects to.
    let one_line_explain = node.identity.clone();

    let mut fields = Vec::with_capacity(3);
    if verbose {
        fields.push((
            "output",
            Pretty::Array(
                node.fields
                    .iter()
                    .map(|f| Pretty::display(f.get_name()))
                    .collect(),
            ),
        ));
        fields.push((
            "stream key",
            Pretty::Array(
                node.stream_key
                    .iter()
                    .map(|i| Pretty::display(node.fields[*i as usize].get_name()))
                    .collect(),
            ),
        ));
    }
    let children = node
        .input
        .iter()
        .map(|input| explain_node(input, verbose))
        .collect();
    Pretty::simple_record(one_line_explain, fields, children)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_describe_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create table t (v1 int, v2 int, v3 int primary key, v4 int);")
            .await
            .unwrap();

        frontend
            .run_sql("create index idx1 on t (v1 DESC, v2);")
            .await
            .unwrap();

        let sql = "describe t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = HashMap::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.insert(
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                );
            }
        }

        let expected_columns: HashMap<String, String> = maplit::hashmap! {
            "v1".into() => "integer".into(),
            "v2".into() => "integer".into(),
            "v3".into() => "integer".into(),
            "v4".into() => "integer".into(),
            "primary key".into() => "v3".into(),
            "distribution key".into() => "v3".into(),
            "_rw_timestamp".into() => "timestamp with time zone".into(),
            "idx1".into() => "index(v1 DESC, v2 ASC, v3 ASC) include(v4) distributed by(v1)".into(),
            "table description".into() => "t".into(),
        };

        assert_eq!(columns, expected_columns);
    }
}
