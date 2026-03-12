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

use risingwave_common::types::ScalarImpl;
use risingwave_common::util::value_encoding::serialize_datum;
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};
use risingwave_pb::expr::{ExprNode, FunctionCall};
use risingwave_pb::stream_plan::CdcFilterNode;

use super::*;
use crate::executor::FilterExecutor;

pub struct CdcFilterExecutorBuilder;

/// Column index of `_rw_table_name` in CDC source chunks.
///
/// Current CDC source schema is `(payload, _rw_offset, _rw_table_name)`.
/// Keep this value in sync with CDC source schema definition.
const RW_TABLE_NAME_COLUMN_IDX: u32 = 2;

/// `CdcFilter` is an extension to the Filter executor
impl ExecutorBuilder for CdcFilterExecutorBuilder {
    type Node = CdcFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let search_condition_proto = build_search_condition_with_compat(node)?;
        let search_condition =
            build_non_strict_from_prost(&search_condition_proto, params.eval_error_report)?;

        let exec = FilterExecutor::new(params.actor_context, input, search_condition);
        Ok((params.info, exec).into())
    }
}

fn build_search_condition_with_compat(node: &CdcFilterNode) -> StreamResult<ExprNode> {
    Ok(with_legacy_sqlserver_table_name_compat(
        node.get_search_condition()?,
    ))
}

/// For legacy SQL Server CDC tables created before table-name normalization, preserve
/// compatibility between:
/// - historical filter literal: `db.schema.table`
/// - runtime `_rw_table_name`: `schema.table`
///
/// NOTE: This only rewrites a top-level equality predicate.
/// `CdcFilter` search condition is expected to be a single equality on `_rw_table_name`.
/// If that assumption changes in planner/proto, this function should be updated accordingly.
fn with_legacy_sqlserver_table_name_compat(search_condition: &ExprNode) -> ExprNode {
    let Some((table_name_ref_expr, table_name_literal_expr, table_name_literal)) =
        extract_cdc_filter_eq_condition(search_condition)
    else {
        return search_condition.clone();
    };

    let Some(normalized_table_name) = normalize_legacy_sqlserver_table_name(table_name_literal)
    else {
        return search_condition.clone();
    };

    // The filter is already in normalized format.
    if normalized_table_name == table_name_literal {
        return search_condition.clone();
    }

    let mut normalized_literal_expr = table_name_literal_expr.clone();
    if let Some(RexNode::Constant(constant)) = normalized_literal_expr.rex_node.as_mut() {
        // Must use serialize_datum to produce the correct value_encoding format
        // (null tag byte + data), not raw UTF-8 bytes.
        constant.body =
            serialize_datum(Some(ScalarImpl::Utf8(normalized_table_name.into()).as_scalar_ref_impl()));
    } else {
        return search_condition.clone();
    }

    let compat_equal_expr = ExprNode {
        function_type: ExprType::Equal as i32,
        return_type: search_condition.return_type.clone(),
        rex_node: Some(RexNode::FuncCall(FunctionCall {
            children: vec![table_name_ref_expr.clone(), normalized_literal_expr],
        })),
    };

    ExprNode {
        function_type: ExprType::Or as i32,
        return_type: search_condition.return_type.clone(),
        rex_node: Some(RexNode::FuncCall(FunctionCall {
            children: vec![search_condition.clone(), compat_equal_expr],
        })),
    }
}

fn extract_cdc_filter_eq_condition(
    search_condition: &ExprNode,
) -> Option<(&ExprNode, &ExprNode, &str)> {
    if search_condition.function_type() != ExprType::Equal {
        return None;
    }
    let RexNode::FuncCall(func_call) = search_condition.rex_node.as_ref()? else {
        return None;
    };
    if func_call.children.len() != 2 {
        return None;
    }
    let lhs = &func_call.children[0];
    let rhs = &func_call.children[1];

    if is_rw_table_name_ref(lhs) {
        return extract_varchar_literal(rhs).map(|s| (lhs, rhs, s));
    }
    if is_rw_table_name_ref(rhs) {
        return extract_varchar_literal(lhs).map(|s| (rhs, lhs, s));
    }
    None
}

fn is_rw_table_name_ref(expr: &ExprNode) -> bool {
    matches!(
        expr.rex_node,
        Some(RexNode::InputRef(RW_TABLE_NAME_COLUMN_IDX))
    )
}

fn extract_varchar_literal(expr: &ExprNode) -> Option<&str> {
    let RexNode::Constant(constant) = expr.rex_node.as_ref()? else {
        return None;
    };
    std::str::from_utf8(&constant.body).ok()
}

fn normalize_legacy_sqlserver_table_name(table_name: &str) -> Option<String> {
    let mut parts = table_name.split('.');
    let (Some(_db), Some(schema), Some(table), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };

    Some(format!("{schema}.{table}"))
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::{DataType, Datum};

    use super::*;

    fn varchar_type() -> DataType {
        DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Varchar as i32,
            ..Default::default()
        }
    }

    fn bool_type() -> DataType {
        DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Boolean as i32,
            ..Default::default()
        }
    }

    fn rw_table_name_ref_expr() -> ExprNode {
        ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(varchar_type()),
            rex_node: Some(RexNode::InputRef(2)),
        }
    }

    fn literal_expr(v: &str) -> ExprNode {
        ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(varchar_type()),
            rex_node: Some(RexNode::Constant(Datum {
                body: v.as_bytes().to_vec(),
            })),
        }
    }

    fn equal_expr(lhs: ExprNode, rhs: ExprNode) -> ExprNode {
        ExprNode {
            function_type: ExprType::Equal as i32,
            return_type: Some(bool_type()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![lhs, rhs],
            })),
        }
    }

    #[test]
    fn test_normalize_legacy_sqlserver_table_name() {
        assert_eq!(
            normalize_legacy_sqlserver_table_name("prod.dbo.Answer").as_deref(),
            Some("dbo.Answer")
        );
        assert_eq!(normalize_legacy_sqlserver_table_name("dbo.Answer"), None);
        assert_eq!(normalize_legacy_sqlserver_table_name("prod.dbo.a.b"), None);
    }

    #[test]
    fn test_legacy_sqlserver_filter_rewrite() {
        let search_condition =
            equal_expr(rw_table_name_ref_expr(), literal_expr("prod.dbo.Answer"));
        let rewritten = with_legacy_sqlserver_table_name_compat(&search_condition);

        assert_eq!(rewritten.function_type(), ExprType::Or);
        let RexNode::FuncCall(or_func) = rewritten.rex_node.unwrap() else {
            panic!("expect OR function call");
        };
        assert_eq!(or_func.children.len(), 2);
    }

    #[test]
    fn test_normalized_filter_no_rewrite() {
        let search_condition = equal_expr(rw_table_name_ref_expr(), literal_expr("dbo.Answer"));
        let rewritten = with_legacy_sqlserver_table_name_compat(&search_condition);
        assert_eq!(rewritten, search_condition);
    }

    #[test]
    fn test_legacy_sqlserver_rewrite_via_cdc_filter_node_entry() {
        let search_condition =
            equal_expr(rw_table_name_ref_expr(), literal_expr("prod.dbo.Answer"));
        let node = CdcFilterNode {
            search_condition: Some(search_condition),
            ..Default::default()
        };

        let rewritten = build_search_condition_with_compat(&node).expect("build condition");
        assert_eq!(rewritten.function_type(), ExprType::Or);

        let RexNode::FuncCall(or_func) = rewritten.rex_node.expect("or node") else {
            panic!("expect OR function call");
        };
        assert_eq!(or_func.children.len(), 2);
    }
}
