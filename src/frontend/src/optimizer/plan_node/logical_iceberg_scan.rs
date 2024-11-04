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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Field;
use risingwave_common::types::{DataType, Decimal, ScalarImpl};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_pb::batch_plan::iceberg_binary_predicate::IcebergBinaryExprType;
use risingwave_pb::batch_plan::iceberg_predicate::PredicateExpr;
use risingwave_pb::batch_plan::iceberg_ref_and_value_predicate::IcebergRefAndValueType;
use risingwave_pb::batch_plan::iceberg_ref_predicate::IcebergRefType;
use risingwave_pb::batch_plan::iceberg_unary_predicate::IcebergUnaryExprType;
use risingwave_pb::batch_plan::{
    IcebergBinaryPredicate, IcebergDatum, IcebergPredicate, IcebergRefAndValuePredicate,
    IcebergRefPredicate, IcebergSetPredicate, IcebergUnaryPredicate,
};

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, InputRef, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    pub iceberg_predicate: IcebergPredicate,
}

impl LogicalIcebergScan {
    pub fn new(logical_source: &LogicalSource) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let core = logical_source.core.clone();
        let base = PlanBase::new_logical_with_core(&core);

        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergScan {
            base,
            core,
            iceberg_predicate: IcebergPredicate {
                predicate_expr: Some(PredicateExpr::Boolean(true)),
            },
        }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());
        let mut core = self.core.clone();
        core.column_catalog = required_cols
            .iter()
            .map(|idx| core.column_catalog[*idx].clone())
            .collect();
        let base = PlanBase::new_logical_with_core(&core);
        let iceberg_predicate = self.iceberg_predicate.clone();

        LogicalIcebergScan {
            base,
            core,
            iceberg_predicate,
        }
    }

    fn clone_with_iceberg_predicate(&self, iceberg_predicate: IcebergPredicate) -> Self {
        let base = self.base.clone();
        let core = self.core.clone();
        let iceberg_predicate = IcebergPredicate {
            predicate_expr: Some(PredicateExpr::BinaryPredicate(Box::new(
                IcebergBinaryPredicate {
                    expr_type: IcebergBinaryExprType::And as i32,
                    left: Some(Box::new(self.iceberg_predicate.clone())),
                    right: Some(Box::new(iceberg_predicate)),
                },
            ))),
        };
        LogicalIcebergScan {
            base,
            core,
            iceberg_predicate,
        }
    }
}

impl_plan_tree_node_for_leaf! {LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalIcebergScan", fields)
    }
}

impl ColPrunable for LogicalIcebergScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        if required_cols.is_empty() {
            let mapping =
                ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
            // If reuqiured_cols is empty, we use the first column of iceberg to avoid the empty schema.
            LogicalProject::with_mapping(self.clone_with_required_cols(&[0]).into(), mapping).into()
        } else {
            self.clone_with_required_cols(required_cols).into()
        }
    }
}

impl ExprRewritable for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    /// NOTE(kwannoel):
    /// 1. We expect it to be constant folded
    /// 2. We don't convert `inputRefs` of type boolean directly to `IcebergPredicates`.
    /// 3. The leaf nodes are always logical comparison operators:
    ///    `Equal`, `NotEqual`, `GreaterThan`,
    ///    `GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual`.
    /// 4. For leaf nodes, their LHS is always an `inputRef`
    ///    and their RHS is always a `Literal` to be compatible with Iceberg.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        fn rw_literal_to_iceberg_datum(literal: &Literal) -> Option<IcebergDatum> {
            let datum @ Some(scalar) = literal.get_data() else {
                return None;
            };

            match scalar {
                ScalarImpl::Bool(_) => Some(IcebergDatum {
                    datatype: DataType::Boolean.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Int32(_) => Some(IcebergDatum {
                    datatype: DataType::Int32.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Int64(_) => Some(IcebergDatum {
                    datatype: DataType::Int64.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Float32(_) => Some(IcebergDatum {
                    datatype: DataType::Float32.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Float64(_) => Some(IcebergDatum {
                    datatype: DataType::Float64.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Decimal(d) => {
                    let Decimal::Normalized(_) = d else {
                        return None;
                    };
                    Some(IcebergDatum {
                        datatype: DataType::Decimal.to_protobuf().into(),
                        value: datum.to_protobuf().into(),
                    })
                }
                ScalarImpl::Date(_) => Some(IcebergDatum {
                    datatype: DataType::Date.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Timestamp(_) => Some(IcebergDatum {
                    datatype: DataType::Timestamp.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Timestamptz(_) => Some(IcebergDatum {
                    datatype: DataType::Timestamptz.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Utf8(_) => Some(IcebergDatum {
                    datatype: DataType::Varchar.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                ScalarImpl::Bytea(_) => Some(IcebergDatum {
                    datatype: DataType::Bytea.to_protobuf().into(),
                    value: datum.to_protobuf().into(),
                }),
                _ => None,
            }
        }

        fn rw_expr_to_iceberg_predicate(
            expr: &ExprImpl,
            fields: &[Field],
        ) -> Option<IcebergPredicate> {
            match expr {
                ExprImpl::Literal(l) => {
                    if let Some(ScalarImpl::Bool(b)) = l.get_data() {
                        Some(IcebergPredicate {
                            predicate_expr: Some(PredicateExpr::Boolean(*b)),
                        })
                    } else {
                        None
                    }
                }
                ExprImpl::FunctionCall(f) => {
                    fn create_binary_predicate(
                        expr_type: IcebergBinaryExprType,
                        left: &ExprImpl,
                        right: &ExprImpl,
                        fields: &[Field],
                    ) -> Option<IcebergPredicate> {
                        let left = rw_expr_to_iceberg_predicate(left, fields)?;
                        let right = rw_expr_to_iceberg_predicate(right, fields)?;
                        Some(IcebergPredicate {
                            predicate_expr: Some(PredicateExpr::BinaryPredicate(Box::new(
                                IcebergBinaryPredicate {
                                    expr_type: expr_type as i32,
                                    left: Some(Box::new(left)),
                                    right: Some(Box::new(right)),
                                },
                            ))),
                        })
                    }

                    fn create_ref_predicate(
                        expr_type: IcebergRefType,
                        reference: &InputRef,
                        fields: &[Field],
                    ) -> Option<IcebergPredicate> {
                        let reference = fields[reference.index].name.clone();
                        Some(IcebergPredicate {
                            predicate_expr: Some(PredicateExpr::RefPredicate(
                                IcebergRefPredicate {
                                    expr_type: expr_type as i32,
                                    reference,
                                },
                            )),
                        })
                    }

                    /// predicate: a < 1
                    ///
                    /// a: reference
                    /// 1: value.
                    fn create_ref_and_value_predicate(
                        expr_type: IcebergRefAndValueType,
                        reference: &InputRef,
                        arg: &Literal,
                        fields: &[Field],
                    ) -> Option<IcebergPredicate> {
                        let reference = fields[reference.index].name.clone();
                        let arg = rw_literal_to_iceberg_datum(arg)?;
                        Some(IcebergPredicate {
                            predicate_expr: Some(PredicateExpr::RefAndValuePredicate(
                                IcebergRefAndValuePredicate {
                                    expr_type: expr_type as i32,
                                    reference,
                                    value: Some(arg),
                                },
                            )),
                        })
                    }

                    let args = f.inputs();
                    match f.func_type() {
                        ExprType::Not => {
                            let arg = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                            Some(IcebergPredicate {
                                predicate_expr: Some(PredicateExpr::UnaryPredicate(Box::new(
                                    IcebergUnaryPredicate {
                                        expr_type: IcebergUnaryExprType::Not as i32,
                                        arg: Some(Box::new(arg)),
                                    },
                                ))),
                            })
                        }
                        ExprType::And => create_binary_predicate(
                            IcebergBinaryExprType::And,
                            &args[0],
                            &args[1],
                            fields,
                        ),
                        ExprType::Or => create_binary_predicate(
                            IcebergBinaryExprType::Or,
                            &args[0],
                            &args[1],
                            fields,
                        ),
                        ExprType::Equal => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                            | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Eq,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::NotEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                            | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Ne,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::GreaterThan => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Gt,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Le,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::GreaterThanOrEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Ge,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Lt,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::LessThan => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Lt,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Ge,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::LessThanOrEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Le,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                create_ref_and_value_predicate(
                                    IcebergRefAndValueType::Gt,
                                    lhs,
                                    rhs,
                                    fields,
                                )
                            }
                            _ => None,
                        },
                        ExprType::IsNull => match &args[0] {
                            ExprImpl::InputRef(reference) => {
                                create_ref_predicate(IcebergRefType::IsNull, reference, fields)
                            }
                            _ => None,
                        },
                        ExprType::IsNotNull => match &args[0] {
                            ExprImpl::InputRef(reference) => {
                                create_ref_predicate(IcebergRefType::IsNotNull, reference, fields)
                            }
                            _ => None,
                        },
                        ExprType::In => match &args[0] {
                            ExprImpl::InputRef(lhs) => {
                                let reference = fields[lhs.index].name.clone();
                                let mut datums = Vec::with_capacity(args.len() - 1);
                                for arg in &args[1..] {
                                    if let ExprImpl::Literal(l) = arg {
                                        if let Some(datum) = rw_literal_to_iceberg_datum(l) {
                                            datums.push(datum);
                                        } else {
                                            return None;
                                        }
                                    } else {
                                        return None;
                                    }
                                }
                                Some(IcebergPredicate {
                                    predicate_expr: Some(PredicateExpr::SetPredicate(
                                        IcebergSetPredicate {
                                            expr_type: IcebergRefAndValueType::In as i32,
                                            reference,
                                            values: datums,
                                        },
                                    )),
                                })
                            }
                            _ => None,
                        },
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        fn rw_predicate_to_iceberg_predicate(
            predicate: Condition,
            fields: &[Field],
        ) -> (Condition, Option<IcebergPredicate>) {
            if predicate.always_true() {
                return (predicate, None);
            }
            let mut conjunctions = predicate.conjunctions;
            let mut ignored_conjunctions: Vec<ExprImpl> = Vec::with_capacity(conjunctions.len());
            let mut iceberg_condition_root;
            loop {
                let Some(rw_condition_root) = conjunctions.pop() else {
                    return (Condition { conjunctions }, None);
                };
                match rw_expr_to_iceberg_predicate(&rw_condition_root, fields) {
                    Some(iceberg_predicate) => {
                        iceberg_condition_root = iceberg_predicate;
                        break;
                    }
                    None => ignored_conjunctions.push(rw_condition_root),
                }
            }
            for rw_condition in conjunctions {
                match rw_expr_to_iceberg_predicate(&rw_condition, fields) {
                    Some(iceberg_predicate) => {
                        iceberg_condition_root = IcebergPredicate {
                            predicate_expr: Some(PredicateExpr::BinaryPredicate(Box::new(
                                IcebergBinaryPredicate {
                                    expr_type: IcebergBinaryExprType::And as i32,
                                    left: Some(Box::new(iceberg_condition_root)),
                                    right: Some(Box::new(iceberg_predicate)),
                                },
                            ))),
                        }
                    }
                    None => ignored_conjunctions.push(rw_condition),
                }
            }
            (
                Condition {
                    conjunctions: ignored_conjunctions,
                },
                Some(iceberg_condition_root),
            )
        }

        let schema = self.schema();
        let fields = &schema.fields;

        let (rw_predicate, iceberg_predicate_opt) =
            rw_predicate_to_iceberg_predicate(predicate, fields);
        // No pushdown.
        let this = if let Some(iceberg_predicate) = iceberg_predicate_opt {
            self.clone_with_iceberg_predicate(iceberg_predicate).into()
        } else {
            self.clone().into()
        };
        LogicalFilter::create(this, rw_predicate)
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<PlanRef> {
        let plan: PlanRef = BatchIcebergScan::new(self.core.clone()).into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
