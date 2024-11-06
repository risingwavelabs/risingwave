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

use chrono::Datelike;
use educe::Educe;
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
use iceberg::spec::Datum as IcebergDatum;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{Decimal, ScalarImpl};

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Educe, Debug, Clone, PartialEq)]
#[educe(Eq, Hash)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    /// TODO(kwannoel): If we support plan sharing, we can't just ignore the predicate.
    #[educe(Hash(ignore))]
    pub predicate: IcebergPredicate,
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
            predicate: IcebergPredicate::AlwaysTrue,
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

        LogicalIcebergScan {
            base,
            core,
            predicate: self.predicate.clone(),
        }
    }

    pub fn clone_with_predicate(&self, predicate: IcebergPredicate) -> Self {
        let base = PlanBase::new_logical_with_core(&self.core);
        let predicate = predicate.and(self.predicate.clone());
        LogicalIcebergScan {
            base,
            core: self.core.clone(),
            predicate,
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
    /// 2. We don't convert `inputRefs` of type boolean directly to IcebergPredicates.
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
            let Some(scalar) = literal.get_data() else {
                return None;
            };
            match scalar {
                ScalarImpl::Bool(b) => Some(IcebergDatum::bool(*b)),
                ScalarImpl::Int32(i) => Some(IcebergDatum::int(*i)),
                ScalarImpl::Int64(i) => Some(IcebergDatum::long(*i)),
                ScalarImpl::Float32(f) => Some(IcebergDatum::float(*f)),
                ScalarImpl::Float64(f) => Some(IcebergDatum::double(*f)),
                ScalarImpl::Decimal(d) => {
                    let Decimal::Normalized(d) = d else {
                        return None;
                    };
                    let Ok(d) = IcebergDatum::decimal(*d) else {
                        return None;
                    };
                    Some(d)
                }
                ScalarImpl::Date(d) => {
                    let Ok(datum) = IcebergDatum::date_from_ymd(d.0.year(), d.0.month(), d.0.day())
                    else {
                        return None;
                    };
                    Some(datum)
                }
                ScalarImpl::Timestamp(t) => Some(IcebergDatum::timestamp_nanos(
                    t.0.and_utc().timestamp_nanos_opt()?,
                )),
                ScalarImpl::Timestamptz(t) => {
                    Some(IcebergDatum::timestamptz_micros(t.timestamp_micros()))
                }
                ScalarImpl::Utf8(s) => Some(IcebergDatum::string(s)),
                ScalarImpl::Bytea(b) => Some(IcebergDatum::binary(b.clone())),
                _ => None,
            }
        }

        fn rw_expr_to_iceberg_predicate(
            expr: &ExprImpl,
            fields: &[Field],
        ) -> Option<IcebergPredicate> {
            match expr {
                ExprImpl::Literal(l) => match l.get_data() {
                    Some(ScalarImpl::Bool(b)) => {
                        if *b {
                            Some(IcebergPredicate::AlwaysTrue)
                        } else {
                            Some(IcebergPredicate::AlwaysFalse)
                        }
                    }
                    _ => None,
                },
                ExprImpl::FunctionCall(f) => {
                    let args = f.inputs();
                    match f.func_type() {
                        ExprType::Not => {
                            let arg = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                            Some(IcebergPredicate::negate(arg))
                        }
                        ExprType::And => {
                            let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                            let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                            Some(IcebergPredicate::and(arg0, arg1))
                        }
                        ExprType::Or => {
                            let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                            let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                            Some(IcebergPredicate::or(arg0, arg1))
                        }
                        ExprType::Equal => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                            | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.equal_to(datum))
                            }
                            _ => None,
                        },
                        ExprType::NotEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                            | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.not_equal_to(datum))
                            }
                            _ => None,
                        },
                        ExprType::GreaterThan => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.greater_than(datum))
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.less_than_or_equal_to(datum))
                            }
                            _ => None,
                        },
                        ExprType::GreaterThanOrEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.greater_than_or_equal_to(datum))
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.less_than(datum))
                            }
                            _ => None,
                        },
                        ExprType::LessThan => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.less_than(datum))
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.greater_than_or_equal_to(datum))
                            }
                            _ => None,
                        },
                        ExprType::LessThanOrEqual => match [&args[0], &args[1]] {
                            [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.less_than_or_equal_to(datum))
                            }
                            [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                let datum = rw_literal_to_iceberg_datum(rhs)?;
                                Some(reference.greater_than(datum))
                            }
                            _ => None,
                        },
                        ExprType::IsNull => match &args[0] {
                            ExprImpl::InputRef(lhs) => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                Some(reference.is_null())
                            }
                            _ => None,
                        },
                        ExprType::IsNotNull => match &args[1] {
                            ExprImpl::InputRef(lhs) => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
                                Some(reference.is_not_null())
                            }
                            _ => None,
                        },
                        ExprType::In => match &args[1] {
                            ExprImpl::InputRef(lhs) => {
                                let column_name = &fields[lhs.index].name;
                                let reference = Reference::new(column_name);
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
                                Some(reference.is_in(datums))
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
        ) -> (IcebergPredicate, Condition) {
            if predicate.always_true() {
                return (IcebergPredicate::AlwaysTrue, predicate);
            }

            let mut conjunctions = predicate.conjunctions;
            let mut ignored_conjunctions: Vec<ExprImpl> = Vec::with_capacity(conjunctions.len());

            let mut iceberg_condition_root = None;
            while let Some(conjunction) = conjunctions.pop() {
                match rw_expr_to_iceberg_predicate(&conjunction, fields) {
                    iceberg_predicate @ Some(_) => {
                        iceberg_condition_root = iceberg_predicate;
                        break;
                    }
                    None => {
                        ignored_conjunctions.push(conjunction);
                        continue;
                    }
                }
            }

            let mut iceberg_condition_root = match iceberg_condition_root {
                Some(p) => p,
                None => {
                    return (
                        IcebergPredicate::AlwaysTrue,
                        Condition {
                            conjunctions: ignored_conjunctions,
                        },
                    )
                }
            };

            for rw_condition in conjunctions {
                match rw_expr_to_iceberg_predicate(&rw_condition, fields) {
                    Some(iceberg_predicate) => {
                        iceberg_condition_root = iceberg_condition_root.and(iceberg_predicate)
                    }
                    None => ignored_conjunctions.push(rw_condition),
                }
            }
            (
                iceberg_condition_root,
                Condition {
                    conjunctions: ignored_conjunctions,
                },
            )
        }

        let schema = self.schema();
        let fields = &schema.fields;
        let (iceberg_predicate, rw_predicate) =
            rw_predicate_to_iceberg_predicate(predicate, fields);
        // No pushdown.
        let this = self.clone_with_predicate(iceberg_predicate);
        LogicalFilter::create(this.into(), rw_predicate)
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
