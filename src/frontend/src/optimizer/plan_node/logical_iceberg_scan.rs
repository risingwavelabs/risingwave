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

use std::rc::Rc;

use chrono::Datelike;
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
use iceberg::spec::Datum as IcebergDatum;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId, Field};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, LogicalProject, PlanBase,
    PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{Expr, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::stream_dynamic_filter::ExprType;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, ExprImpl, LogicalFilter, LogicalSource,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Debug, Clone)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    pub iceberg_scan_type: IcebergScanType,
    pub predicate: Condition,
    pub snapshot_id: Option<i64>,
}

impl PartialEq for LogicalIcebergScan {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base
            && self.core == other.core
            && self.iceberg_scan_type == other.iceberg_scan_type
            && self.snapshot_id == other.snapshot_id
            && self.predicate == other.predicate
    }
}

impl Eq for LogicalIcebergScan {}

impl std::hash::Hash for LogicalIcebergScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.base.hash(state);
        self.core.hash(state);
        self.iceberg_scan_type.hash(state);
        self.snapshot_id.hash(state);
        self.predicate.hash(state);
    }
}

impl LogicalIcebergScan {
    pub fn new(
        logical_source: &LogicalSource,
        iceberg_scan_type: IcebergScanType,
        snapshot_id: Option<i64>,
    ) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let core = logical_source.core.clone();
        let base = PlanBase::new_logical_with_core(&core);

        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergScan {
            base,
            core,
            iceberg_scan_type,
            predicate: Condition::true_cond(),
            snapshot_id,
        }
    }

    pub fn iceberg_scan_type(&self) -> IcebergScanType {
        self.iceberg_scan_type
    }

    pub fn predicate(&self) -> Condition {
        self.predicate.clone()
    }

    pub fn clone_with_predicate(&self, predicate: Condition) -> Self {
        Self {
            base: self.base.clone(),
            core: self.core.clone(),
            iceberg_scan_type: self.iceberg_scan_type,
            predicate,
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn new_count_star_with_logical_iceberg_scan(
        logical_iceberg_scan: &LogicalIcebergScan,
    ) -> Self {
        let mut core = logical_iceberg_scan.core.clone();
        core.column_catalog = vec![ColumnCatalog::visible(ColumnDesc::named(
            "count",
            ColumnId::first_user_column(),
            DataType::Int64,
        ))];
        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergScan {
            base,
            core,
            iceberg_scan_type: IcebergScanType::CountStar,
            predicate: Condition::true_cond(),
            snapshot_id: logical_iceberg_scan.snapshot_id,
        }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());
        let mut core = self.core.clone();
        let mut has_row_id = false;
        core.column_catalog = required_cols
            .iter()
            .map(|idx| {
                if Some(*idx) == core.row_id_index {
                    has_row_id = true;
                }
                core.column_catalog[*idx].clone()
            })
            .collect();
        if !has_row_id {
            core.row_id_index = None;
        }
        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergScan {
            base,
            core,
            iceberg_scan_type: self.iceberg_scan_type,
            predicate: self.predicate.clone(),
            snapshot_id: self.snapshot_id,
        }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                ("iceberg_scan_type", Pretty::debug(&self.iceberg_scan_type)),
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

impl ExprRewritable<Logical> for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let iceberg_predicate =
            rw_predicate_to_iceberg_predicate(self.predicate.clone(), self.schema().fields())?;
        let plan = BatchIcebergScan::new(
            self.core.clone(),
            self.iceberg_scan_type,
            self.snapshot_id,
            iceberg_predicate,
        )
        .into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}

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
        ScalarImpl::Decimal(_) => {
            // TODO(iceberg): iceberg-rust doesn't support decimal predicate pushdown yet.
            None
        }
        ScalarImpl::Date(d) => {
            let Ok(datum) = IcebergDatum::date_from_ymd(d.0.year(), d.0.month(), d.0.day()) else {
                return None;
            };
            Some(datum)
        }
        ScalarImpl::Timestamp(t) => Some(IcebergDatum::timestamp_micros(
            t.0.and_utc().timestamp_micros(),
        )),
        ScalarImpl::Timestamptz(t) => Some(IcebergDatum::timestamptz_micros(t.timestamp_micros())),
        ScalarImpl::Utf8(s) => Some(IcebergDatum::string(s)),
        ScalarImpl::Bytea(b) => Some(IcebergDatum::binary(b.clone())),
        _ => None,
    }
}

fn rw_expr_to_iceberg_predicate(expr: &ExprImpl, fields: &[Field]) -> Option<IcebergPredicate> {
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
                ExprType::Equal if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                        | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::NotEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                        | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.not_equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::GreaterThan if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
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
                    }
                }
                ExprType::GreaterThanOrEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
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
                    }
                }
                ExprType::LessThan if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
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
                    }
                }
                ExprType::LessThanOrEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
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
                    }
                }
                ExprType::IsNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        Some(reference.is_null())
                    }
                    _ => None,
                },
                ExprType::IsNotNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        Some(reference.is_not_null())
                    }
                    _ => None,
                },
                ExprType::In => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        let mut datums = Vec::with_capacity(args.len() - 1);
                        for arg in &args[1..] {
                            if args[0].return_type() != arg.return_type() {
                                return None;
                            }
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
) -> Result<IcebergPredicate> {
    if predicate.always_true() {
        return Ok(IcebergPredicate::AlwaysTrue);
    }

    let mut conjunctions = predicate.conjunctions;

    let mut iceberg_condition_root = conjunctions
        .pop()
        .and_then(|conjunction| rw_expr_to_iceberg_predicate(&conjunction, fields))
        .ok_or_else(|| anyhow::anyhow!("No iceberg predicate could be pushed down"))?;

    while let Some(conjunction) = conjunctions.pop() {
        match rw_expr_to_iceberg_predicate(&conjunction, fields) {
            Some(iceberg_predicate) => {
                iceberg_condition_root = iceberg_condition_root.and(iceberg_predicate);
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Rw iceberg scan predicate must be convert to iceberg predicate"
                )
                .into());
            }
        }
    }
    Ok(iceberg_condition_root)
}
