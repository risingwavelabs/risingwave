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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::{ColumnOrder, ColumnOrderDisplay};
use risingwave_expr::function::window::{Frame, WindowFuncKind};
use risingwave_pb::expr::PbWindowFunction;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{InputRef, InputRefDisplay};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;
use crate::OptimizerContextRef;

/// Rewritten version of [`WindowFunction`] which uses `InputRef` instead of `ExprImpl`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanWindowFunction {
    pub kind: WindowFuncKind,
    pub return_type: DataType,
    pub args: Vec<InputRef>,
    pub partition_by: Vec<InputRef>,
    pub order_by: Vec<ColumnOrder>,
    pub frame: Frame,
}

struct PlanWindowFunctionDisplay<'a> {
    pub window_function: &'a PlanWindowFunction,
    pub input_schema: &'a Schema,
}

impl<'a> std::fmt::Debug for PlanWindowFunctionDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let window_function = self.window_function;
        if f.alternate() {
            f.debug_struct("WindowFunction")
                .field("kind", &window_function.kind)
                .field("return_type", &window_function.return_type)
                .field("args", &window_function.args)
                .field("partition_by", &window_function.partition_by)
                .field("order_by", &window_function.order_by)
                .field("frame", &window_function.frame)
                .finish()
        } else {
            write!(f, "{}(", window_function.kind)?;
            let mut delim = "";
            for arg in &window_function.args {
                write!(f, "{}", delim)?;
                delim = ", ";
                write!(
                    f,
                    "{}",
                    InputRefDisplay {
                        input_ref: arg,
                        input_schema: self.input_schema
                    }
                )?;
            }
            write!(f, ") OVER(")?;
            let mut delim = "";
            if !window_function.partition_by.is_empty() {
                delim = " ";
                write!(
                    f,
                    "PARTITION BY {}",
                    window_function
                        .partition_by
                        .iter()
                        .format_with(", ", |input_ref, f| {
                            f(&InputRefDisplay {
                                input_ref,
                                input_schema: self.input_schema,
                            })
                        })
                )?;
            }
            if !window_function.order_by.is_empty() {
                write!(
                    f,
                    "{delim}ORDER BY {}",
                    window_function.order_by.iter().format_with(", ", |o, f| {
                        f(&ColumnOrderDisplay {
                            column_order: o,
                            input_schema: self.input_schema,
                        })
                    })
                )?;
            }
            write!(f, "{delim}{}", window_function.frame)?;
            f.write_str(")")?;

            Ok(())
        }
    }
}

impl PlanWindowFunction {
    pub fn to_protobuf(&self) -> PbWindowFunction {
        use risingwave_pb::expr::window_function::{PbGeneralType, PbType};
        use WindowFuncKind::*;

        let r#type = match self.kind {
            RowNumber => PbType::General(PbGeneralType::RowNumber as _),
            Rank => PbType::General(PbGeneralType::Rank as _),
            DenseRank => PbType::General(PbGeneralType::DenseRank as _),
            Lag => PbType::General(PbGeneralType::Lag as _),
            Lead => PbType::General(PbGeneralType::Lead as _),
            Aggregate(agg_kind) => PbType::Aggregate(agg_kind.to_protobuf() as _),
        };

        PbWindowFunction {
            r#type: Some(r#type),
            args: self.args.iter().map(InputRef::to_proto).collect(),
            return_type: Some(self.return_type.to_protobuf()),
            frame: Some(self.frame.to_protobuf()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OverWindow<PlanRef> {
    pub window_functions: Vec<PlanWindowFunction>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> OverWindow<PlanRef> {
    pub fn new(window_functions: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        Self {
            window_functions,
            input,
        }
    }

    pub fn output_len(&self) -> usize {
        self.input.schema().len() + self.window_functions.len()
    }

    pub fn funcs_have_same_partition_and_order(&self) -> bool {
        self.window_functions
            .iter()
            .map(|f| (&f.partition_by, &f.order_by))
            .all_equal()
    }
}

impl<PlanRef: GenericPlanRef> OverWindow<PlanRef> {
    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        let window_funcs_display = self
            .window_functions
            .iter()
            .map(|func| PlanWindowFunctionDisplay {
                window_function: func,
                input_schema: self.input.schema(),
            })
            .collect::<Vec<_>>();
        builder.field("window_functions", &window_funcs_display);
        builder.finish()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for OverWindow<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        let mapping =
            ColIndexMapping::identity_or_none(self.input.schema().len(), self.output_len());
        let fd_set = self.input.functional_dependency().clone();
        mapping.rewrite_functional_dependency_set(fd_set)
    }

    fn schema(&self) -> Schema {
        let mut schema = self.input.schema().clone();
        self.window_functions.iter().for_each(|call| {
            schema.fields.push(Field::with_name(
                call.return_type.clone(),
                call.kind.to_string(),
            ));
        });
        schema
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}
