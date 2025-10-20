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

use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::plan_common::PbVectorIndexReaderDesc;
use risingwave_sqlparser::ast::AsOf;

use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::expr::{ExprDisplay, ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, educe::Educe)]
#[educe(Hash, PartialEq, Eq)]
pub struct VectorIndexLookupJoin<PlanRef> {
    pub input: PlanRef,
    pub top_n: u64,
    pub distance_type: PbDistanceType,
    pub index_name: String,
    pub index_table_id: TableId,
    pub info_column_desc: Vec<ColumnDesc>,
    pub info_output_indices: Vec<usize>,
    pub include_distance: bool,
    pub as_of: Option<AsOf>,

    pub vector_column_idx: usize,
    pub hnsw_ef_search: Option<usize>,
    #[educe(Hash(ignore), Eq(ignore))]
    pub ctx: OptimizerContextRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for VectorIndexLookupJoin<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        // TODO: copy the one of input, and extend with an extra column with no dependency
        FunctionalDependencySet::new(
            self.info_output_indices.len() + if self.include_distance { 1 } else { 0 },
        )
    }

    fn schema(&self) -> Schema {
        let mut schema = self.input.schema().clone();
        schema.fields.push(Field::new(
            "vector_info",
            DataType::list(
                StructType::new(
                    self.info_output_indices
                        .iter()
                        .map(|idx| {
                            (
                                self.info_column_desc[*idx].name.clone(),
                                self.info_column_desc[*idx].data_type.clone(),
                            )
                        })
                        .chain(
                            self.include_distance
                                .then(|| [("__distance".to_owned(), DataType::Float64)].into_iter())
                                .into_iter()
                                .flatten(),
                        ),
                )
                .into(),
            ),
        ));
        schema
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.input.stream_key().map(|key| key.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}

impl<PlanRef: GenericPlanRef> VectorIndexLookupJoin<PlanRef> {
    pub fn distill<'a>(&self) -> Vec<(&'static str, Pretty<'a>)> {
        let mut fields = vec![
            ("top_n", Pretty::debug(&self.top_n)),
            ("distance_type", Pretty::debug(&self.distance_type)),
            ("index_name", Pretty::debug(&self.index_name)),
            (
                "vector",
                Pretty::debug(&ExprDisplay {
                    expr: &ExprImpl::InputRef(
                        InputRef::new(
                            self.vector_column_idx,
                            self.input.schema()[self.vector_column_idx].data_type(),
                        )
                        .into(),
                    ),
                    input_schema: self.input.schema(),
                }),
            ),
            (
                "lookup_output",
                Pretty::Array(
                    self.info_output_indices
                        .iter()
                        .map(|idx| {
                            let col = &self.info_column_desc[*idx];
                            Pretty::debug(&(&col.name, &col.data_type))
                        })
                        .collect(),
                ),
            ),
            ("include_distance", Pretty::debug(&self.include_distance)),
        ];
        if let Some(hnsw_ef_search) = self.hnsw_ef_search {
            fields.push(("hnsw_ef_search", Pretty::debug(&hnsw_ef_search)));
        }
        if let Some(as_of) = &self.as_of {
            fields.push(("as_of", Pretty::debug(&as_of)));
        }
        fields
    }

    pub fn to_reader_desc(&self) -> PbVectorIndexReaderDesc {
        PbVectorIndexReaderDesc {
            table_id: self.index_table_id.table_id,
            info_column_desc: self
                .info_column_desc
                .iter()
                .map(|col| col.to_protobuf())
                .collect(),
            top_n: self.top_n as _,
            distance_type: self.distance_type as _,
            hnsw_ef_search: self.hnsw_ef_search.unwrap_or(0) as _,
            info_output_indices: self
                .info_output_indices
                .iter()
                .map(|&idx| idx as _)
                .collect(),
            include_distance: self.include_distance,
        }
    }
}
