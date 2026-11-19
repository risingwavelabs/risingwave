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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::stream_plan::multiway_delta_index_join_node::{
    PbArrangement, PbLookupPath, PbLookupStep,
};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ArrangementInfo, MultiwayDeltaIndexJoinNode};

use super::stream::prelude::*;
use super::stream_delta_join::build_lookup_visibility_catalog;
use super::utils::{Distill, childless_record};
use super::{
    BackfillType, ExprRewritable, PlanBase, PlanTreeNode, StreamPlanRef as PlanRef, TryToStreamPb,
};
use crate::error::{ErrorCode, Result};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{
    Distribution, FunctionalDependencySet, MonotonicityMap, WatermarkColumns,
};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// A lookup of an arrangement in a [`MultiwayDeltaJoinPath`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiwayDeltaJoinLookup {
    /// The arrangement to look up, as an index into the inputs of [`StreamMultiwayDeltaJoin`].
    pub arrangement: usize,
    /// Join key of the stream side, as column indices of the rows joined so far on the path.
    pub stream_key: Vec<usize>,
    /// Join key of the arrangement side, as column indices of the arrangement scan. It is the
    /// distribution key of the arrangement, in the same order.
    pub arrange_key: Vec<usize>,
    /// Whether to look up the arrangement of the current epoch instead of the previous epoch.
    pub use_current_epoch: bool,
}

/// The changes of one join input, joined with all other join inputs by a chain of lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiwayDeltaJoinPath {
    /// The arrangement whose changes drive this path, as an index into the inputs of
    /// [`StreamMultiwayDeltaJoin`].
    pub stream_arrangement: usize,
    pub lookups: Vec<MultiwayDeltaJoinLookup>,
}

/// [`StreamMultiwayDeltaJoin`] implements an inner equi-join of three or more inputs with delta
/// join. Every join input is read from one or more arrangements (indexes or primary tables), which
/// are the inputs of this plan node.
///
/// For each join input there is a path, which looks up the arrangements of all other join inputs
/// for every change of the join input. Take `A join B join C` as an example, where `dA` means the
/// changes of `A` in an epoch, `A` means the arrangement of the previous epoch, and `A'` means the
/// arrangement of the current epoch:
///
/// ```plain
/// d(A join B join C) = dC join A  join B
///                    + dB join A  join C'
///                    + dA join B' join C'
/// ```
///
/// The paths are emitted in the order above, so that every path produces the changes from the
/// result of the previous path, and the output is a consistent change stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamMultiwayDeltaJoin {
    pub base: PlanBase<Stream>,
    /// Scans of the arrangements. Every one of them is a `StreamTableScan`.
    arrangements: Vec<PlanRef>,
    /// The join input of each arrangement.
    arrangement_inputs: Vec<usize>,
    /// The number of columns of each join input.
    input_widths: Vec<usize>,
    /// The lookup paths, in the order their outputs are emitted.
    paths: Vec<MultiwayDeltaJoinPath>,
    /// The equi-join conditions, as pairs of column indices of the concatenation of all join
    /// inputs. Only used for explaining.
    eq_conds: Vec<(usize, usize)>,
    /// The output columns, as column indices of the concatenation of all join inputs.
    output_indices: Vec<usize>,
    /// The hash distribution key of the output. The outputs of the paths are shuffled by it before
    /// being merged. If `None`, they are shuffled by the stream key, and the output distribution is
    /// unspecified.
    dist_key: Option<Vec<usize>>,
}

impl StreamMultiwayDeltaJoin {
    pub fn new(
        arrangements: Vec<PlanRef>,
        arrangement_inputs: Vec<usize>,
        paths: Vec<MultiwayDeltaJoinPath>,
        eq_conds: Vec<(usize, usize)>,
        output_indices: Vec<usize>,
        stream_key: Vec<usize>,
        dist_key: Option<Vec<usize>>,
    ) -> Result<Self> {
        assert_eq!(arrangements.len(), arrangement_inputs.len());
        assert!(
            arrangements
                .iter()
                .all(|a| a.as_stream_table_scan().is_some())
        );
        let input_num = arrangement_inputs.iter().max().unwrap() + 1;
        assert_eq!(paths.len(), input_num);

        let ctx = arrangements[0].ctx();

        // All arrangements of a join input have the same columns, so we take the first one.
        let input_schemas = (0..input_num)
            .map(|input| {
                let arrangement = arrangement_inputs
                    .iter()
                    .position(|&i| i == input)
                    .expect("every join input should have an arrangement");
                arrangements[arrangement].schema().clone()
            })
            .collect_vec();
        let input_widths = input_schemas.iter().map(|s| s.len()).collect_vec();
        let concat_fields = input_schemas
            .iter()
            .flat_map(|s| s.fields.iter().cloned())
            .collect_vec();
        let schema = Schema::new(
            output_indices
                .iter()
                .map(|&i| concat_fields[i].clone())
                .collect(),
        );

        let mut all_append_only = true;
        for arrangement in &arrangements {
            match arrangement.stream_kind() {
                StreamKind::AppendOnly => {}
                StreamKind::Retract => all_append_only = false,
                StreamKind::Upsert => {
                    return Err(ErrorCode::NotSupported(
                        "upsert input in delta join".to_owned(),
                        "Please use a different join strategy.".to_owned(),
                    )
                    .into());
                }
            }
        }
        let stream_kind = if all_append_only {
            StreamKind::AppendOnly
        } else {
            StreamKind::Retract
        };

        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_stream(
            ctx,
            schema,
            Some(stream_key),
            functional_dependency,
            match &dist_key {
                Some(dist_key) => Distribution::HashShard(dist_key.clone()),
                None => Distribution::SomeShard,
            },
            stream_kind,
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );

        Ok(Self {
            base,
            arrangements,
            arrangement_inputs,
            input_widths,
            paths,
            eq_conds,
            output_indices,
            dist_key,
        })
    }

    fn concat_field_names(&self) -> Vec<String> {
        (0..self.input_widths.len())
            .flat_map(|input| {
                let arrangement = self
                    .arrangement_inputs
                    .iter()
                    .position(|&i| i == input)
                    .unwrap();
                self.arrangements[arrangement]
                    .schema()
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect_vec()
            })
            .collect()
    }

    fn arrangement_name(&self, arrangement: usize) -> String {
        self.arrangements[arrangement]
            .as_stream_table_scan()
            .unwrap()
            .core()
            .table_name()
            .to_owned()
    }

    fn arrangement_info(&self, arrangement: usize) -> SchedulerResult<ArrangementInfo> {
        let scan = self.arrangements[arrangement]
            .as_stream_table_scan()
            .unwrap();
        let core = scan.core();
        let table_catalog = &*core.table_catalog;
        // The order key of the arrangement, as column indices of the scan output, so that the
        // lookup executor can match it with the arrangement join key. Primary key columns are
        // always in the scan output.
        let arrange_key_orders = table_catalog
            .pk
            .iter()
            .filter_map(|order| {
                let column_index = core
                    .output_col_idx
                    .iter()
                    .position(|&i| i == order.column_index)?;
                Some(ColumnOrder::new(column_index, order.order_type).to_protobuf())
            })
            .collect();
        Ok(ArrangementInfo {
            arrange_key_orders,
            column_descs: core
                .column_descs()
                .iter()
                .map(ColumnDesc::to_protobuf)
                .collect(),
            table_desc: Some(table_catalog.table_desc().try_to_protobuf()?),
            output_col_idx: core.output_col_idx.iter().map(|&v| v as u32).collect(),
        })
    }
}

impl Distill for StreamMultiwayDeltaJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let names = self.concat_field_names();

        let predicate = self
            .eq_conds
            .iter()
            .map(|&(l, r)| format!("{} = {}", names[l], names[r]))
            .join(" AND ");
        let lookups = self
            .paths
            .iter()
            .map(|path| {
                let mut s = self.arrangement_name(path.stream_arrangement);
                for lookup in &path.lookups {
                    s.push_str(" -> ");
                    s.push_str(&self.arrangement_name(lookup.arrangement));
                    if lookup.use_current_epoch {
                        s.push('*');
                    }
                }
                Pretty::from(s)
            })
            .collect();

        let mut vec = vec![
            ("type", Pretty::from("Inner")),
            ("predicate", Pretty::from(predicate)),
            ("lookups", Pretty::Array(lookups)),
        ];
        if verbose {
            let output = self
                .output_indices
                .iter()
                .map(|&i| Pretty::from(names[i].clone()))
                .collect();
            vec.push(("output", Pretty::Array(output)));
        }
        childless_record("StreamMultiwayDeltaJoin", vec)
    }
}

impl PlanTreeNode<Stream> for StreamMultiwayDeltaJoin {
    fn inputs(&self) -> smallvec::SmallVec<[PlanRef; 2]> {
        smallvec::SmallVec::from_vec(self.arrangements.clone())
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        Self::new(
            inputs.to_vec(),
            self.arrangement_inputs.clone(),
            self.paths.clone(),
            self.eq_conds.clone(),
            self.output_indices.clone(),
            self.base.stream_key().unwrap().to_vec(),
            self.dist_key.clone(),
        )
        .unwrap()
        .into()
    }
}

impl TryToStreamPb for StreamMultiwayDeltaJoin {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        let is_backfilled = |arrangement: usize| {
            self.arrangements[arrangement]
                .as_stream_table_scan()
                .unwrap()
                .backfill_type()
                == BackfillType::ArrangementBackfill
        };
        let arrangements = (0..self.arrangements.len())
            .map(|arrangement| {
                let scan = self.arrangements[arrangement]
                    .as_stream_table_scan()
                    .unwrap();
                Ok(PbArrangement {
                    input: self.arrangement_inputs[arrangement] as u32,
                    table_id: scan.core().table_catalog.id,
                    info: Some(self.arrangement_info(arrangement)?),
                })
            })
            .collect::<SchedulerResult<_>>()?;
        let paths = self
            .paths
            .iter()
            .map(|path| PbLookupPath {
                stream_arrangement: path.stream_arrangement as u32,
                steps: path
                    .lookups
                    .iter()
                    .map(|lookup| PbLookupStep {
                        arrangement: lookup.arrangement as u32,
                        stream_key: lookup.stream_key.iter().map(|&i| i as u32).collect(),
                        arrange_key: lookup.arrange_key.iter().map(|&i| i as u32).collect(),
                        use_current_epoch: lookup.use_current_epoch,
                        // A lookup of the backfilled arrangement only sees the rows the backfill
                        // has emitted.
                        visibility_table: is_backfilled(lookup.arrangement).then(|| {
                            build_lookup_visibility_catalog(state).to_internal_table_prost()
                        }),
                    })
                    .collect(),
            })
            .collect();
        Ok(NodeBody::MultiwayDeltaIndexJoin(Box::new(
            MultiwayDeltaIndexJoinNode {
                arrangements,
                paths,
                output_indices: self.output_indices.iter().map(|&i| i as u32).collect(),
                dist_key: self.dist_key.iter().flatten().map(|&i| i as u32).collect(),
            },
        )))
    }
}

impl ExprRewritable<Stream> for StreamMultiwayDeltaJoin {}

impl ExprVisitable for StreamMultiwayDeltaJoin {}
