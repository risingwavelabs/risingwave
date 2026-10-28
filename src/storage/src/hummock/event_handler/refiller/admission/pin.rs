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

//! Pin input adaptation: selected plans, surviving receipt-time intent and object dependencies.
//! Request selection, metadata guards and executor submission stay in `refiller::pin`.

use std::collections::HashSet;

use risingwave_pb::id::TableId;

use super::select_local_blocks;
use crate::hummock::TableHolder;
use crate::hummock::pin_cache_refill::PinCacheRefillObject;
use crate::hummock::refill_locality::vnode_range_overlaps_bitmap;
use crate::hummock::table_cache_metadata::TableCacheMetadata;

/// Copies only owned, uncached candidates into metadata preparation. The caller rejects
/// cached objects first; the original queued intent remains available for policy withdrawal.
pub(in super::super) fn select_owned_candidates<'a>(
    candidates: impl Iterator<Item = &'a PinCacheRefillObject>,
    metadata: &TableCacheMetadata,
) -> Vec<PinCacheRefillObject> {
    candidates
        .filter_map(|candidate| {
            let admitted_tables: HashSet<_> = candidate
                .admitted_tables
                .iter()
                .copied()
                .filter(|table| metadata.has_vnodes(table))
                .collect();
            if admitted_tables.is_empty() {
                return None;
            }
            Some(PinCacheRefillObject {
                source_sst: candidate.source_sst.clone(),
                admitted_tables,
            })
        })
        .collect()
}

/// Consumes prepared inputs and yields only admitted objects, with cancellation dependencies
/// narrowed to local projections. The caller holds the metadata guard through submission;
/// surviving tables come from the selected requests so RESET then SET cannot revive intent.
pub(in super::super) fn iter_admitted_objects<'a>(
    prepared: Vec<(PinCacheRefillObject, TableHolder)>,
    metadata: &'a TableCacheMetadata,
    surviving_tables: impl IntoIterator<Item = TableId>,
) -> impl Iterator<Item = PinCacheRefillObject> + 'a {
    let surviving_tables: HashSet<_> = surviving_tables
        .into_iter()
        .filter(|table| metadata.is_pinned(table))
        .collect();
    prepared
        .into_iter()
        .filter_map(move |(mut candidate, sst)| {
            let locality = select_local_blocks(
                sst,
                |table| {
                    candidate.admitted_tables.contains(table) && surviving_tables.contains(table)
                },
                |table, range| {
                    metadata
                        .owned_vnodes(table)
                        .any(|bitmap| vnode_range_overlaps_bitmap(range, bitmap))
                },
            );
            if !locality.blocks.any() {
                return None;
            }
            candidate.admitted_tables.clear();
            candidate.admitted_tables.extend(
                locality
                    .blocks
                    .iter_ones()
                    .map(|index| locality.sst.meta.block_metas[index].table_id()),
            );
            Some(candidate)
        })
}
