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
//! Batch selection, metadata guards and executor submission stay in `refiller::pin`.

use std::collections::HashMap;

use futures::future::try_join_all;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use super::{TableLocality, load_sst_metadata, select_local_blocks};
use crate::hummock::pin_cache_refill::{PinCacheRefillObject, PinCacheRefillPlan};
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};

/// Coarse admission at selection, before metadata preparation allocates work.
pub(in super::super) fn retain_owned_candidates(
    plan: &mut PinCacheRefillPlan,
    metadata: &TableCacheMetadata,
) {
    plan.objects.retain(|_, candidate| {
        candidate
            .admitted_tables
            .retain(|table| metadata.ownership().has_vnodes(table));
        !candidate.admitted_tables.is_empty()
    });
}

/// Loads metadata only after Pin selects its endpoint and discards superseded,
/// unowned and cached candidates. Final admission follows under current metadata.
pub(in super::super) async fn load_metadata(
    store: &SstableStoreRef,
    concurrency: Option<&Semaphore>,
    plan: PinCacheRefillPlan,
) -> HummockResult<Vec<(PinCacheRefillObject, TableHolder)>> {
    try_join_all(plan.objects.into_values().map(|candidate| async move {
        let sst = load_sst_metadata(store, &candidate.source_sst, concurrency).await?;
        Ok((candidate, sst))
    }))
    .await
}

/// Consumes prepared inputs and yields only admitted objects, with cancellation dependencies
/// narrowed to local projections. The caller holds the metadata guard through submission;
/// surviving tables come from the selected requests so RESET then SET cannot revive intent.
pub(in super::super) fn iter_admitted_objects<'a>(
    prepared: Vec<(PinCacheRefillObject, TableHolder)>,
    metadata: &'a TableCacheMetadata,
    surviving_tables: impl IntoIterator<Item = TableId>,
) -> impl Iterator<Item = PinCacheRefillObject> + 'a {
    let contexts = pin_refill_contexts(metadata, surviving_tables);
    prepared
        .into_iter()
        .filter_map(move |(mut candidate, sst)| {
            let locality = select_local_blocks(
                sst,
                |table| candidate.admitted_tables.contains(table),
                |table| contexts.get(table).copied(),
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

/// Pin enablement is independent of the ordinary refill policy. Capture one context per
/// surviving table after metadata I/O; borrow under the same guard as download submission.
fn pin_refill_contexts(
    metadata: &TableCacheMetadata,
    tables: impl IntoIterator<Item = TableId>,
) -> HashMap<TableId, TableLocality<'_>> {
    let mut contexts = HashMap::new();
    for table in tables {
        if metadata.is_pinned(&table) && metadata.ownership().has_vnodes(&table) {
            contexts.entry(table).or_insert_with(|| TableLocality {
                policy: CacheRefillPolicy::Both,
                streaming_vnode_bitmap: metadata
                    .ownership()
                    .streaming
                    .and_then(|map| map.get(&table)),
                serving_vnode_bitmap: metadata.ownership().serving.and_then(|map| map.get(&table)),
            });
        }
    }
    contexts
}
