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

use std::collections::HashMap;
use std::ops::Bound;

use anyhow::anyhow;
use futures::{StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{DefaultOrd, DefaultOrdered, ScalarImpl, ToOwnedDatum};
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::DummyExecutor;
use super::barrier_align::*;
use super::eowc::SortBuffer;
use super::join::{JoinType, JoinTypePrimitive};
use super::temporal_join::apply_indices_map;
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTable;
use crate::consistency::consistency_panic;
use crate::executor::prelude::*;
use crate::task::AtomicU64Ref;

type RightVersionMap = EstimatedBTreeMap<DefaultOrdered<ScalarImpl>, OwnedRow>;
type RightVersionCache = ManagedLruCache<OwnedRow, RightVersionMap>;
const RIGHT_CLEANUP_ANCHOR_BATCH_SIZE: usize = 4096;

pub struct EventTimeTemporalJoinExecutor<S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    #[expect(dead_code)]
    info: ExecutorInfo,
    metrics: Arc<StreamingMetrics>,
    left: Executor,
    right: Executor,
    left_pending_by_time: StateTable<S>,
    left_buffer: SortBuffer<S>,
    right_versions_by_key: StateTable<S>,
    right_versions_by_time: StateTable<S>,
    right_version_cache: RightVersionCache,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    left_event_time_key: usize,
    right_event_time_key: usize,
    chunk_size: usize,
    left_watermark: Option<Watermark>,
    right_watermark: Option<Watermark>,
    ready_watermark: Option<ScalarImpl>,
}

impl<S: StateStore, const T: JoinTypePrimitive> EventTimeTemporalJoinExecutor<S, T> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        metrics: Arc<StreamingMetrics>,
        left: Executor,
        right: Executor,
        left_pending_by_time: StateTable<S>,
        right_versions_by_key: StateTable<S>,
        right_versions_by_time: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        left_join_keys: Vec<usize>,
        right_join_keys: Vec<usize>,
        null_safe: Vec<bool>,
        condition: Option<NonStrictExpression>,
        output_indices: Vec<usize>,
        left_event_time_key: usize,
        right_event_time_key: usize,
        chunk_size: usize,
    ) -> Self {
        let metrics_info = MetricsInfo::new(
            metrics.clone(),
            right_versions_by_key.table_id(),
            ctx.id,
            "event-time temporal join right versions",
        );
        let right_version_cache = RightVersionCache::unbounded(watermark_epoch, metrics_info);
        let left_buffer = SortBuffer::new(left_event_time_key, &left_pending_by_time);

        Self {
            ctx,
            info,
            metrics,
            left,
            right,
            left_buffer,
            left_pending_by_time,
            right_versions_by_key,
            right_versions_by_time,
            right_version_cache,
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            left_event_time_key,
            right_event_time_key,
            chunk_size,
            left_watermark: None,
            right_watermark: None,
            ready_watermark: None,
        }
    }

    fn row_time(row: impl Row, idx: usize) -> Option<ScalarImpl> {
        row.datum_at(idx).to_owned_datum()
    }

    fn is_late(ts: &ScalarImpl, watermark: Option<&Watermark>) -> bool {
        watermark.is_some_and(|watermark| ts.default_cmp(&watermark.val).is_lt())
    }

    fn is_finalized(ts: &ScalarImpl, ready_watermark: Option<&ScalarImpl>) -> bool {
        ready_watermark.is_some_and(|watermark| ts.default_cmp(watermark).is_lt())
    }

    fn apply_left_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        for r in chunk.rows_with_holes() {
            let Some((op, row_ref)) = r else {
                continue;
            };
            let row = row_ref.into_owned_row();
            let Some(ts) = Self::row_time(&row, self.left_event_time_key) else {
                return Err(anyhow!(
                    "event-time temporal join left AS OF column should not be NULL"
                )
                .into());
            };
            if Self::is_finalized(&ts, self.ready_watermark.as_ref()) {
                continue;
            }
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.left_buffer.insert(row, &mut self.left_pending_by_time);
                }
                Op::Delete | Op::UpdateDelete => {
                    self.left_buffer.delete(row, &mut self.left_pending_by_time);
                }
            }
        }
        Ok(())
    }

    fn right_key_row(&self, right_row: impl Row, tombstone: bool) -> OwnedRow {
        right_row
            .chain(row::once(Some(ScalarImpl::Bool(tombstone))))
            .into_owned_row()
    }

    fn right_time_row(&self, right_row: impl Row) -> OwnedRow {
        let mut datums = vec![
            right_row
                .datum_at(self.right_event_time_key)
                .to_owned_datum(),
        ];
        datums.extend(
            self.right_join_keys
                .iter()
                .map(|idx| right_row.datum_at(*idx).to_owned_datum()),
        );
        OwnedRow::new(datums)
    }

    fn right_version_time(&self, right_row: impl Row) -> Option<DefaultOrdered<ScalarImpl>> {
        Self::right_version_time_at(self.right_event_time_key, right_row)
    }

    fn right_join_key(&self, right_row: impl Row) -> OwnedRow {
        Self::right_join_key_at(&self.right_join_keys, right_row)
    }

    fn right_version_time_at(
        right_event_time_key: usize,
        right_row: impl Row,
    ) -> Option<DefaultOrdered<ScalarImpl>> {
        Self::row_time(right_row, right_event_time_key).map(Into::into)
    }

    fn right_join_key_at(right_join_keys: &[usize], right_row: impl Row) -> OwnedRow {
        right_row.project(right_join_keys).into_owned_row()
    }

    fn insert_right_version_cache(&mut self, row: &OwnedRow) {
        let join_key = self.right_join_key(row);
        let Some(version_time) = self.right_version_time(row) else {
            return;
        };
        let right_key_row = self.right_key_row(row, false);
        if let Some(mut versions) = self.right_version_cache.get_mut(&join_key)
            && versions
                .insert(version_time.clone(), right_key_row)
                .is_some()
        {
            consistency_panic!(
                ?join_key,
                ?version_time,
                "double inserting an event-time temporal join right version cache entry"
            );
        }
    }

    fn delete_right_version_cache(&mut self, row: &OwnedRow) {
        let join_key = self.right_join_key(row);
        let Some(version_time) = self.right_version_time(row) else {
            return;
        };
        if let Some(mut versions) = self.right_version_cache.get_mut(&join_key)
            && versions.remove(&version_time).is_none()
        {
            consistency_panic!(
                ?join_key,
                ?version_time,
                "removing an event-time temporal join right version cache entry but it is not in the cache"
            );
        }
    }

    async fn refill_right_version_cache(
        right_event_time_key: usize,
        right_versions_by_key: &StateTable<S>,
        right_version_cache: &mut RightVersionCache,
        join_key: OwnedRow,
    ) -> StreamExecutorResult<()> {
        let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, Bound::Unbounded);
        let iter = right_versions_by_key
            .iter_with_prefix(join_key.clone(), &sub_range, PrefetchOptions::default())
            .await?;
        pin_mut!(iter);

        let mut versions = RightVersionMap::new();
        while let Some(row) = iter.try_next().await? {
            let row = row.into_owned_row();
            let Some(version_time) = Self::right_version_time_at(right_event_time_key, &row) else {
                continue;
            };
            if versions.insert(version_time.clone(), row).is_some() {
                consistency_panic!(
                    ?join_key,
                    ?version_time,
                    "double inserting an event-time temporal join right version cache entry while refilling"
                );
            }
        }
        right_version_cache.put(join_key, versions);
        Ok(())
    }

    fn apply_right_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        for r in chunk.rows_with_holes() {
            let Some((op, row_ref)) = r else {
                continue;
            };
            let row = row_ref.into_owned_row();
            let Some(ts) = Self::row_time(&row, self.right_event_time_key) else {
                return Err(anyhow!(
                    "event-time temporal join right version-time column should not be NULL"
                )
                .into());
            };
            if Self::is_late(&ts, self.right_watermark.as_ref()) {
                continue;
            }
            let right_time_row = self.right_time_row(&row);
            match op {
                Op::Insert | Op::UpdateInsert => {
                    let right_key_row = self.right_key_row(&row, false);
                    self.right_versions_by_key.insert(right_key_row);
                    self.right_versions_by_time.insert(right_time_row);
                    self.insert_right_version_cache(&row);
                }
                Op::Delete | Op::UpdateDelete => {
                    let right_key_row = self.right_key_row(&row, false);
                    self.right_versions_by_key.delete(right_key_row);
                    self.right_versions_by_time.delete(right_time_row);
                    self.delete_right_version_cache(&row);
                }
            }
        }
        Ok(())
    }

    fn active_right_row(row: OwnedRow) -> Option<OwnedRow> {
        let tombstone = row
            .datum_at(row.len() - 1)
            .map(|scalar| scalar.into_bool())
            .unwrap_or(false);
        let row_len = row.len();
        if tombstone {
            None
        } else {
            Some(row.slice(0..row_len - 1).into_owned_row())
        }
    }

    async fn lookup_right_version(
        left_join_keys: &[usize],
        null_safe: &[bool],
        left_event_time_key: usize,
        right_event_time_key: usize,
        right_versions_by_key: &StateTable<S>,
        right_version_cache: &mut RightVersionCache,
        left_row: impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if left_join_keys
            .iter()
            .zip_eq(null_safe.iter())
            .any(|(idx, null_safe)| !*null_safe && left_row.datum_at(*idx).is_none())
        {
            return Ok(None);
        }
        let Some(left_ts) = Self::row_time(&left_row, left_event_time_key) else {
            return Ok(None);
        };
        let prefix = left_row.project(left_join_keys).into_owned_row();
        if !right_version_cache.contains(&prefix) {
            Self::refill_right_version_cache(
                right_event_time_key,
                right_versions_by_key,
                right_version_cache,
                prefix.clone(),
            )
            .await?;
        }

        let version_time = DefaultOrdered::from(left_ts);
        let row = right_version_cache
            .get(&prefix)
            .expect("right version cache should be refilled")
            .range(..=version_time)
            .next_back()
            .map(|(_, row)| row.clone());

        Ok(row.and_then(Self::active_right_row))
    }

    async fn condition_matches(
        condition: Option<&NonStrictExpression>,
        left_row: &OwnedRow,
        right_row: &OwnedRow,
    ) -> bool {
        let Some(condition) = condition else {
            return true;
        };
        condition
            .eval_row_infallible(&left_row.clone().chain(right_row.clone()).into_owned_row())
            .await
            .is_some_and(|datum| datum.into_bool())
    }

    #[expect(clippy::too_many_arguments)]
    async fn append_joined_left_row(
        left_join_keys: &[usize],
        null_safe: &[bool],
        left_event_time_key: usize,
        right_event_time_key: usize,
        right_versions_by_key: &StateTable<S>,
        right_version_cache: &mut RightVersionCache,
        condition: Option<&NonStrictExpression>,
        output_indices: &[usize],
        right_size: usize,
        builder: &mut StreamChunkBuilder,
        left_row: OwnedRow,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let right_row = Self::lookup_right_version(
            left_join_keys,
            null_safe,
            left_event_time_key,
            right_event_time_key,
            right_versions_by_key,
            right_version_cache,
            &left_row,
        )
        .await?;
        let chunk = match right_row {
            Some(right_row) => {
                if Self::condition_matches(condition, &left_row, &right_row).await {
                    builder.append_row(Op::Insert, left_row.clone().chain(right_row))
                } else if T == JoinType::LeftOuter {
                    builder.append_row(
                        Op::Insert,
                        left_row.clone().chain(row::repeat_n(
                            risingwave_common::types::DatumRef::None,
                            right_size,
                        )),
                    )
                } else {
                    None
                }
            }
            None if T == JoinType::LeftOuter => builder.append_row(
                Op::Insert,
                left_row.clone().chain(row::repeat_n(
                    risingwave_common::types::DatumRef::None,
                    right_size,
                )),
            ),
            _ => None,
        };
        Ok(chunk.map(|chunk| apply_indices_map(chunk, output_indices)))
    }

    async fn scan_right_key_range(
        right_versions_by_key: &StateTable<S>,
        join_key: &OwnedRow,
        upper_bound: Bound<OwnedRow>,
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, upper_bound);
        let iter = right_versions_by_key
            .iter_with_prefix(
                join_key,
                &sub_range,
                PrefetchOptions::prefetch_for_small_range_scan(),
            )
            .await?;
        pin_mut!(iter);

        let mut rows = Vec::new();
        while let Some(row) = iter.try_next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    async fn scan_right_time_batch(
        right_versions_by_time: &StateTable<S>,
        vnode: VirtualNode,
        lower_bound: Bound<OwnedRow>,
        upper_bound: Bound<OwnedRow>,
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        let pk_range = (lower_bound, upper_bound);
        let iter = right_versions_by_time
            .iter_with_vnode(
                vnode,
                &pk_range,
                PrefetchOptions::prefetch_for_small_range_scan(),
            )
            .await?;
        pin_mut!(iter);

        let mut rows = Vec::with_capacity(RIGHT_CLEANUP_ANCHOR_BATCH_SIZE);
        while rows.len() < RIGHT_CLEANUP_ANCHOR_BATCH_SIZE {
            let Some(row) = iter.try_next().await? else {
                break;
            };
            rows.push(row);
        }
        Ok(rows)
    }

    async fn delete_right_versions_before_anchor(
        &mut self,
        join_key: OwnedRow,
        anchor_ts: ScalarImpl,
    ) -> StreamExecutorResult<()> {
        let upper_bound = Bound::Excluded(OwnedRow::new(vec![Some(anchor_ts)]));
        let rows =
            Self::scan_right_key_range(&self.right_versions_by_key, &join_key, upper_bound).await?;
        for row in rows {
            self.delete_right_version_cache(&row);
            self.right_versions_by_key.delete(row);
        }
        Ok(())
    }

    async fn delete_right_versions_before_time_rows(
        &mut self,
        right_time_rows: Vec<OwnedRow>,
    ) -> StreamExecutorResult<()> {
        let mut anchors: HashMap<OwnedRow, ScalarImpl> =
            HashMap::with_capacity(right_time_rows.len());
        for row in right_time_rows {
            let Some(ts) = row.datum_at(0).to_owned_datum() else {
                continue;
            };
            let join_key = row.slice(1..).into_owned_row();
            anchors
                .entry(join_key)
                .and_modify(|anchor_ts| {
                    if anchor_ts.default_cmp(&ts).is_lt() {
                        *anchor_ts = ts.clone();
                    }
                })
                .or_insert(ts);
        }

        for (join_key, anchor_ts) in anchors {
            self.delete_right_versions_before_anchor(join_key, anchor_ts)
                .await?;
        }
        Ok(())
    }

    async fn cleanup_right_versions(&mut self, watermark: ScalarImpl) -> StreamExecutorResult<()> {
        let initial_lower_bound = self
            .ready_watermark
            .as_ref()
            .map(|watermark| Bound::Included(OwnedRow::new(vec![Some(watermark.clone())])))
            .unwrap_or(Bound::Unbounded);
        let upper_bound = Bound::Excluded(OwnedRow::new(vec![Some(watermark.clone())]));
        let vnodes = self
            .right_versions_by_time
            .vnodes()
            .iter_vnodes()
            .collect_vec();
        for vnode in vnodes {
            let mut lower_bound = initial_lower_bound.clone();
            loop {
                let rows = Self::scan_right_time_batch(
                    &self.right_versions_by_time,
                    vnode,
                    lower_bound,
                    upper_bound.clone(),
                )
                .await?;
                if rows.is_empty() {
                    break;
                }

                let next_lower_bound =
                    Bound::Excluded(rows.last().expect("checked non-empty").clone());
                let is_synced = rows.len() < RIGHT_CLEANUP_ANCHOR_BATCH_SIZE;
                self.delete_right_versions_before_time_rows(rows).await?;
                if is_synced {
                    break;
                }
                lower_bound = next_lower_bound;
            }
        }
        self.right_versions_by_time.update_watermark(watermark);

        Ok(())
    }

    fn update_input_watermark(&mut self, is_left: bool, watermark: Watermark) -> Option<Watermark> {
        if is_left {
            if watermark.col_idx != self.left_event_time_key {
                return None;
            }
            self.left_watermark = Some(watermark);
        } else {
            if watermark.col_idx != self.right_event_time_key {
                return None;
            }
            self.right_watermark = Some(watermark);
        }

        let Some(left_watermark) = &self.left_watermark else {
            return None;
        };
        let Some(right_watermark) = &self.right_watermark else {
            return None;
        };
        let ready = if left_watermark.val.default_cmp(&right_watermark.val).is_le() {
            left_watermark.clone()
        } else {
            right_watermark.clone().with_idx(self.left_event_time_key)
        };
        if self
            .ready_watermark
            .as_ref()
            .is_some_and(|old| !old.default_cmp(&ready.val).is_lt())
        {
            return None;
        }

        Some(ready)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn emit_ready_watermark(
        &mut self,
        ready: Watermark,
        full_schema: Vec<DataType>,
        right_size: usize,
        output_watermark_col_idx: Option<usize>,
    ) {
        let mut builder = StreamChunkBuilder::new(self.chunk_size, full_schema);
        {
            let left_rows = self
                .left_buffer
                .consume(ready.val.clone(), &mut self.left_pending_by_time);
            pin_mut!(left_rows);
            while let Some(left_row) = left_rows.try_next().await? {
                if let Some(chunk) = Self::append_joined_left_row(
                    &self.left_join_keys,
                    &self.null_safe,
                    self.left_event_time_key,
                    self.right_event_time_key,
                    &self.right_versions_by_key,
                    &mut self.right_version_cache,
                    self.condition.as_ref(),
                    &self.output_indices,
                    right_size,
                    &mut builder,
                    left_row,
                )
                .await?
                {
                    yield Message::Chunk(chunk);
                }
            }
        }
        if let Some(chunk) = builder.take() {
            yield Message::Chunk(apply_indices_map(chunk, &self.output_indices));
        }
        self.cleanup_right_versions(ready.val.clone()).await?;
        self.ready_watermark = Some(ready.val.clone());
        if let Some(output_idx) = output_watermark_col_idx {
            yield Message::Watermark(ready.with_idx(output_idx));
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let left_schema = self.left.schema().clone();
        let right_schema = self.right.schema().clone();
        let right_size = right_schema.len();
        let full_schema = left_schema
            .data_types()
            .into_iter()
            .chain(right_schema.data_types().into_iter())
            .collect_vec();
        let output_watermark_col_idx = self
            .output_indices
            .iter()
            .position(|idx| *idx == self.left_event_time_key);

        let left_input = std::mem::replace(
            &mut self.left,
            Executor::new(ExecutorInfo::default(), Box::new(DummyExecutor)),
        );
        let right_input = std::mem::replace(
            &mut self.right,
            Executor::new(ExecutorInfo::default(), Box::new(DummyExecutor)),
        );
        let aligned_stream = barrier_align(
            left_input.execute(),
            right_input.execute(),
            self.ctx.id,
            self.ctx.fragment_id,
            self.metrics.clone(),
            "EventTime Temporal Join",
        );
        pin_mut!(aligned_stream);

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        let barrier_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.left_pending_by_time.init_epoch(barrier_epoch).await?;
        self.right_versions_by_key.init_epoch(barrier_epoch).await?;
        self.right_versions_by_time
            .init_epoch(barrier_epoch)
            .await?;
        self.left_buffer
            .refill_cache(None, &self.left_pending_by_time)
            .await?;

        while let Some(msg) = aligned_stream.next().await {
            self.right_version_cache.evict();
            match msg? {
                AlignedMessage::Left(chunk) => {
                    self.apply_left_chunk(chunk)?;
                }
                AlignedMessage::Right(chunk) => {
                    self.apply_right_chunk(chunk)?;
                }
                watermark_msg @ (AlignedMessage::WatermarkLeft(_)
                | AlignedMessage::WatermarkRight(_)) => {
                    let (is_left, watermark) = match watermark_msg {
                        AlignedMessage::WatermarkLeft(watermark) => (true, watermark),
                        AlignedMessage::WatermarkRight(watermark) => (false, watermark),
                        _ => unreachable!(),
                    };
                    if let Some(ready) = self.update_input_watermark(is_left, watermark) {
                        let messages = self.emit_ready_watermark(
                            ready,
                            full_schema.clone(),
                            right_size,
                            output_watermark_col_idx,
                        );
                        pin_mut!(messages);
                        while let Some(message) = messages.try_next().await? {
                            yield message;
                        }
                    }
                }
                AlignedMessage::Barrier(barrier) => {
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let left_time_post_commit =
                        self.left_pending_by_time.commit(barrier.epoch).await?;
                    let right_key_post_commit =
                        self.right_versions_by_key.commit(barrier.epoch).await?;
                    let right_time_post_commit =
                        self.right_versions_by_time.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                    let left_vnode_update = left_time_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?
                        .is_some();
                    if left_vnode_update {
                        self.left_buffer
                            .refill_cache(None, &self.left_pending_by_time)
                            .await?;
                    }
                    right_key_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    right_time_post_commit
                        .post_yield_barrier(update_vnode_bitmap)
                        .await?;
                }
            }
        }
    }
}

impl<S: StateStore, const T: JoinTypePrimitive> Execute for EventTimeTemporalJoinExecutor<S, T> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }
}
