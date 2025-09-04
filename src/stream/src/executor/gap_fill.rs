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

use futures::{StreamExt, stream};
use risingwave_common::array::Op;
use risingwave_common::gap_fill_types::FillStrategy;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, OwnedRow, Row};
use risingwave_common::types::{
    CheckedAdd, Datum, Decimal, ScalarImpl, ScalarRefImpl, ToOwnedDatum,
};
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;
use tracing::warn;

use crate::common::table::state_table::StateTable;
use crate::executor::prelude::*;

/// Arguments for creating `GapFillExecutor`
pub struct GapFillExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    pub schema: Schema,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: Vec<(usize, FillStrategy)>,
    pub gap_interval: NonStrictExpression,
    pub state_table: StateTable<S>,
}

/// Row metadata to track whether a row is original data or filled
#[derive(Debug, Clone, PartialEq)]
enum RowType {
    Original,
    Filled,
}

#[derive(Debug, Clone)]
struct ExtendedRow {
    row: OwnedRow,
    row_type: RowType,
}

pub struct GapFillExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: Vec<(usize, FillStrategy)>,
    gap_interval: NonStrictExpression,

    // State management
    state_table: StateTable<S>,
    // In-memory buffer for rows - kept sorted by time manually
    row_buffer: Vec<ExtendedRow>,
}

impl<S: StateStore> GapFillExecutor<S> {
    /// Compare two `ScalarImpl` values for ordering (used for time comparison)
    fn compare_scalars(a: &ScalarImpl, b: &ScalarImpl) -> std::cmp::Ordering {
        use risingwave_common::types::ScalarImpl::*;
        match (a, b) {
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Timestamptz(a), Timestamptz(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal, // Fallback for other types
        }
    }

    /// Convert a row to state table format by appending the `is_filled` flag
    fn row_to_state_row(row: &OwnedRow, is_filled: bool) -> OwnedRow {
        let mut state_row_data = row.as_inner().to_vec();
        state_row_data.push(Some(ScalarImpl::Bool(is_filled)));
        OwnedRow::new(state_row_data)
    }

    /// Convert a state table row back to output format by removing the `is_filled` flag
    fn state_row_to_output_row(state_row: &OwnedRow) -> OwnedRow {
        let mut output_data = state_row.as_inner().to_vec();
        output_data.pop(); // Remove the last column (is_filled flag)
        OwnedRow::new(output_data)
    }

    /// Extract the `is_filled` flag from a state table row
    fn extract_is_filled_flag(state_row: &OwnedRow) -> bool {
        let last_idx = state_row.len() - 1;
        if let Some(ScalarImpl::Bool(is_filled)) = state_row
            .datum_at(last_idx)
            .and_then(|d| d.to_owned_datum())
        {
            is_filled
        } else {
            false // Default to false if flag is missing or invalid
        }
    }

    fn calculate_step(d1: Datum, d2: Datum, steps: usize) -> Datum {
        let (Some(s1), Some(s2)) = (d1, d2) else {
            return None;
        };
        if steps == 0 {
            return None;
        }
        match (ScalarRefImpl::from(&s1), ScalarRefImpl::from(&s2)) {
            (ScalarRefImpl::Int16(v1), ScalarRefImpl::Int16(v2)) => {
                Some(ScalarImpl::Int16((v2 - v1) / steps as i16))
            }
            (ScalarRefImpl::Int32(v1), ScalarRefImpl::Int32(v2)) => {
                Some(ScalarImpl::Int32((v2 - v1) / steps as i32))
            }
            (ScalarRefImpl::Int64(v1), ScalarRefImpl::Int64(v2)) => {
                Some(ScalarImpl::Int64((v2 - v1) / steps as i64))
            }
            (ScalarRefImpl::Float32(v1), ScalarRefImpl::Float32(v2)) => {
                Some(ScalarImpl::Float32((v2 - v1) / steps as f32))
            }
            (ScalarRefImpl::Float64(v1), ScalarRefImpl::Float64(v2)) => {
                Some(ScalarImpl::Float64((v2 - v1) / steps as f64))
            }
            (ScalarRefImpl::Decimal(v1), ScalarRefImpl::Decimal(v2)) => {
                Some(ScalarImpl::Decimal((v2 - v1) / Decimal::from(steps)))
            }
            _ => None,
        }
    }

    fn apply_step(current: &mut Datum, step: &ScalarImpl) {
        if let Some(curr) = current.as_mut() {
            match (curr, step) {
                (ScalarImpl::Int16(v1), &ScalarImpl::Int16(v2)) => *v1 += v2,
                (ScalarImpl::Int32(v1), &ScalarImpl::Int32(v2)) => *v1 += v2,
                (ScalarImpl::Int64(v1), &ScalarImpl::Int64(v2)) => *v1 += v2,
                (ScalarImpl::Float32(v1), &ScalarImpl::Float32(v2)) => *v1 += v2,
                (ScalarImpl::Float64(v1), &ScalarImpl::Float64(v2)) => *v1 += v2,
                (ScalarImpl::Decimal(v1), &ScalarImpl::Decimal(v2)) => *v1 = *v1 + v2,
                _ => (),
            }
        }
    }

    /// Find position to insert a new time value to keep the buffer sorted (static version)
    fn find_insert_position_static(
        row_buffer: &[ExtendedRow],
        time_value: &OwnedRow,
        time_column_index: usize,
    ) -> usize {
        let time_datum = time_value.datum_at(time_column_index);
        if let Some(time_scalar) = time_datum {
            // Convert to ScalarImpl for comparison
            let time_impl = time_scalar.to_owned_datum().unwrap();

            row_buffer
                .binary_search_by(|extended_row| {
                    if let Some(existing_time) = extended_row.row.datum_at(time_column_index)
                        && let Some(existing_impl) = existing_time.to_owned_datum()
                    {
                        return Self::compare_scalars(&existing_impl, &time_impl);
                    }
                    std::cmp::Ordering::Equal
                })
                .unwrap_or_else(|pos| pos)
        } else {
            row_buffer.len()
        }
    }

    pub fn new(args: GapFillExecutorArgs<S>) -> Self {
        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            time_column_index: args.time_column_index,
            fill_columns: args.fill_columns,
            gap_interval: args.gap_interval,
            state_table: args.state_table,
            row_buffer: Vec::new(),
        }
    }

    fn generate_filled_rows_between_static(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        interval: &risingwave_common::types::Interval,
        time_column_index: usize,
        fill_columns: &[(usize, FillStrategy)],
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        let mut filled_rows = Vec::new();

        let (Some(prev_time_scalar), Some(curr_time_scalar)) = (
            prev_row.datum_at(time_column_index),
            curr_row.datum_at(time_column_index),
        ) else {
            return Ok(filled_rows);
        };

        let prev_time = match prev_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", prev_time_scalar);
                return Ok(filled_rows);
            }
        };

        let curr_time = match curr_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", curr_time_scalar);
                return Ok(filled_rows);
            }
        };

        if prev_time >= curr_time {
            return Ok(filled_rows);
        }

        // Generate template row with fill strategies
        let mut fill_values: Vec<Datum> = Vec::with_capacity(prev_row.len());
        for i in 0..prev_row.len() {
            if i == time_column_index {
                fill_values.push(None); // Will be set later
            } else if let Some((_, strategy)) = fill_columns.iter().find(|(col, _)| *col == i) {
                match strategy {
                    FillStrategy::Locf | FillStrategy::Interpolate => {
                        fill_values.push(prev_row.datum_at(i).to_owned_datum())
                    }
                    FillStrategy::Null => fill_values.push(None),
                }
            } else {
                fill_values.push(prev_row.datum_at(i).to_owned_datum());
            }
        }

        // Generate filled timestamps
        let mut fill_time = match prev_time.checked_add(*interval) {
            Some(t) => t,
            None => return Ok(filled_rows),
        };

        let mut data = Vec::new();
        while fill_time < curr_time {
            let mut new_row_data = fill_values.clone();
            let fill_time_scalar = match prev_time_scalar {
                ScalarRefImpl::Timestamp(_) => ScalarImpl::Timestamp(fill_time),
                ScalarRefImpl::Timestamptz(_) => {
                    let micros = fill_time.0.and_utc().timestamp_micros();
                    ScalarImpl::Timestamptz(risingwave_common::types::Timestamptz::from_micros(
                        micros,
                    ))
                }
                _ => unreachable!("Time column should be Timestamp or Timestamptz"),
            };
            new_row_data[time_column_index] = Some(fill_time_scalar);
            data.push(new_row_data);

            fill_time = match fill_time.checked_add(*interval) {
                Some(t) => t,
                None => break,
            };
        }

        // --- NEW LOGIC FOR INTERPOLATION ---
        for (col_idx, strategy) in fill_columns {
            if matches!(strategy, FillStrategy::Interpolate) {
                let steps = data.len();
                let step = Self::calculate_step(
                    prev_row.datum_at(*col_idx).to_owned_datum(),
                    curr_row.datum_at(*col_idx).to_owned_datum(),
                    steps + 1,
                );
                if let Some(step) = step {
                    for (i, row) in data.iter_mut().enumerate() {
                        // The value in `row` is a copy of `prev_row`.
                        // So we need to apply step `i+1` times.
                        for _ in 0..=i {
                            Self::apply_step(&mut row[*col_idx], &step);
                        }
                    }
                }
            }
        }

        for row_data in data {
            filled_rows.push(OwnedRow::new(row_data));
        }

        Ok(filled_rows)
    }
}

impl<S: StateStore> Execute for GapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> GapFillExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            input,
            mut state_table,
            mut row_buffer,
            schema,
            chunk_size,
            time_column_index,
            fill_columns,
            gap_interval,
            ..
        } = *self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;

        // Load data from state table on initialization
        let mut all_rows_from_state: Vec<OwnedRow> = vec![];
        let vnodes = state_table.vnodes().clone();
        let streams = futures::future::try_join_all(vnodes.iter_vnodes().map(|vnode| {
            state_table.iter_with_vnode(
                vnode,
                &(
                    std::ops::Bound::<row::Empty>::Unbounded,
                    std::ops::Bound::<row::Empty>::Unbounded,
                ),
                Default::default(),
            )
        }))
        .await?
        .into_iter()
        .map(Box::pin);
        let all_rows_stream = stream::select_all(streams);
        #[for_await]
        for row in all_rows_stream {
            all_rows_from_state.push(row?.into_owned_row());
        }

        // Initialize row_buffer with rows from state, correctly identifying filled vs original
        for state_row in all_rows_from_state {
            let is_filled = Self::extract_is_filled_flag(&state_row);
            let output_row = Self::state_row_to_output_row(&state_row);
            let extended_row = ExtendedRow {
                row: output_row.clone(),
                row_type: if is_filled {
                    RowType::Filled
                } else {
                    RowType::Original
                },
            };
            let pos = Self::find_insert_position_static(
                &row_buffer,
                &extended_row.row,
                time_column_index,
            );
            row_buffer.insert(pos, extended_row);
        }

        // Initial fill for existing data
        let mut chunk_builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
        let mut filled_rows_to_add: Vec<(usize, ExtendedRow)> = Vec::new();

        let dummy_row = OwnedRow::new(vec![]);
        if let Some(ScalarImpl::Interval(interval)) =
            gap_interval.eval_row_infallible(&dummy_row).await
        {
            for i in 0..row_buffer.len() {
                if i + 1 < row_buffer.len() {
                    let prev = &row_buffer[i];
                    let next = &row_buffer[i + 1];

                    if prev.row_type == RowType::Original && next.row_type == RowType::Original {
                        let filled_rows = Self::generate_filled_rows_between_static(
                            &prev.row,
                            &next.row,
                            &interval,
                            time_column_index,
                            &fill_columns,
                        )?;
                        for (j, filled_row) in filled_rows.into_iter().enumerate() {
                            let state_row = Self::row_to_state_row(&filled_row, true);
                            state_table.insert(&state_row);
                            if let Some(chunk) = chunk_builder.append_row(Op::Insert, &filled_row) {
                                yield Message::Chunk(chunk);
                            }
                            filled_rows_to_add.push((
                                i + 1 + j,
                                ExtendedRow {
                                    row: filled_row,
                                    row_type: RowType::Filled,
                                },
                            ));
                        }
                    }
                }
            }
        }

        for (index, row) in filled_rows_to_add.into_iter().rev() {
            row_buffer.insert(index, row);
        }

        if let Some(chunk) = chunk_builder.take() {
            yield Message::Chunk(chunk);
        }

        let is_optimizable = !fill_columns
            .iter()
            .any(|(_, s)| matches!(s, FillStrategy::Interpolate));

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let mut chunk_builder =
                        StreamChunkBuilder::new(chunk_size, schema.data_types());
                    let chunk = chunk.compact();
                    let mut temp_outputs = Vec::new();

                    let mut pending_update_delete: Option<OwnedRow> = None;

                    for (op, row_ref) in chunk.rows() {
                        let row = row_ref.into_owned_row();

                        // Defer UpdateDelete to be handled with its corresponding UpdateInsert
                        if let Op::UpdateDelete = op
                            && is_optimizable
                        {
                            pending_update_delete = Some(row);
                            continue;
                        }

                        let is_optimizable_update = is_optimizable
                            && matches!(op, Op::UpdateInsert)
                            && pending_update_delete.is_some();

                        if is_optimizable_update {
                            // --- Optimized path for Update operations on LOCF and NULL strategies ---
                            let old_row = pending_update_delete.take().unwrap();
                            let new_row = row;

                            if let Some(pos) = row_buffer
                                .iter()
                                .position(|r| r.row == old_row && r.row_type == RowType::Original)
                            {
                                // For both NULL and LOCF, the old version of the row must be deleted and the new one inserted.
                                row_buffer.remove(pos);
                                let old_state_row = Self::row_to_state_row(&old_row, false);
                                state_table.delete(&old_state_row);
                                temp_outputs.push((Op::UpdateDelete, old_row));

                                let insert_pos = Self::find_insert_position_static(
                                    &row_buffer,
                                    &new_row,
                                    time_column_index,
                                );
                                row_buffer.insert(
                                    insert_pos,
                                    ExtendedRow {
                                        row: new_row.clone(),
                                        row_type: RowType::Original,
                                    },
                                );
                                let new_state_row = Self::row_to_state_row(&new_row, false);
                                state_table.insert(&new_state_row);
                                temp_outputs.push((Op::UpdateInsert, new_row.clone()));

                                let is_locf = fill_columns
                                    .iter()
                                    .any(|(_, s)| matches!(s, FillStrategy::Locf));

                                if is_locf {
                                    // For LOCF, filled rows after the updated row might change.
                                    let next_original_info = row_buffer[insert_pos + 1..]
                                        .iter()
                                        .find(|r| r.row_type == RowType::Original)
                                        .cloned();

                                    if let Some(next) = &next_original_info {
                                        // Delete old filled rows between the updated row and the next original row
                                        let end_idx = row_buffer
                                            .iter()
                                            .position(|r| r.row == next.row)
                                            .unwrap();
                                        if insert_pos + 1 < end_idx {
                                            let rows_to_delete = row_buffer
                                                .drain(insert_pos + 1..end_idx)
                                                .collect::<Vec<_>>();
                                            for r in rows_to_delete {
                                                if r.row_type == RowType::Filled {
                                                    let state_row =
                                                        Self::row_to_state_row(&r.row, true);
                                                    state_table.delete(&state_row);
                                                    temp_outputs.push((Op::Delete, r.row));
                                                }
                                            }
                                        }
                                    }

                                    // Insert new filled rows
                                    if let Some(next) = next_original_info {
                                        let dummy_row = OwnedRow::new(vec![]);
                                        if let Some(ScalarImpl::Interval(interval)) =
                                            gap_interval.eval_row_infallible(&dummy_row).await
                                        {
                                            let filled_rows =
                                                Self::generate_filled_rows_between_static(
                                                    &new_row,
                                                    &next.row,
                                                    &interval,
                                                    time_column_index,
                                                    &fill_columns,
                                                )?;
                                            let fill_insert_pos = insert_pos + 1;
                                            for (i, filled_row) in
                                                filled_rows.into_iter().enumerate()
                                            {
                                                let state_row =
                                                    Self::row_to_state_row(&filled_row, true);
                                                state_table.insert(&state_row);
                                                temp_outputs.push((Op::Insert, filled_row.clone()));
                                                row_buffer.insert(
                                                    fill_insert_pos + i,
                                                    ExtendedRow {
                                                        row: filled_row,
                                                        row_type: RowType::Filled,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }
                                // For pure NULL, no filled rows need to be touched.
                            }
                        } else {
                            // --- General path for Insert, Delete, and all operations with Interpolate strategy ---
                            match op {
                                Op::Insert | Op::UpdateInsert => {
                                    // The "delete and rebuild" strategy is necessary here.
                                    let pos = Self::find_insert_position_static(
                                        &row_buffer,
                                        &row,
                                        time_column_index,
                                    );
                                    let prev_original_info = row_buffer[..pos]
                                        .iter()
                                        .rfind(|r| r.row_type == RowType::Original)
                                        .cloned();
                                    let next_original_info = row_buffer[pos..]
                                        .iter()
                                        .find(|r| r.row_type == RowType::Original)
                                        .cloned();

                                    if let (Some(prev), Some(next)) =
                                        (&prev_original_info, &next_original_info)
                                    {
                                        let start_idx = row_buffer
                                            .iter()
                                            .position(|r| r.row == prev.row)
                                            .unwrap()
                                            + 1;
                                        let end_idx = row_buffer
                                            .iter()
                                            .position(|r| r.row == next.row)
                                            .unwrap();

                                        if start_idx < end_idx {
                                            let rows_to_delete = row_buffer
                                                .drain(start_idx..end_idx)
                                                .collect::<Vec<_>>();
                                            for extended_row in rows_to_delete {
                                                if extended_row.row_type == RowType::Filled {
                                                    let state_row = Self::row_to_state_row(
                                                        &extended_row.row,
                                                        true,
                                                    );
                                                    state_table.delete(&state_row);
                                                    temp_outputs
                                                        .push((Op::Delete, extended_row.row));
                                                }
                                            }
                                        }
                                    }

                                    let pos_new = Self::find_insert_position_static(
                                        &row_buffer,
                                        &row,
                                        time_column_index,
                                    );
                                    let extended_row = ExtendedRow {
                                        row: row.clone(),
                                        row_type: RowType::Original,
                                    };
                                    row_buffer.insert(pos_new, extended_row);
                                    let state_row = Self::row_to_state_row(&row, false);
                                    state_table.insert(&state_row);
                                    temp_outputs.push((op, row.clone()));

                                    let dummy_row = OwnedRow::new(vec![]);
                                    if let Some(ScalarImpl::Interval(interval)) =
                                        gap_interval.eval_row_infallible(&dummy_row).await
                                    {
                                        if let Some(prev) = prev_original_info {
                                            let filled_rows =
                                                Self::generate_filled_rows_between_static(
                                                    &prev.row,
                                                    &row,
                                                    &interval,
                                                    time_column_index,
                                                    &fill_columns,
                                                )?;
                                            let insert_pos = row_buffer
                                                .iter()
                                                .position(|r| r.row == prev.row)
                                                .unwrap()
                                                + 1;
                                            for (i, filled_row) in
                                                filled_rows.into_iter().enumerate()
                                            {
                                                let state_row =
                                                    Self::row_to_state_row(&filled_row, true);
                                                state_table.insert(&state_row);
                                                temp_outputs.push((Op::Insert, filled_row.clone()));
                                                row_buffer.insert(
                                                    insert_pos + i,
                                                    ExtendedRow {
                                                        row: filled_row,
                                                        row_type: RowType::Filled,
                                                    },
                                                );
                                            }
                                        }

                                        if let Some(next) = next_original_info {
                                            let filled_rows =
                                                Self::generate_filled_rows_between_static(
                                                    &row,
                                                    &next.row,
                                                    &interval,
                                                    time_column_index,
                                                    &fill_columns,
                                                )?;
                                            let insert_pos = row_buffer
                                                .iter()
                                                .position(|r| r.row == row)
                                                .unwrap()
                                                + 1;
                                            for (i, filled_row) in
                                                filled_rows.into_iter().enumerate()
                                            {
                                                let state_row =
                                                    Self::row_to_state_row(&filled_row, true);
                                                state_table.insert(&state_row);
                                                temp_outputs.push((Op::Insert, filled_row.clone()));
                                                row_buffer.insert(
                                                    insert_pos + i,
                                                    ExtendedRow {
                                                        row: filled_row,
                                                        row_type: RowType::Filled,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }
                                Op::Delete | Op::UpdateDelete => {
                                    if let Some(pos) = row_buffer.iter().position(|r| {
                                        r.row == row && r.row_type == RowType::Original
                                    }) {
                                        let prev_original_info = row_buffer[..pos]
                                            .iter()
                                            .rfind(|r| r.row_type == RowType::Original)
                                            .cloned();
                                        let next_original_info = row_buffer[pos + 1..]
                                            .iter()
                                            .find(|r| r.row_type == RowType::Original)
                                            .cloned();

                                        // Delete surrounding filled rows before this one
                                        if let Some(prev) = &prev_original_info {
                                            let start_idx = row_buffer
                                                .iter()
                                                .position(|r| r.row == prev.row)
                                                .unwrap()
                                                + 1;
                                            if start_idx < pos {
                                                let rows_to_delete = row_buffer
                                                    .drain(start_idx..pos)
                                                    .collect::<Vec<_>>();
                                                for r in rows_to_delete {
                                                    if r.row_type == RowType::Filled {
                                                        let state_row =
                                                            Self::row_to_state_row(&r.row, true);
                                                        state_table.delete(&state_row);
                                                        temp_outputs.push((Op::Delete, r.row));
                                                    }
                                                }
                                            }
                                        }

                                        let pos_new =
                                            row_buffer.iter().position(|r| r.row == row).unwrap();

                                        // Delete surrounding filled rows after this one
                                        if let Some(next) = &next_original_info {
                                            let end_idx = row_buffer
                                                .iter()
                                                .position(|r| r.row == next.row)
                                                .unwrap();
                                            if pos_new + 1 < end_idx {
                                                let rows_to_delete = row_buffer
                                                    .drain(pos_new + 1..end_idx)
                                                    .collect::<Vec<_>>();
                                                for r in rows_to_delete {
                                                    if r.row_type == RowType::Filled {
                                                        let state_row =
                                                            Self::row_to_state_row(&r.row, true);
                                                        state_table.delete(&state_row);
                                                        temp_outputs.push((Op::Delete, r.row));
                                                    }
                                                }
                                            }
                                        }

                                        let pos_final =
                                            row_buffer.iter().position(|r| r.row == row).unwrap();
                                        row_buffer.remove(pos_final);
                                        let state_row = Self::row_to_state_row(&row, false);
                                        state_table.delete(&state_row);
                                        temp_outputs.push((op, row));

                                        // Refill the gap
                                        if let (Some(prev), Some(next)) =
                                            (prev_original_info, next_original_info)
                                        {
                                            let dummy_row = OwnedRow::new(vec![]);
                                            if let Some(ScalarImpl::Interval(interval)) =
                                                gap_interval.eval_row_infallible(&dummy_row).await
                                            {
                                                let filled_rows =
                                                    Self::generate_filled_rows_between_static(
                                                        &prev.row,
                                                        &next.row,
                                                        &interval,
                                                        time_column_index,
                                                        &fill_columns,
                                                    )?;
                                                let insert_pos = row_buffer
                                                    .iter()
                                                    .position(|r| r.row == prev.row)
                                                    .unwrap()
                                                    + 1;
                                                for (i, filled_row) in
                                                    filled_rows.into_iter().enumerate()
                                                {
                                                    let state_row =
                                                        Self::row_to_state_row(&filled_row, true);
                                                    state_table.insert(&state_row);
                                                    temp_outputs
                                                        .push((Op::Insert, filled_row.clone()));
                                                    row_buffer.insert(
                                                        insert_pos + i,
                                                        ExtendedRow {
                                                            row: filled_row,
                                                            row_type: RowType::Filled,
                                                        },
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    temp_outputs.sort_by(|(op1, r1), (op2, r2)| {
                        let t1 = r1
                            .datum_at(time_column_index)
                            .unwrap()
                            .to_owned_datum()
                            .unwrap();
                        let t2 = r2
                            .datum_at(time_column_index)
                            .unwrap()
                            .to_owned_datum()
                            .unwrap();
                        let op1_order = match op1 {
                            Op::Delete | Op::UpdateDelete => 0,
                            Op::Insert | Op::UpdateInsert => 1,
                        };
                        let op2_order = match op2 {
                            Op::Delete | Op::UpdateDelete => 0,
                            Op::Insert | Op::UpdateInsert => 1,
                        };
                        op1_order
                            .cmp(&op2_order)
                            .then_with(|| Self::compare_scalars(&t1, &t2))
                    });

                    for (op, row) in temp_outputs {
                        if let Some(chunk) = chunk_builder.append_row(op, &row) {
                            yield Message::Chunk(chunk);
                        }
                    }

                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
                Message::Watermark(watermark) => {
                    // In pure streaming mode, watermarks are just forwarded.
                    // The logic to trim old data can be added here if needed.
                    yield Message::Watermark(watermark);
                }
                Message::Barrier(barrier) => {
                    let post_commit = state_table.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    yield Message::Barrier(barrier);
                    post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Interval};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::LiteralExpression;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MessageSender, MockSource};

    async fn create_executor(
        store: MemoryStateStore,
        fill_columns: Vec<(usize, FillStrategy)>,
        schema: Schema,
        gap_interval: Interval,
    ) -> (MessageSender, BoxedMessageStream) {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), vec![0]);

        let mut table_columns: Vec<ColumnDesc> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc::unnamed(ColumnId::new(i as i32), f.data_type.clone()))
            .collect();

        // Add the is_filled flag column at the end
        let is_filled_column_id = table_columns.len() as i32;
        table_columns.push(ColumnDesc::unnamed(
            ColumnId::new(is_filled_column_id),
            DataType::Boolean,
        ));

        let table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(0),
                table_columns,
                vec![OrderType::ascending()],
                vec![0],
                0,
                vec![],
            ),
            store,
            None,
        )
        .await;

        let time_column_index = 0;

        let executor = GapFillExecutor::new(GapFillExecutorArgs {
            ctx: ActorContext::for_test(123),
            input: source,
            schema: schema.clone(),
            chunk_size: 1024,
            time_column_index,
            fill_columns,
            gap_interval: NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            state_table: table,
        });

        (tx, executor.boxed().execute())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_locf() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = vec![(1, FillStrategy::Locf), (2, FillStrategy::Locf)];
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0
                + 2022-01-01T00:03:00 4   4.0"
            )
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        // This tests if the executor can correctly retract old filled rows and create new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        // Expect a chunk that retracts the old fills, inserts the new row, and adds the new fills.
        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 1   1.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 2   2.0"
            )
        );

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and generation of new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0"
            )
        );

        // 4. Send an update chunk to modify an original data point.
        // This should also trigger retraction and re-generation of fills.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        // The filled rows' values don't change as they depend on the first row,
        // but they are still retracted and re-inserted.
        assert_eq!(
            chunk4,
            StreamChunk::from_pretty(
                " TS                  i   F
                U- 2022-01-01T00:03:00 4   4.0
                U+ 2022-01-01T00:03:00 5   5.0"
            )
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_null() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = vec![(1, FillStrategy::Null), (2, FillStrategy::Null)];
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:03:00 4   4.0"
            )
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 2   2.0"
            )
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   ."
            )
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4,
            StreamChunk::from_pretty(
                " TS                  i   F
                U- 2022-01-01T00:03:00 4   4.0
                U+ 2022-01-01T00:03:00 5   5.0"
            )
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_interpolate() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
        ];
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:01:00 5   5.5
                + 2022-01-01T00:02:00 10  10.0"
            )
        );

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and re-calculation of interpolated values.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 5   5.5
                - 2022-01-01T00:02:00 10  10.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0"
            )
        );

        // 4. Send an update chunk to modify an original data point.
        // This will cause the interpolated values to be re-calculated.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 10  10.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4,
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                U- 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 4   4.0
                + 2022-01-01T00:02:00 7   7.0
                U+ 2022-01-01T00:03:00 10  10.0"
            )
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_recovery() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = vec![(1, FillStrategy::Locf), (2, FillStrategy::Interpolate)];

        // --- First run ---
        let (mut tx, mut executor) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // Send a chunk and commit.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        // Consume the initial filled chunk
        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 1   2.0
                + 2022-01-01T00:02:00 1   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
        );

        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap(); // Barrier to commit

        // --- Second run (after recovery) ---
        let (mut tx2, mut executor2) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier, which triggers recovery
        tx2.push_barrier(test_epoch(2), false);
        executor2.next().await.unwrap().unwrap(); // Barrier

        // After recovery, the executor should not output anything for the loaded state.

        // Send a new chunk, which should fill the gap between old and new data.
        tx2.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:05:00 6   10.0",
        ));

        let chunk2 = executor2
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2,
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:04:00 4   7.0
                + 2022-01-01T00:05:00 6   10.0"
            )
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_mixed_strategy() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);

        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ];
        let gap_interval = Interval::from_days(1);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, gap_interval).await;

        // Init with barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Send an initial chunk with a gap to test mixed filling strategies.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0"
            )
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-02T10:00:00 17 100 .    130.0
                + 2023-04-03T10:00:00 25 150 3.0 160.0
                + 2023-04-04T10:00:00 37 150 .    180.0"
            )
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            - 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));
        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 17 100 .    130.0
                - 2023-04-03T10:00:00 25 150 3.0 160.0
                - 2023-04-04T10:00:00 37 150 .    180.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0"
            )
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            U- 2023-04-05T10:00:00 50 200 5.0 200.0
            U+ 2023-04-05T10:00:00 50 200 5.0 300.0",
        ));
        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                U- 2023-04-05T10:00:00 50 200 5.0 200.0
                + 2023-04-02T10:00:00 20 100 .    150.0
                + 2023-04-03T10:00:00 30 100 .    200.0
                + 2023-04-04T10:00:00 40 100 .    250.0
                U+ 2023-04-05T10:00:00 50 200 5.0 300.0"
            )
        );
    }
}
