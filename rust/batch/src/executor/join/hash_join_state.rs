// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, LinkedList};
use std::convert::TryFrom;
use std::mem;
use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, ArrayRef, DataChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};

use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::join::hash_join::EquiJoinParams;
use crate::executor::join::JoinType;

const MAX_BUILD_ROW_COUNT: usize = u32::MAX as usize;

type ProbeRowId = usize;

pub(super) struct BuildTable {
    build_data: Vec<DataChunk>,
    row_count: usize,
    params: EquiJoinParams,
}

impl BuildTable {
    pub(super) fn with_params(params: EquiJoinParams) -> Self {
        Self {
            build_data: Vec::new(),
            row_count: 0,
            params,
        }
    }

    pub(super) fn append_build_chunk(&mut self, data_chunk: DataChunk) -> Result<()> {
        ensure!(
            (MAX_BUILD_ROW_COUNT - self.row_count) > data_chunk.cardinality(),
            "Build table size exceeded limit!"
        );
        let data_chunk = data_chunk.compact()?;
        if data_chunk.cardinality() > 0 {
            self.row_count += data_chunk.cardinality();
            self.build_data.push(data_chunk);
        }
        Ok(())
    }

    fn build_hash_map<K: HashKey>(&self) -> Result<(ChunkedData<Option<RowId>>, JoinHashMap<K>)> {
        let mut hash_map =
            JoinHashMap::with_capacity_and_hasher(self.row_count, PrecomputedBuildHasher);
        let mut build_index = ChunkedData::<Option<RowId>>::with_chunk_sizes(
            self.build_data.iter().map(|c| c.cardinality()),
        )?;

        for (chunk_id, data_chunk) in self.build_data.iter().enumerate() {
            let keys = K::build(self.params.build_key_columns(), data_chunk)?;
            for (row_id_in_chunk, row_key) in keys.into_iter().enumerate() {
                // In pg `null` and `null` never joins, so we should skip them in hash table.
                if row_key.has_null() {
                    continue;
                }
                let current_row_id = RowId::new(chunk_id, row_id_in_chunk);
                build_index[current_row_id] = hash_map.insert(row_key, current_row_id);
            }
        }

        Ok((build_index, hash_map))
    }
}

struct ProbeData<K> {
    probe_data_chunk: DataChunk,
    probe_keys: Vec<K>,
}

pub(super) struct ProbeTable<K> {
    /// Hashmap created by join keys.
    ///
    /// Key is composed by fields in join condition.
    ///
    /// Value of this map is the first row id in `build_data` which has same key. The chain of rows
    /// with same key are stored in `build_index`.
    ///
    /// For example, when we have following build keys:
    ///
    /// |key|
    /// |---|
    /// | a |
    /// | b |
    /// | a |
    ///
    /// The `build_table` has following values:
    ///
    /// ```ignore
    /// {a -> RowId(0, 0), b -> RowId(0, 1)}
    /// ```
    ///
    /// And the `build_index` has following data:
    /// ```ignore
    /// |Some(2)| // Point to next row with same key.
    /// |None|    // No more row with same key.
    /// |None|    // No more row with same key.
    /// ```
    build_table: JoinHashMap<K>,
    build_data: Vec<DataChunk>,
    build_index: ChunkedData<Option<RowId>>,

    /// Used only when join remaining is required after probing.
    ///
    /// See [`JoinType::need_join_remaining`]
    build_matched: Option<ChunkedData<bool>>,
    probe_matched: Option<LinkedList<Vec<usize>>>,
    cur_probe_matched: usize,
    /// When a chunk is full, if true, there are still rows in
    /// the build side that matches the current row in probe
    /// side that has not been added to the chunk.
    has_pending_matched: bool,

    /// Map from row ids in join result chunk to those in probe/build chunk
    /// with length of batch size.
    result_build_index: Vec<Option<RowId>>,
    result_probe_index: Vec<Option<ProbeRowId>>,
    // Only used when reentry remove_duplicated_rows function
    result_offset: usize,

    /// Fields for generating one chunk during probe
    cur_probe_data: Option<ProbeData<K>>,
    cur_joined_build_row_id: Option<RowId>,
    cur_probe_row_id: usize,

    // For join remaining
    cur_remaining_build_row_id: Option<RowId>,

    params: EquiJoinParams,

    array_builders: Vec<ArrayBuilderImpl>,
}

/// Iterator for joined row ids for one key.
///
/// See [`ProbeTable`]
struct JoinedRowIdIterator<'a> {
    cur: Option<RowId>,
    index: &'a ChunkedData<Option<RowId>>,
}

impl<K: HashKey> TryFrom<BuildTable> for ProbeTable<K> {
    type Error = RwError;

    fn try_from(build_table: BuildTable) -> Result<Self> {
        let (build_index, hash_map) = build_table.build_hash_map()?;

        let mut build_matched = None;
        let mut remaining_build_row_id = None;
        let mut probe_matched = None;
        if build_table.params.join_type().need_build_flag()
            && !build_table.params.has_non_equi_cond()
        {
            build_matched = Some(ChunkedData::<bool>::with_chunk_sizes(
                build_table.build_data.iter().map(|c| c.cardinality()),
            )?);
            remaining_build_row_id = Some(RowId::default());
        }
        if build_table.params.join_type().need_probe_flag()
            && build_table.params.has_non_equi_cond()
        {
            probe_matched = Some(LinkedList::new());
        }

        let result_build_index = Vec::with_capacity(build_table.params.batch_size());
        let result_probe_index = Vec::with_capacity(build_table.params.batch_size());

        let array_builders = build_table
            .params
            .output_types()
            .iter()
            .map(|data_type| data_type.create_array_builder(build_table.params.batch_size()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            build_table: hash_map,
            build_data: build_table.build_data,
            build_index,
            build_matched,
            probe_matched,
            cur_probe_matched: 0,
            has_pending_matched: false,
            result_build_index,
            result_probe_index,
            result_offset: 0,
            cur_probe_data: None,
            cur_joined_build_row_id: None,
            cur_probe_row_id: 0,
            cur_remaining_build_row_id: remaining_build_row_id,
            params: build_table.params,
            array_builders,
        })
    }
}

impl<K: HashKey> ProbeTable<K> {
    pub(super) fn has_non_equi_cond(&self) ->bool {
        self.params.has_non_equi_cond()
    }

    pub(super) fn join_type(&self) -> JoinType {
        self.params.join_type()
    }

    pub(super) fn set_probe_data(&mut self, probe_data_chunk: DataChunk) -> Result<()> {
        self.build_data_chunk()?;
        let probe_data_chunk = probe_data_chunk.compact()?;
        ensure!(probe_data_chunk.cardinality() > 0);
        let probe_keys = K::build(self.params.probe_key_columns(), &probe_data_chunk)?;
        if self.params.join_type().need_probe_flag() && self.params.has_non_equi_cond() {
            self.probe_matched
                .as_mut()
                .map(|list| list.push_back(vec![0; probe_data_chunk.capacity()]));
        }
        self.cur_probe_row_id = 0;
        self.cur_joined_build_row_id = self.first_joined_row_id(&probe_keys[0]);
        self.cur_probe_data = Some(ProbeData::<K> {
            probe_data_chunk,
            probe_keys,
        });
        Ok(())
    }

    /// Do join using
    pub(super) fn join(&mut self) -> Result<Option<DataChunk>> {
        if self.params.cond.is_none() {
            match self.params.join_type() {
                JoinType::Inner => self.do_inner_join(),
                JoinType::LeftOuter => self.do_left_outer_join(),
                JoinType::LeftAnti => self.do_left_anti_join(),
                JoinType::LeftSemi => self.do_left_semi_join(),
                JoinType::RightOuter => self.do_right_outer_join(),
                JoinType::RightAnti => self.do_right_anti_join(),
                JoinType::RightSemi => self.do_right_semi_join(),
                JoinType::FullOuter => self.do_full_outer_join(),
            }
        } else {
            match self.params.join_type() {
                JoinType::Inner => self.do_inner_join(),
                JoinType::LeftOuter => self.do_left_outer_join_with_non_equi_condition(),
                JoinType::LeftAnti => self.do_left_anti_join_with_non_equi_conditon(),
                JoinType::LeftSemi => self.do_left_semi_join_with_non_equi_condition(),
                JoinType::RightOuter => self.do_right_outer_join_with_non_equi_condition(),
                JoinType::RightAnti => self.do_right_anti_join_with_non_equi_condition(),
                JoinType::RightSemi => self.do_right_semi_join_with_non_equi_condition(),
                JoinType::FullOuter => self.do_full_outer_join_with_non_equi_condition(),
            }
        }
    }

    fn nullify_build_side_for_non_equi_condition(
        &mut self,
        data_chunk: &mut DataChunk,
        filter: &Bitmap,
    ) {
        for column_id in self.params.output_columns().iter().copied() {
            match column_id {
                // probe side column, do nothing
                Either::Left(_) => {}

                // build side column
                Either::Right(idx) => data_chunk
                    .column_mut_at(idx)
                    .array_mut_ref()
                    .set_bitmap(filter.clone()),
            }
        }
    }

    fn remove_duplicate_rows_for_left_outer(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let probe_matched_list = self.probe_matched.as_mut().unwrap();
        let mut last_probe_matched = None;
        let mut result_row_id = 0;
        let chunk_len = filter.len();
        let mut new_filter = Vec::with_capacity(chunk_len);
        while let Some(mut probe_matched) = probe_matched_list.pop_front() {
            while self.cur_probe_matched < probe_matched.len() {
                let mut cur_result_row_cnt = probe_matched[self.cur_probe_matched];
                // Probe side does not match any, but will have a row where build side is null
                if cur_result_row_cnt == 0 {
                    cur_result_row_cnt = 1;
                }
                for _ in 0..cur_result_row_cnt {
                    let filter_bit = filter.is_set(result_row_id).unwrap();

                    if probe_matched[self.cur_probe_matched] == 0 {
                        new_filter.push(true);
                    } else if probe_matched[self.cur_probe_matched] == 1
                        && !(self.has_pending_matched && result_row_id == chunk_len - 1)
                    {
                        if filter_bit == false {
                            new_filter.push(true);
                        }
                    } else if filter_bit == true {
                        probe_matched[self.cur_probe_matched] -= 1;
                        new_filter.push(filter_bit);
                    }

                    result_row_id += 1;
                }
                self.cur_probe_matched += 1;
            }
            self.cur_probe_matched = 0;
            if probe_matched_list.is_empty() {
                last_probe_matched = Some(probe_matched);
            }
        }
        assert_eq!(result_row_id, chunk_len);
        // push the last probe_match vec back bacause the probe may not be finished
        probe_matched_list.push_back(last_probe_matched.unwrap());
        new_filter.try_into()
    }

    fn remove_duplicate_rows_for_left_semi(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let probe_matched_list = self.probe_matched.as_mut().unwrap();
        let mut last_probe_matched = None;
        let mut result_row_id = 0;
        let chunk_len = filter.len();
        let mut new_filter = Vec::with_capacity(chunk_len);
        while let Some(mut probe_matched) = probe_matched_list.pop_front() {
            while self.cur_probe_matched < probe_matched.len() {
                let mut cur_result_row_cnt = probe_matched[self.cur_probe_matched];
                // Probe side does not match any, but will have a row where build side is null
                if cur_result_row_cnt == 0 {
                    cur_result_row_cnt = 1;
                }
                for _ in 0..cur_result_row_cnt {
                    let filter_bit = filter.is_set(result_row_id).unwrap();

                    if filter_bit == true {
                        if probe_matched[self.cur_probe_matched] == 0 {
                            probe_matched[self.cur_probe_matched] = 1;
                            new_filter.push(true);
                        } else {
                            new_filter.push(false);
                        }
                    } else {
                        new_filter.push(filter_bit);
                    }

                    result_row_id += 1;
                }
                self.cur_probe_matched += 1;
            }
            self.cur_probe_matched = 0;
            if probe_matched_list.is_empty() {
                last_probe_matched = Some(probe_matched);
            }
        }
        assert_eq!(result_row_id, chunk_len);
        // push the last probe_match vec back bacause the probe may not be finished
        probe_matched_list.push_back(last_probe_matched.unwrap());
        new_filter.try_into()
    }

    fn remove_duplicate_rows_for_left_anti(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let probe_matched_list = self.probe_matched.as_mut().unwrap();
        let mut last_probe_matched = None;
        let mut result_row_id = 0;
        let chunk_len = filter.len();
        let mut new_filter = Vec::with_capacity(chunk_len);
        while let Some(mut probe_matched) = probe_matched_list.pop_front() {
            while self.cur_probe_matched < probe_matched.len() {
                let mut cur_result_row_cnt = probe_matched[self.cur_probe_matched];
                // Probe side does not match any, but will have a row where build side is null
                if cur_result_row_cnt == 0 {
                    cur_result_row_cnt = 1;
                }
                for _ in 0..cur_result_row_cnt {
                    let filter_bit = filter.is_set(result_row_id).unwrap();

                    if probe_matched[self.cur_probe_matched] == 0 {
                        new_filter.push(true);
                    } else if probe_matched[self.cur_probe_matched] == 1
                        && !(self.has_pending_matched && result_row_id == chunk_len - 1)
                    {
                        probe_matched[self.cur_probe_matched] -= 1;
                        new_filter.push(!filter_bit);
                    } else if filter_bit == false {
                        probe_matched[self.cur_probe_matched] -= 1;
                        new_filter.push(filter_bit);
                    } else {
                        new_filter.push(false);
                    }

                    result_row_id += 1;
                }
                self.cur_probe_matched += 1;
            }
            self.cur_probe_matched = 0;
            if probe_matched_list.is_empty() {
                last_probe_matched = Some(probe_matched);
            }
        }
        assert_eq!(result_row_id, chunk_len);
        // push the last probe_match vec back bacause the probe may not be finished
        probe_matched_list.push_back(last_probe_matched.unwrap());
        new_filter.try_into()
    }

    fn remove_duplicate_rows_for_right_outer(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let chunk_len = filter.len();
        for result_row_id in 0..chunk_len {
            let filter_bit = filter.is_set(result_row_id).unwrap();
            if filter_bit == true {
                // Not possible to have a null row in right side in right outer join.
                // Thus just unwrap.
                let build_row_id = self.result_build_index[result_row_id].unwrap();
                self.set_build_matched(build_row_id)?;
            }
        }
        Ok(filter)
    }

    fn remove_duplicate_rows_for_right_semi(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let chunk_len = filter.len();
        let mut new_filter = Vec::with_capacity(chunk_len);
        for result_row_id in 0..chunk_len {
            let filter_bit = filter.is_set(result_row_id).unwrap();
            if filter_bit == true {
                // Not possible to have a null row in right side in right semi join.
                // Thus just unwrap.
                let build_row_id = self.result_build_index[result_row_id].unwrap();
                if !self.is_build_matched(build_row_id)? {
                    self.set_build_matched(build_row_id)?;
                    new_filter.push(filter_bit);
                } else {
                    new_filter.push(false);
                }
            } else {
                new_filter.push(filter_bit);
            }
        }
        new_filter.try_into()
    }

    fn remove_duplicate_rows_for_right_anti(&mut self, filter: Bitmap) -> Result<Bitmap> {
        let chunk_len = filter.len();
        for result_row_id in 0..chunk_len {
            let filter_bit = filter.is_set(result_row_id).unwrap();
            if filter_bit == true {
                // Not possible to have a null row in right side in right anti join.
                // Thus just unwrap.
                let build_row_id = self.result_build_index[result_row_id].unwrap();
                self.set_build_matched(build_row_id)?;
            }
        }
        Ok(filter)
    }

    fn remove_duplicate_rows_for_full_outer(&mut self, filter: Bitmap) -> Result<Bitmap> {
        // TODO(yuhao): This is a bit dirty. I have to take the list out of struct
        // and put it back before return to cheat the borrow checker.
        let mut probe_matched_list = self.probe_matched.take().unwrap();
        let mut last_probe_matched = None;
        let mut result_row_id = 0;
        let chunk_len = filter.len();
        let mut new_filter = Vec::with_capacity(chunk_len);
        while let Some(mut probe_matched) = probe_matched_list.pop_front() {
            while self.cur_probe_matched < probe_matched.len() {
                let mut cur_result_row_cnt = probe_matched[self.cur_probe_matched];
                // Probe side does not match any, but will have a row where build side is null
                if cur_result_row_cnt == 0 {
                    cur_result_row_cnt = 1;
                }
                for _ in 0..cur_result_row_cnt {
                    let filter_bit = filter.is_set(result_row_id).unwrap();
                    if probe_matched[self.cur_probe_matched] == 0 {
                        new_filter.push(true);
                    } else if probe_matched[self.cur_probe_matched] == 1
                        && !(self.has_pending_matched && result_row_id == chunk_len - 1)
                    {
                        if filter_bit == true {
                            // Not possible to have a null row in right side when probe
                            // matched >= 1. Thus just unwrap.
                            self.set_build_matched(
                                self.result_build_index[result_row_id].unwrap(),
                            )?;
                        }
                        new_filter.push(true);
                    } else {
                        if filter_bit == false {
                            probe_matched[self.cur_probe_matched] -= 1;
                        } else {
                            self.set_build_matched(
                                self.result_build_index[result_row_id].unwrap(),
                            )?;
                        }
                        new_filter.push(filter_bit);
                    }

                    result_row_id += 1;
                }
                self.cur_probe_matched += 1;
            }
            self.cur_probe_matched = 0;
            if probe_matched_list.is_empty() {
                last_probe_matched = Some(probe_matched);
            }
        }
        assert_eq!(result_row_id, chunk_len);
        // push the last probe_match vec back bacause the probe may not be finished
        probe_matched_list.push_back(last_probe_matched.unwrap());
        self.probe_matched = Some(probe_matched_list);
        new_filter.try_into()
    }

    fn remove_duplicate_rows(&mut self, filter: Bitmap) -> Result<Bitmap> {
        match self.params.join_type() {
            JoinType::FullOuter => self.remove_duplicate_rows_for_full_outer(filter),
            JoinType::LeftOuter => self.remove_duplicate_rows_for_left_outer(filter),
            JoinType::LeftSemi => self.remove_duplicate_rows_for_left_semi(filter),
            JoinType::LeftAnti => self.remove_duplicate_rows_for_left_anti(filter),
            JoinType::RightOuter => self.remove_duplicate_rows_for_right_outer(filter),
            JoinType::RightSemi => self.remove_duplicate_rows_for_right_semi(filter),
            JoinType::RightAnti => self.remove_duplicate_rows_for_right_anti(filter),
            JoinType::Inner => unreachable!(),
        }
    }

    pub(super) fn process_non_equi_condition(&mut self, data_chunk: &mut DataChunk) -> Result<()> {
        match self.params.join_type() {
            JoinType::Inner => self.process_inner_join_non_equi_condition(data_chunk)?,
            JoinType::LeftOuter | JoinType::FullOuter => {
                self.process_outer_join_non_equi_condition(data_chunk)?
            }
            JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightOuter => self.process_semi_join_non_equi_condition(data_chunk)?,
            JoinType::RightAnti => self.process_right_anti_join_non_equi_condition(data_chunk)?,
        };
        // Clear the flag when finishing processing a chunk.
        self.has_pending_matched = false;
        Ok(())
    }

    fn process_inner_join_non_equi_condition(&mut self, data_chunk: &mut DataChunk) -> Result<()> {
        let filter = self
            .get_non_equi_cond_filter(&data_chunk)?;
        data_chunk.set_visibility(filter);
        Ok(())
    }

    fn process_outer_join_non_equi_condition(&mut self, data_chunk: &mut DataChunk) -> Result<()> {
        let filter = self
            .get_non_equi_cond_filter(&data_chunk)?;
        self.nullify_build_side_for_non_equi_condition(data_chunk, &filter);
        let filter = self.remove_duplicate_rows(filter)?;
        data_chunk.set_visibility(filter);
        Ok(())
    }

    fn process_semi_join_non_equi_condition(&mut self, data_chunk: &mut DataChunk) -> Result<()> {
        let filter = self
            .get_non_equi_cond_filter(&data_chunk)?;
        let filter = self.remove_duplicate_rows(filter)?;
        data_chunk.set_visibility(filter);
        Ok(())
    }

    fn process_right_anti_join_non_equi_condition(
        &mut self,
        data_chunk: &mut DataChunk,
    ) -> Result<()> {
        let filter = self
            .get_non_equi_cond_filter(&data_chunk)?;
        let filter = self.remove_duplicate_rows(filter)?;
        data_chunk.set_visibility(filter);
        Ok(())
    }

    fn get_non_equi_cond_filter(&mut self, data_chunk: &DataChunk) -> Result<Bitmap> {
        let array = self.params.cond.as_mut().unwrap().eval(data_chunk)?;
        array.as_bool().try_into()
    }

    pub(super) fn join_remaining(&mut self) -> Result<Option<DataChunk>> {
        self.do_join_remaining()
    }

    pub(super) fn consume_left(&mut self) -> Result<DataChunk> {
        Ok(self.finish_data_chunk()?)
    }

    fn next_probe_row(&mut self) {
        self.cur_probe_row_id += 1;
        // We must put the rest of `cur_build_row_id` here because we may reenter this method.
        if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            self.cur_joined_build_row_id =
                self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
        }
    }

    fn do_inner_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_left_outer_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    return Ok(Some(ret_data_chunk));
                }
            }

            // We need this because for unmatched left side row, we need to emit null
            if self
                .first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id))
                .is_none()
            {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(None, Some(self.cur_probe_row_id))?
                {
                    self.next_probe_row();
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_left_outer_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        self.do_full_outer_join_with_non_equi_condition()
    }

    fn do_left_semi_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            let cur_probe_row_id = self.cur_probe_row_id;
            self.cur_probe_row_id += 1;

            if self
                .first_joined_row_id(self.current_probe_key_at(cur_probe_row_id))
                .is_some()
            {
                if let Some(ret_data_chunk) = self.append_one_row(None, Some(cur_probe_row_id))? {
                    return Ok(Some(ret_data_chunk));
                }
            }
        }
        Ok(None)
    }

    fn do_left_semi_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        self.do_full_outer_join_with_non_equi_condition()
    }

    fn do_left_anti_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            let cur_probe_row_id = self.cur_probe_row_id;
            self.cur_probe_row_id += 1;

            if self
                .first_joined_row_id(self.current_probe_key_at(cur_probe_row_id))
                .is_none()
            {
                if let Some(ret_data_chunk) = self.append_one_row(None, Some(cur_probe_row_id))? {
                    return Ok(Some(ret_data_chunk));
                }
            }
        }
        Ok(None)
    }

    fn do_left_anti_join_with_non_equi_conditon(&mut self) -> Result<Option<DataChunk>> {
        self.do_full_outer_join_with_non_equi_condition()
    }

    fn do_right_outer_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                self.set_build_matched(build_row_id)?;
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_right_outer_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_right_semi_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                if !self.is_build_matched(build_row_id)? {
                    self.set_build_matched(build_row_id)?;
                    if let Some(ret_data_chunk) = self.append_one_row(Some(build_row_id), None)? {
                        return Ok(Some(ret_data_chunk));
                    }
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_right_semi_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        self.do_right_outer_join_with_non_equi_condition()
    }

    fn do_right_anti_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                self.set_build_matched(build_row_id)?;
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_right_anti_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        self.do_right_outer_join_with_non_equi_condition()
    }

    fn do_join_remaining(&mut self) -> Result<Option<DataChunk>> {
        while let Some(build_row_id) = self.next_join_remaining_build_row_id() {
            if !self.is_build_matched(build_row_id)? {
                if let Some(ret_data_chunk) = self.append_one_row(Some(build_row_id), None)? {
                    return Ok(Some(ret_data_chunk));
                }
            }
        }

        Ok(None)
    }

    fn do_full_outer_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                self.set_build_matched(build_row_id)?;
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    return Ok(Some(ret_data_chunk));
                }
            }

            // We need this because for unmatched left side row, we need to emit null
            if self
                .first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id))
                .is_none()
            {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(None, Some(self.cur_probe_row_id))?
                {
                    self.next_probe_row();
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        Ok(None)
    }

    fn do_full_outer_join_with_non_equi_condition(&mut self) -> Result<Option<DataChunk>> {
        // TODO(yuhao): This is a bit dirty. I have to take the list out of struct
        // and put it back before return to cheat the borrow checker.
        let mut probe_matched_list = self.probe_matched.take().unwrap();
        let probe_matched = probe_matched_list.front_mut().unwrap();
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                // Only needed for non-equi condition
                probe_matched[self.cur_probe_row_id] += 1;
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(Some(build_row_id), Some(self.cur_probe_row_id))?
                {
                    // There should be more rows in the build side that
                    // matches the current row in probe side.
                    if self.build_index[build_row_id].is_some() {
                        self.has_pending_matched = true;
                    }

                    self.probe_matched = Some(probe_matched_list);
                    return Ok(Some(ret_data_chunk));
                }
            }

            // We need this because for unmatched left side row, we need to emit null
            // TODO(yuhao): avoid searching hash table here.
            if self
                .first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id))
                .is_none()
            {
                // Here we have one full data chunk
                if let Some(ret_data_chunk) =
                    self.append_one_row(None, Some(self.cur_probe_row_id))?
                {
                    self.next_probe_row();

                    self.probe_matched = Some(probe_matched_list);
                    return Ok(Some(ret_data_chunk));
                }
            }
            self.next_probe_row();
        }
        self.probe_matched = Some(probe_matched_list);
        Ok(None)
    }

    fn set_build_matched(&mut self, build_row_id: RowId) -> Result<()> {
        match self.build_matched.as_mut() {
            Some(flags) => {
                flags[build_row_id] = true;
                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    fn is_build_matched(&self, build_row_id: RowId) -> Result<bool> {
        match self.build_matched.as_ref() {
            Some(flags) => Ok(flags[build_row_id]),
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    fn current_probe_data_chunk_size(&self) -> usize {
        self.cur_probe_data.as_ref().unwrap().probe_keys.len()
    }

    fn current_probe_key_at(&self, row_id: usize) -> &K {
        &self.cur_probe_data.as_ref().unwrap().probe_keys[row_id]
    }

    fn first_joined_row_id(&self, probe_key: &K) -> Option<RowId> {
        self.build_table.get(probe_key).copied()
    }

    fn joined_row_ids_from(&self, start: Option<RowId>) -> impl Iterator<Item = RowId> + '_ {
        JoinedRowIdIterator {
            cur: start,
            index: &self.build_index,
        }
    }

    fn all_joined_row_ids(&self, probe_key: &K) -> impl Iterator<Item = RowId> + '_ {
        match self.first_joined_row_id(probe_key) {
            Some(first_joined_row_id) => self.joined_row_ids_from(Some(first_joined_row_id)),
            None => self.joined_row_ids_from(None),
        }
    }

    /// Append a row id to result index array. Build the data chunk when the buffer is full.
    fn append_one_row(
        &mut self,
        build_row_id: Option<RowId>,
        probe_row_id: Option<usize>,
    ) -> Result<Option<DataChunk>> {
        assert_eq!(self.result_build_index.len(), self.result_probe_index.len());
        self.result_build_index.push(build_row_id);
        self.result_probe_index.push(probe_row_id);
        if self.result_build_index.len() == self.params.batch_size() {
            Ok(Some(self.finish_data_chunk()?))
        } else {
            Ok(None)
        }
    }

    /// Append data chunk builders without producing [`DataChunk`].
    fn build_data_chunk(&mut self) -> Result<()> {
        // The indices before the offset are already appended and dirty.
        let offset = self.result_offset;
        self.result_offset = self.result_build_index.len();
        for (builder_idx, column_id) in self.params.output_columns().iter().copied().enumerate() {
            match column_id {
                // probe side column
                Either::Left(idx) => {
                    for probe_row_id in &self.result_probe_index[offset..] {
                        if let Some(row_id) = probe_row_id {
                            let array = self
                                .cur_probe_data
                                .as_ref()
                                .unwrap()
                                .probe_data_chunk
                                .columns()[idx]
                                .array_ref();
                            self.array_builders[builder_idx]
                                .append_array_element(array, *row_id)?;
                        } else {
                            self.array_builders[builder_idx].append_null()?;
                        }
                    }
                }

                // build side column
                Either::Right(idx) => {
                    for build_row_id in &self.result_build_index[offset..] {
                        if let Some(row_id) = build_row_id {
                            let array_ref = self.get_build_array(*row_id, idx);
                            let array = array_ref.as_ref();
                            self.array_builders[builder_idx]
                                .append_array_element(array, row_id.row_id())?;
                        } else {
                            self.array_builders[builder_idx].append_null()?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Produce a data chunk from builder.
    fn finish_data_chunk(&mut self) -> Result<DataChunk> {
        self.build_data_chunk()?;
        let new_array_builders = self
            .params
            .output_types()
            .iter()
            .map(|data_type| data_type.create_array_builder(self.params.batch_size()))
            .collect::<Result<Vec<_>>>()?;
        let new_arrays = mem::replace(&mut self.array_builders, new_array_builders)
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;
        let new_columns = new_arrays
            .into_iter()
            .map(|array| Column::new(Arc::new(array)))
            .collect_vec();
        self.result_build_index.clear();
        self.result_probe_index.clear();
        self.result_offset = 0;
        let data_chunk = DataChunk::try_from(new_columns)?;
        Ok(data_chunk)
    }

    fn get_build_array(&self, row_id: RowId, idx: usize) -> ArrayRef {
        self.build_data[row_id.chunk_id()].columns()[idx].array()
    }

    fn all_build_row_ids(&self) -> impl Iterator<Item = RowId> + '_ {
        self.build_index.all_row_ids()
    }

    fn next_joined_build_row_id(&mut self) -> Option<RowId> {
        let ret = self.cur_joined_build_row_id;
        if let Some(cur_row_id) = self.cur_joined_build_row_id {
            self.cur_joined_build_row_id = self.build_index[cur_row_id];
        }

        ret
    }

    fn next_join_remaining_build_row_id(&mut self) -> Option<RowId> {
        if let Some(cur) = self.cur_remaining_build_row_id {
            self.cur_remaining_build_row_id = self.build_index.next_row_id(cur);
            Some(cur)
        } else {
            None
        }
    }
}

pub(super) type JoinHashMap<K> = HashMap<K, RowId, PrecomputedBuildHasher>;

impl<'a> Iterator for JoinedRowIdIterator<'a> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.cur;
        if let Some(cur_row_id) = self.cur {
            self.cur = self.index[cur_row_id];
        }

        ret
    }
}
