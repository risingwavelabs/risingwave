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

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;

use either::Either;
use risingwave_common::array::{ArrayImpl, DataChunk};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::join::hash_join::EquiJoinParams;
use crate::executor::join::JoinType;

const MAX_BUILD_ROW_COUNT: usize = u32::MAX as usize;

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

    /// Fields for generating one chunk during probe
    cur_probe_data: Option<ProbeData<K>>,
    data_chunk_builder: RefCell<DataChunkBuilder>,
    cur_joined_build_row_id: Option<RowId>,
    cur_probe_row_id: usize,

    // For join remaining
    cur_remaining_build_row_id: Option<RowId>,

    params: EquiJoinParams,
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
        if build_table.params.join_type().need_build_flag() {
            build_matched = Some(ChunkedData::<bool>::with_chunk_sizes(
                build_table.build_data.iter().map(|c| c.cardinality()),
            )?);
            remaining_build_row_id = Some(RowId::default());
        }

        let data_chunk_builder = RefCell::new(DataChunkBuilder::new(
            build_table.params.output_types().to_vec(),
            build_table.params.batch_size(),
        ));

        Ok(Self {
            build_table: hash_map,
            build_data: build_table.build_data,
            build_index,
            build_matched,
            cur_probe_data: None,
            data_chunk_builder,
            cur_joined_build_row_id: None,
            cur_probe_row_id: 0,
            cur_remaining_build_row_id: remaining_build_row_id,
            params: build_table.params,
        })
    }
}

impl<K: HashKey> ProbeTable<K> {
    pub(super) fn join_type(&self) -> JoinType {
        self.params.join_type()
    }

    pub(super) fn set_probe_data(&mut self, probe_data_chunk: DataChunk) -> Result<()> {
        let probe_data_chunk = probe_data_chunk.compact()?;
        ensure!(probe_data_chunk.cardinality() > 0);
        let probe_keys = K::build(self.params.probe_key_columns(), &probe_data_chunk)?;

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
    }

    pub(super) fn join_remaining(&mut self) -> Result<Option<DataChunk>> {
        self.do_join_remaining()
    }

    pub(super) fn consume_left(self) -> Result<Option<DataChunk>> {
        self.data_chunk_builder.borrow_mut().consume_all()
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

            self.cur_probe_row_id += 1;
            // We must put the rest of `cur_build_row_id` here because we may reenter this method.
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
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
                    self.cur_probe_row_id += 1;
                    // We must put the rest of `cur_build_row_id` here because we may reenter this
                    // method.
                    if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                        self.cur_joined_build_row_id = self
                            .first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
                    }
                    return Ok(Some(ret_data_chunk));
                }
            }

            self.cur_probe_row_id += 1;
            // We must put the rest of `cur_build_row_id` here because we may reenter this method.
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
        }

        Ok(None)
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

            self.cur_probe_row_id += 1;
            // We must put the rest of `cur_build_row_id` here because we may reenter this method.
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
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

            self.cur_probe_row_id += 1;
            // We must put the rest of `cur_build_row_id` here because we may reenter this method.
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
        }

        Ok(None)
    }

    fn do_right_anti_join(&mut self) -> Result<Option<DataChunk>> {
        while self.cur_probe_row_id < self.current_probe_data_chunk_size() {
            while let Some(build_row_id) = self.next_joined_build_row_id() {
                self.set_build_matched(build_row_id)?;
            }

            self.cur_probe_row_id += 1;
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
        }

        Ok(None)
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
                    self.cur_probe_row_id += 1;
                    // We must put the rest of `cur_build_row_id` here because we may reenter this
                    // method.
                    if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                        self.cur_joined_build_row_id = self
                            .first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
                    }
                    return Ok(Some(ret_data_chunk));
                }
            }

            self.cur_probe_row_id += 1;
            // We must put the rest of `cur_build_row_id` here because we may reenter this method.
            if self.cur_probe_row_id < self.current_probe_data_chunk_size() {
                self.cur_joined_build_row_id =
                    self.first_joined_row_id(self.current_probe_key_at(self.cur_probe_row_id));
            }
        }

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

    fn append_one_row(
        &mut self,
        build_row_id: Option<RowId>,
        probe_row_id: Option<usize>,
    ) -> Result<Option<DataChunk>> {
        let row = self
            .params
            .output_columns()
            .iter()
            .copied()
            .map(|column_id| {
                match column_id {
                    // probe side column
                    Either::Left(idx) => probe_row_id.map(|row_id| {
                        (
                            self.cur_probe_data
                                .as_ref()
                                .unwrap()
                                .probe_data_chunk
                                .columns()[idx]
                                .array_ref(),
                            row_id,
                        )
                    }),
                    // build side column
                    Either::Right(idx) => build_row_id.map(|row_id| {
                        (self.get_build_array(row_id, idx), row_id.row_id() as usize)
                    }),
                }
            });

        self.data_chunk_builder.borrow_mut().append_one_row(row)
    }

    fn get_build_array(&self, row_id: RowId, idx: usize) -> &ArrayImpl {
        self.build_data[row_id.chunk_id()].columns()[idx].array_ref()
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

impl JoinType {
    fn need_build_flag(self) -> bool {
        match self {
            JoinType::RightSemi => true,
            other => other.need_join_remaining(),
        }
    }
}
