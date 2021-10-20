use crate::array::column::Column;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::executor::hash_map::{HashKey, PrecomputedBuildHasher};
use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::join::hash_join::EquiJoinParams;
use crate::executor::join::JoinType;
use crate::types::DataType;
use either::Either;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

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
        self.row_count += data_chunk.cardinality();
        self.build_data.push(data_chunk);
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
    build_matched: RefCell<Option<ChunkedData<bool>>>,
    cur_probe_data_chunk: Option<DataChunk>,
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

        let mut build_matched = RefCell::new(None);
        if build_table.params.join_type().need_join_remaining() {
            build_matched = RefCell::new(Some(ChunkedData::<bool>::with_chunk_sizes(
                build_table.build_data.iter().map(|c| c.cardinality()),
            )?));
        }

        Ok(Self {
            build_table: hash_map,
            build_data: build_table.build_data,
            build_index,
            build_matched,
            cur_probe_data_chunk: None,
            params: build_table.params,
        })
    }
}

impl<K: HashKey> ProbeTable<K> {
    pub(super) fn join_type(&self) -> JoinType {
        self.params.join_type()
    }

    // TODO: Should we consider output multi data chunks when necessary?
    pub(super) fn join(&mut self, data_chunk: DataChunk) -> Result<DataChunk> {
        let probe_keys = K::build(self.params.probe_key_columns(), &data_chunk)?;
        let mut output_array_builders =
            self.create_output_array_builders(data_chunk.cardinality())?;
        self.cur_probe_data_chunk = Some(data_chunk);

        match self.params.join_type() {
            JoinType::Inner => self.do_inner_join(probe_keys, &mut output_array_builders)?,
            JoinType::LeftOuter => {
                self.do_left_outer_join(probe_keys, &mut output_array_builders)?
            }
            JoinType::LeftAnti => self.do_left_anti_join(probe_keys, &mut output_array_builders)?,
            JoinType::LeftSemi => self.do_left_semi_join(probe_keys, &mut output_array_builders)?,
            JoinType::RightOuter => {
                self.do_right_outer_join(probe_keys, &mut output_array_builders)?
            }
            JoinType::RightAnti => {
                self.do_right_anti_join(probe_keys, &mut output_array_builders)?
            }
            JoinType::RightSemi => {
                self.do_right_semi_join(probe_keys, &mut output_array_builders)?
            }
            JoinType::FullOuter => {
                self.do_full_outer_join(probe_keys, &mut output_array_builders)?
            }
        };

        self.create_data_chunk(output_array_builders)
    }

    pub(super) fn join_remaining(&mut self) -> Result<Option<DataChunk>> {
        // TODO: Count null values
        let mut output_array_builders = self.create_output_array_builders(2048)?;

        match self.params.join_type() {
            JoinType::RightAnti | JoinType::RightOuter => {
                self.do_right_anti_join_remaining(&mut output_array_builders)?
            }
            JoinType::FullOuter => self.do_full_outer_join_remaining(&mut output_array_builders)?,
            _ => return Ok(None),
        };

        self.create_data_chunk(output_array_builders).map(Some)
    }

    fn create_data_chunk(&self, output_array_builders: Vec<ArrayBuilderImpl>) -> Result<DataChunk> {
        let columns = output_array_builders
            .into_iter()
            .zip(self.params.output_types().iter())
            .try_fold(
                Vec::with_capacity(self.params.output_types().len()),
                |mut vec, (array_builder, data_type)| -> Result<Vec<Column>> {
                    let array = array_builder.finish()?;
                    let column = Column::new(Arc::new(array), data_type.clone());
                    vec.push(column);
                    Ok(vec)
                },
            )?;

        DataChunk::try_from(columns)
    }

    fn do_inner_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            for joined_row_id in self.all_joined_row_ids(probe_key) {
                self.append_one_row(Some(joined_row_id), Some(row_id), output_array_builders)?;
            }
        }

        Ok(())
    }

    fn do_left_outer_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            match self.first_joined_row_id(probe_key) {
                Some(first_joined_row) => self
                    .joined_row_ids_from(Some(first_joined_row))
                    .try_for_each(|build_row_id| {
                        self.append_one_row(Some(build_row_id), Some(row_id), output_array_builders)
                    })?,
                None => self.append_one_row(None, Some(row_id), output_array_builders)?,
            }
        }

        Ok(())
    }

    fn do_left_semi_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            self.all_joined_row_ids(probe_key)
                .take(1)
                .try_for_each(|_| self.append_one_row(None, Some(row_id), output_array_builders))?;
        }

        Ok(())
    }

    fn do_left_anti_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            match self.first_joined_row_id(probe_key) {
                Some(_) => (),
                None => self.append_one_row(None, Some(row_id), output_array_builders)?,
            }
        }

        Ok(())
    }

    fn do_right_outer_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            for build_row_id in self.all_joined_row_ids(probe_key) {
                self.append_one_row(Some(build_row_id), Some(row_id), output_array_builders)?;
                self.set_build_matched(build_row_id)?;
            }
        }

        Ok(())
    }

    fn do_right_semi_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for probe_key in probe_keys.iter() {
            for build_row_id in self.all_joined_row_ids(probe_key) {
                if !self.is_build_matched(build_row_id)? {
                    self.append_one_row(Some(build_row_id), None, output_array_builders)?;
                    self.set_build_matched(build_row_id)?;
                }
            }
        }

        Ok(())
    }

    fn do_right_anti_join(
        &mut self,
        probe_keys: Vec<K>,
        _output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for probe_key in probe_keys.iter() {
            for build_row_id in self.all_joined_row_ids(probe_key) {
                self.set_build_matched(build_row_id)?
            }
        }

        Ok(())
    }

    fn do_right_anti_join_remaining(
        &mut self,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for build_row_id in self.all_build_row_ids() {
            if !self.is_build_matched(build_row_id)? {
                self.append_one_row(Some(build_row_id), None, output_array_builders)?;
            }
        }

        Ok(())
    }

    fn do_full_outer_join(
        &mut self,
        probe_keys: Vec<K>,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (row_id, probe_key) in probe_keys.iter().enumerate() {
            match self.first_joined_row_id(probe_key) {
                Some(first_joined_row) => {
                    for build_row_id in self.joined_row_ids_from(Some(first_joined_row)) {
                        self.append_one_row(
                            Some(build_row_id),
                            Some(row_id),
                            output_array_builders,
                        )?;
                        self.set_build_matched(build_row_id)?;
                    }
                }
                None => self.append_one_row(None, Some(row_id), output_array_builders)?,
            }
        }

        Ok(())
    }

    fn do_full_outer_join_remaining(
        &mut self,
        output_array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for build_row_id in self.all_build_row_ids() {
            if !self.is_build_matched(build_row_id)? {
                self.append_one_row(Some(build_row_id), None, output_array_builders)?;
            }
        }

        Ok(())
    }

    fn set_build_matched(&self, build_row_id: RowId) -> Result<()> {
        match self.build_matched.borrow_mut().deref_mut() {
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
        match self.build_matched.borrow().deref() {
            Some(flags) => Ok(flags[build_row_id]),
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    fn create_output_array_builders(&self, capacity: usize) -> Result<Vec<ArrayBuilderImpl>> {
        self.params
            .output_types()
            .iter()
            .map(|t| DataType::create_array_builder(t.clone(), capacity))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()
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
        &self,
        build_row_id: Option<RowId>,
        probe_row_id: Option<usize>,
        array_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for (column_id, array_builder) in self
            .params
            .output_columns()
            .iter()
            .copied()
            .zip(array_builders)
        {
            match column_id {
                // probe side column
                Either::Left(idx) => match probe_row_id {
                    Some(row_id) => array_builder
                        .append_array_element(self.get_probe_array(idx)?.array_ref(), row_id)?,
                    None => array_builder.append_null()?,
                },
                // build side column
                Either::Right(idx) => match build_row_id {
                    Some(row_id) => array_builder.append_array_element(
                        self.get_build_array(row_id, idx)?.array_ref(),
                        row_id.row_id() as usize,
                    )?,
                    None => array_builder.append_null()?,
                },
            }
        }

        Ok(())
    }

    fn get_probe_array(&self, idx: usize) -> Result<Column> {
        self.cur_probe_data_chunk
            .as_ref()
            .ok_or_else(|| RwError::from(InternalError("Probe chunk not found!".to_string())))
            .and_then(|data_chunk| data_chunk.column_at(idx))
    }

    fn get_build_array(&self, row_id: RowId, idx: usize) -> Result<Column> {
        self.build_data[row_id.chunk_id()].column_at(idx)
    }

    fn all_build_row_ids(&self) -> impl Iterator<Item = RowId> + '_ {
        self.build_index.all_row_ids()
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
