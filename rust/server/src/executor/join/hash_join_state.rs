use crate::array2::column::Column;
use crate::array2::{ArrayBuilderImpl, DataChunk, DataChunkRef};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::executor::hash_map::{HashKey, PrecomputedBuildHasher};
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

const END: RowId = RowId {
    chunk_id: u32::MAX,
    row_id: u32::MAX,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub(super) struct RowId {
    chunk_id: u32,
    row_id: u32,
}

impl RowId {
    #[inline(always)]
    fn is_end(&self) -> bool {
        *self == END
    }

    #[inline(always)]
    fn chunk_id(&self) -> usize {
        self.chunk_id as usize
    }

    #[inline(always)]
    fn row_id(&self) -> usize {
        self.row_id as usize
    }

    #[inline(always)]
    fn next_chunk(self) -> RowId {
        RowId {
            chunk_id: self.chunk_id + 1,
            row_id: 0,
        }
    }

    #[inline(always)]
    fn next_row(self) -> RowId {
        RowId {
            chunk_id: self.chunk_id,
            row_id: self.row_id + 1,
        }
    }
}

#[derive(Default)]
pub(super) struct BuildTable {
    build_data: Vec<DataChunkRef>,
    row_count: usize,
}

impl BuildTable {
    pub(super) fn append_build_chunk(&mut self, data_chunk: DataChunkRef) -> Result<()> {
        ensure!(
            (MAX_BUILD_ROW_COUNT - self.row_count) > data_chunk.cardinality(),
            "Build table size exceeded limit!"
        );
        self.row_count += data_chunk.cardinality();
        self.build_data.push(data_chunk);
        Ok(())
    }
}

pub(super) struct ProbeTable<K> {
    build_table: JoinHashMap<K>,
    build_data: Vec<DataChunkRef>,
    build_index: Vec<Vec<RowId>>,
    build_matched: RefCell<Option<Vec<Vec<bool>>>>,
    cur_probe_data_chunk: Option<DataChunkRef>,
    params: EquiJoinParams,
}

impl<K> Default for ProbeTable<K> {
    fn default() -> Self {
        Self {
            build_table: JoinHashMap::default(),
            build_data: Vec::default(),
            build_index: Vec::default(),
            build_matched: RefCell::new(None),
            cur_probe_data_chunk: None,
            params: EquiJoinParams::default(),
        }
    }
}

struct JoinedRowIdIterator<'a> {
    cur: RowId,
    index: &'a Vec<Vec<RowId>>,
}

impl<'a> Iterator for JoinedRowIdIterator<'a> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur.is_end() {
            None
        } else {
            let ret = self.cur;
            let next = self.index[self.cur.chunk_id()][self.cur.row_id()];
            self.cur = next;
            Some(ret)
        }
    }
}

struct AllRowIdIterator<'a> {
    cur: RowId,
    data: &'a Vec<DataChunkRef>,
}

impl<'a> Iterator for AllRowIdIterator<'a> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.get(self.cur.chunk_id()) {
            Some(chunk) => {
                let ret = Some(self.cur);
                if self.cur.row_id() > chunk.cardinality() {
                    self.cur = self.cur.next_chunk();
                } else {
                    self.cur = self.cur.next_row();
                }

                ret
            }
            None => None,
        }
    }
}

impl<K> TryFrom<BuildTable> for ProbeTable<K> {
    type Error = RwError;

    fn try_from(_build_table: BuildTable) -> Result<Self> {
        todo!()
    }
}

impl<K: HashKey> ProbeTable<K> {
    // TODO: Should we consider output multi data chunks when necessary?
    pub(super) fn join(&mut self, data_chunk: DataChunkRef) -> Result<DataChunkRef> {
        self.cur_probe_data_chunk = Some(data_chunk.clone());
        let probe_keys = K::build(self.params.probe_key_columns(), data_chunk.as_ref())?;
        let mut output_array_builders =
            self.create_output_array_builders(data_chunk.cardinality())?;

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

    pub(super) fn join_remaining(&mut self) -> Result<Option<DataChunkRef>> {
        // TODO: Count null values
        let mut output_array_builders = self.create_output_array_builders(2048)?;

        match self.params.join_type() {
            JoinType::RightAnti => self.do_right_anti_join_remaining(&mut output_array_builders)?,
            JoinType::FullOuter => self.do_full_outer_join_remaining(&mut output_array_builders)?,
            _ => return Ok(None),
        };

        self.create_data_chunk(output_array_builders).map(Some)
    }

    fn create_data_chunk(
        &self,
        output_array_builders: Vec<ArrayBuilderImpl>,
    ) -> Result<DataChunkRef> {
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

        DataChunk::try_from(columns).map(Arc::new)
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
                Some(first_joined_row) => {
                    self.joined_row_ids_from(first_joined_row)
                        .try_for_each(|build_row_id| {
                            self.append_one_row(
                                Some(build_row_id),
                                Some(row_id),
                                output_array_builders,
                            )
                        })?
                }
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
                    for build_row_id in self.joined_row_ids_from(first_joined_row) {
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
                flags[build_row_id.chunk_id()][build_row_id.row_id()] = true;
                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    fn is_build_matched(&self, build_row_id: RowId) -> Result<bool> {
        match self.build_matched.borrow().deref() {
            Some(flags) => Ok(flags[build_row_id.chunk_id()][build_row_id.row_id()]),
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

    fn joined_row_ids_from(&self, start: RowId) -> impl Iterator<Item = RowId> + '_ {
        JoinedRowIdIterator {
            cur: start,
            index: &self.build_index,
        }
    }

    fn all_joined_row_ids(&self, probe_key: &K) -> impl Iterator<Item = RowId> + '_ {
        match self.first_joined_row_id(probe_key) {
            Some(first_joined_row_id) => self.joined_row_ids_from(first_joined_row_id),
            None => self.joined_row_ids_from(END),
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
        AllRowIdIterator {
            cur: RowId::default(),
            data: &self.build_data,
        }
    }
}

pub(super) type JoinHashMap<K> = HashMap<K, RowId, PrecomputedBuildHasher>;
