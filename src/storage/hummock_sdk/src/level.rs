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

use std::mem::size_of;

use itertools::Itertools;
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::{
    PbInputLevel, PbLevel, PbLevelType, PbOverlappingLevel, PbSstableInfo,
};

use crate::sstable_info::SstableInfo;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct OverlappingLevelCommon<T> {
    pub sub_levels: Vec<LevelCommon<T>>,
    pub total_file_size: u64,
    pub uncompressed_file_size: u64,
}

pub type OverlappingLevel = OverlappingLevelCommon<SstableInfo>;

impl<T> From<&PbOverlappingLevel> for OverlappingLevelCommon<T>
where
    for<'a> LevelCommon<T>: From<&'a PbLevel>,
{
    fn from(pb_overlapping_level: &PbOverlappingLevel) -> Self {
        Self {
            sub_levels: pb_overlapping_level
                .sub_levels
                .iter()
                .map(LevelCommon::from)
                .collect_vec(),
            total_file_size: pb_overlapping_level.total_file_size,
            uncompressed_file_size: pb_overlapping_level.uncompressed_file_size,
        }
    }
}

impl<T> From<&OverlappingLevelCommon<T>> for PbOverlappingLevel
where
    for<'a> &'a LevelCommon<T>: Into<PbLevel>,
{
    fn from(overlapping_level: &OverlappingLevelCommon<T>) -> Self {
        Self {
            sub_levels: overlapping_level
                .sub_levels
                .iter()
                .map(|level| level.into())
                .collect_vec(),
            total_file_size: overlapping_level.total_file_size,
            uncompressed_file_size: overlapping_level.uncompressed_file_size,
        }
    }
}

impl<T> From<OverlappingLevelCommon<T>> for PbOverlappingLevel
where
    LevelCommon<T>: Into<PbLevel>,
{
    fn from(overlapping_level: OverlappingLevelCommon<T>) -> Self {
        Self {
            sub_levels: overlapping_level
                .sub_levels
                .into_iter()
                .map(|pb_level| pb_level.into())
                .collect_vec(),
            total_file_size: overlapping_level.total_file_size,
            uncompressed_file_size: overlapping_level.uncompressed_file_size,
        }
    }
}

impl<T> From<PbOverlappingLevel> for OverlappingLevelCommon<T>
where
    LevelCommon<T>: From<PbLevel>,
{
    fn from(pb_overlapping_level: PbOverlappingLevel) -> Self {
        Self {
            sub_levels: pb_overlapping_level
                .sub_levels
                .into_iter()
                .map(LevelCommon::from)
                .collect_vec(),
            total_file_size: pb_overlapping_level.total_file_size,
            uncompressed_file_size: pb_overlapping_level.uncompressed_file_size,
        }
    }
}

impl OverlappingLevel {
    pub fn estimated_encode_len(&self) -> usize {
        self.sub_levels
            .iter()
            .map(|level| level.estimated_encode_len())
            .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LevelCommon<T> {
    pub level_idx: u32,
    pub level_type: PbLevelType,
    pub table_infos: Vec<T>,
    pub total_file_size: u64,
    pub sub_level_id: u64,
    pub uncompressed_file_size: u64,
    pub vnode_partition_count: u32,
}

pub type Level = LevelCommon<SstableInfo>;

impl<T> From<&PbLevel> for LevelCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_level: &PbLevel) -> Self {
        Self {
            level_idx: pb_level.level_idx,
            level_type: PbLevelType::try_from(pb_level.level_type).unwrap(),
            table_infos: pb_level.table_infos.iter().map(Into::into).collect_vec(),
            total_file_size: pb_level.total_file_size,
            sub_level_id: pb_level.sub_level_id,
            uncompressed_file_size: pb_level.uncompressed_file_size,
            vnode_partition_count: pb_level.vnode_partition_count,
        }
    }
}

impl<T> From<&LevelCommon<T>> for PbLevel
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(level: &LevelCommon<T>) -> Self {
        Self {
            level_idx: level.level_idx,
            level_type: level.level_type.into(),
            table_infos: level.table_infos.iter().map(Into::into).collect_vec(),
            total_file_size: level.total_file_size,
            sub_level_id: level.sub_level_id,
            uncompressed_file_size: level.uncompressed_file_size,
            vnode_partition_count: level.vnode_partition_count,
        }
    }
}

impl<T> From<LevelCommon<T>> for PbLevel
where
    PbSstableInfo: From<T>,
{
    fn from(level: LevelCommon<T>) -> Self {
        Self {
            level_idx: level.level_idx,
            level_type: level.level_type.into(),
            table_infos: level.table_infos.into_iter().map(Into::into).collect_vec(),
            total_file_size: level.total_file_size,
            sub_level_id: level.sub_level_id,
            uncompressed_file_size: level.uncompressed_file_size,
            vnode_partition_count: level.vnode_partition_count,
        }
    }
}

impl<T> From<PbLevel> for LevelCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_level: PbLevel) -> Self {
        Self {
            level_idx: pb_level.level_idx,
            level_type: PbLevelType::try_from(pb_level.level_type).unwrap(),
            table_infos: pb_level
                .table_infos
                .into_iter()
                .map(Into::into)
                .collect_vec(),
            total_file_size: pb_level.total_file_size,
            sub_level_id: pb_level.sub_level_id,
            uncompressed_file_size: pb_level.uncompressed_file_size,
            vnode_partition_count: pb_level.vnode_partition_count,
        }
    }
}

impl Level {
    pub fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<u32>()
            + self
                .table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u32>()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LevelsCommon<T> {
    pub levels: Vec<LevelCommon<T>>,
    pub l0: OverlappingLevelCommon<T>,
    pub group_id: u64,
    pub parent_group_id: u64,

    #[deprecated]
    pub member_table_ids: Vec<u32>,
    pub compaction_group_version_id: u64,
}

pub type Levels = LevelsCommon<SstableInfo>;

impl Levels {
    pub fn level0(&self) -> &OverlappingLevel {
        &self.l0
    }

    pub fn get_level(&self, level_idx: usize) -> &Level {
        &self.levels[level_idx - 1]
    }

    pub fn get_level_mut(&mut self, level_idx: usize) -> &mut Level {
        &mut self.levels[level_idx - 1]
    }

    pub fn is_last_level(&self, level_idx: u32) -> bool {
        self.levels
            .last()
            .as_ref()
            .is_some_and(|level| level.level_idx == level_idx)
    }

    pub fn count_ssts(&self) -> usize {
        self.level0()
            .sub_levels
            .iter()
            .chain(self.levels.iter())
            .map(|level| level.table_infos.len())
            .sum()
    }
}

impl<T> LevelsCommon<T>
where
    PbLevels: for<'a> From<&'a LevelsCommon<T>>,
{
    pub fn to_protobuf(&self) -> PbLevels {
        self.into()
    }
}

impl<T> LevelsCommon<T>
where
    LevelsCommon<T>: for<'a> From<&'a PbLevels>,
{
    pub fn from_protobuf(pb_levels: &PbLevels) -> LevelsCommon<T> {
        LevelsCommon::<T>::from(pb_levels)
    }
}

impl Levels {
    pub fn estimated_encode_len(&self) -> usize {
        let mut basic = self
            .levels
            .iter()
            .map(|level| level.estimated_encode_len())
            .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u32>();
        basic += self.l0.estimated_encode_len();

        basic
    }
}

impl<T> From<&PbLevels> for LevelsCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    #[expect(deprecated)]
    fn from(pb_levels: &PbLevels) -> Self {
        Self {
            l0: OverlappingLevelCommon::from(pb_levels.l0.as_ref().unwrap()),
            levels: pb_levels.levels.iter().map(Into::into).collect_vec(),
            group_id: pb_levels.group_id,
            parent_group_id: pb_levels.parent_group_id,
            member_table_ids: pb_levels.member_table_ids.clone(),
            compaction_group_version_id: pb_levels.compaction_group_version_id,
        }
    }
}

impl<T> From<&LevelsCommon<T>> for PbLevels
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    #[expect(deprecated)]
    fn from(levels: &LevelsCommon<T>) -> Self {
        Self {
            l0: Some((&levels.l0).into()),
            levels: levels.levels.iter().map(PbLevel::from).collect_vec(),
            group_id: levels.group_id,
            parent_group_id: levels.parent_group_id,
            member_table_ids: levels.member_table_ids.clone(),
            compaction_group_version_id: levels.compaction_group_version_id,
        }
    }
}

impl<T> From<PbLevels> for LevelsCommon<T>
where
    T: From<PbSstableInfo>,
{
    #[expect(deprecated)]
    fn from(pb_levels: PbLevels) -> Self {
        Self {
            l0: OverlappingLevelCommon::from(pb_levels.l0.unwrap()),
            levels: pb_levels
                .levels
                .into_iter()
                .map(LevelCommon::from)
                .collect_vec(),
            group_id: pb_levels.group_id,
            parent_group_id: pb_levels.parent_group_id,
            member_table_ids: pb_levels.member_table_ids,
            compaction_group_version_id: pb_levels.compaction_group_version_id,
        }
    }
}

impl<T> From<LevelsCommon<T>> for PbLevels
where
    PbSstableInfo: From<T>,
{
    fn from(levels: LevelsCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            l0: Some(levels.l0.into()),
            levels: levels.levels.into_iter().map(PbLevel::from).collect_vec(),
            group_id: levels.group_id,
            parent_group_id: levels.parent_group_id,
            member_table_ids: levels.member_table_ids,
            compaction_group_version_id: levels.compaction_group_version_id,
        }
    }
}

#[derive(Clone, PartialEq, Default, Debug)]
pub struct InputLevel {
    pub level_idx: u32,
    pub level_type: PbLevelType,
    pub table_infos: Vec<SstableInfo>,
}

impl InputLevel {
    pub fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<i32>()
            + self
                .table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
    }
}

impl From<PbInputLevel> for InputLevel {
    fn from(pb_input_level: PbInputLevel) -> Self {
        Self {
            level_idx: pb_input_level.level_idx,
            level_type: PbLevelType::try_from(pb_input_level.level_type).unwrap(),
            table_infos: pb_input_level
                .table_infos
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
        }
    }
}

impl From<&PbInputLevel> for InputLevel {
    fn from(pb_input_level: &PbInputLevel) -> Self {
        Self {
            level_idx: pb_input_level.level_idx,
            level_type: PbLevelType::try_from(pb_input_level.level_type).unwrap(),
            table_infos: pb_input_level
                .table_infos
                .iter()
                .map(SstableInfo::from)
                .collect_vec(),
        }
    }
}

impl From<InputLevel> for PbInputLevel {
    fn from(input_level: InputLevel) -> Self {
        Self {
            level_idx: input_level.level_idx,
            level_type: input_level.level_type.into(),
            table_infos: input_level
                .table_infos
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
        }
    }
}

impl From<&InputLevel> for PbInputLevel {
    fn from(input_level: &InputLevel) -> Self {
        Self {
            level_idx: input_level.level_idx,
            level_type: input_level.level_type.into(),
            table_infos: input_level
                .table_infos
                .iter()
                .map(|sst| sst.into())
                .collect_vec(),
        }
    }
}
