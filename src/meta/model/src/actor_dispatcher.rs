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

use std::hash::Hash;

use risingwave_pb::stream_plan::PbDispatcherType;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(
    Hash, Copy, Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum DispatcherType {
    #[sea_orm(string_value = "HASH")]
    Hash,
    #[sea_orm(string_value = "BROADCAST")]
    Broadcast,
    #[sea_orm(string_value = "SIMPLE")]
    Simple,
    #[sea_orm(string_value = "NO_SHUFFLE")]
    NoShuffle,
}

impl From<PbDispatcherType> for DispatcherType {
    fn from(val: PbDispatcherType) -> Self {
        match val {
            PbDispatcherType::Unspecified => unreachable!(),
            PbDispatcherType::Hash => DispatcherType::Hash,
            PbDispatcherType::Broadcast => DispatcherType::Broadcast,
            PbDispatcherType::Simple => DispatcherType::Simple,
            PbDispatcherType::NoShuffle => DispatcherType::NoShuffle,
        }
    }
}

impl From<DispatcherType> for PbDispatcherType {
    fn from(val: DispatcherType) -> Self {
        match val {
            DispatcherType::Hash => PbDispatcherType::Hash,
            DispatcherType::Broadcast => PbDispatcherType::Broadcast,
            DispatcherType::Simple => PbDispatcherType::Simple,
            DispatcherType::NoShuffle => PbDispatcherType::NoShuffle,
        }
    }
}
