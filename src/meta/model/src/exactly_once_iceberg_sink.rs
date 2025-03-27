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

use sea_orm::entity::prelude::*;
use sea_orm::{DeriveEntityModel, DeriveRelation, EnumIter};

use crate::{Epoch, SinkId};
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "exactly_once_iceberg_sink_metadata")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub end_epoch: Epoch,
    pub start_epoch: Epoch,
    pub metadata: ::prost::alloc::vec::Vec<u8>,
    pub snapshot_id: i64,
    pub committed: bool,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
