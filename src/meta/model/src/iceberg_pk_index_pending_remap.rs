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

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::SinkId;

/// Durable record of a pk-index compaction remap whose overwrite committed but whose writer-side
/// pk-index remap may not yet have been applied. Written before the `IcebergPkIndexRemap` barrier
/// mutation is enqueued, so a crash in the window between the overwrite commit and the mutation
/// delivery is recoverable: on meta startup every un-satisfied row re-triggers its mutation.
///
/// `mapping_paths` and `input_files` are JSON-encoded `Vec<String>` blobs (row-provenance NDJSON
/// paths and the removed input DATA/DV file paths respectively). Keyed by `(sink_id, remap_id)`,
/// where `remap_id` is the compaction task id, so more than one concurrent pending remap per sink
/// (disjoint input sets) is representable.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "iceberg_pk_index_pending_remap")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub remap_id: i64,
    pub mapping_paths: Vec<u8>,
    pub input_files: Vec<u8>,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::sink::Entity",
        from = "Column::SinkId",
        to = "super::sink::Column::SinkId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Sink,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SinkId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}
