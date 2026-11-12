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

use crate::{Epoch, ReplacedSinkCatalog, SinkId, SinkSchemachange};

/// Exactly-once epochs that a sink had pre-committed but not yet committed to the external
/// system when `REPLACE SINK` removed it.
///
/// The rows are moved here from `pending_sink_state` in the transaction that deletes the replaced
/// sink, and are committed by the sink coordinator manager afterwards. The successor sink must not
/// commit to the external system while any of its rows remain.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "replaced_sink_pending_state")]
pub struct Model {
    /// The replaced sink. Its catalog object no longer exists.
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub epoch: Epoch,
    /// The sink that replaced `sink_id`. Dropping it discards the rows.
    pub successor_sink_id: SinkId,
    /// Catalog of the replaced sink, used to rebuild its commit coordinator. Secrets stay as
    /// references and are resolved when the coordinator is built.
    pub sink: ReplacedSinkCatalog,
    pub metadata: Option<Vec<u8>>,
    pub schema_change: Option<SinkSchemachange>,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SuccessorSinkId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}
