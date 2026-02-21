use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::SourceId;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "source_external_schema")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: SourceId,
    pub version: String,
    pub content: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::source::Entity",
        from = "Column::SourceId",
        to = "super::source::Column::SourceId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Source,
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
