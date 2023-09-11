//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.2

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "actor")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub actor_id: i32,
    pub fragment_id: i32,
    pub status: Option<String>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub splits: Option<Json>,
    pub parallel_unit_id: i32,
    pub upstream_actor_ids: Option<Vec<i32>>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub dispatchers: Option<Json>,
    pub vnode_bitmap: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::FragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Fragment,
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
