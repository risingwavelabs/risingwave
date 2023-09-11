//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.2

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "user")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub user_id: i32,
    pub name: String,
    pub is_super: Option<bool>,
    pub can_create_db: Option<bool>,
    pub can_create_user: Option<bool>,
    pub can_login: Option<bool>,
    pub auth_type: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::connection::Entity")]
    Connection,
    #[sea_orm(has_many = "super::database::Entity")]
    Database,
    #[sea_orm(has_many = "super::function::Entity")]
    Function,
    #[sea_orm(has_many = "super::index::Entity")]
    Index,
    #[sea_orm(has_many = "super::schema::Entity")]
    Schema,
    #[sea_orm(has_many = "super::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "super::source::Entity")]
    Source,
    #[sea_orm(has_many = "super::table::Entity")]
    Table,
    #[sea_orm(has_many = "super::view::Entity")]
    View,
}

impl Related<super::connection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Connection.def()
    }
}

impl Related<super::database::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Database.def()
    }
}

impl Related<super::function::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Function.def()
    }
}

impl Related<super::index::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Index.def()
    }
}

impl Related<super::schema::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Schema.def()
    }
}

impl Related<super::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl Related<super::view::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::View.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
