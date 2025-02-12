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

use std::fmt::Formatter;
use risingwave_pb::user::grant_privilege::PbAction;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::{ObjectId, PrivilegeId, UserId};

#[derive(
    Clone, Copy, Debug, Hash, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum Action {
    #[sea_orm(string_value = "INSERT")]
    Insert,
    #[sea_orm(string_value = "SELECT")]
    Select,
    #[sea_orm(string_value = "UPDATE")]
    Update,
    #[sea_orm(string_value = "DELETE")]
    Delete,
    #[sea_orm(string_value = "USAGE")]
    Usage,
    #[sea_orm(string_value = "CREATE")]
    Create,
    #[sea_orm(string_value = "CONNECT")]
    Connect,
    #[sea_orm(string_value = "EXECUTE")]
    Execute,
}

impl From<PbAction> for Action {
    fn from(action: PbAction) -> Self {
        match action {
            PbAction::Unspecified => unreachable!("unspecified action"),
            PbAction::Insert => Self::Insert,
            PbAction::Select => Self::Select,
            PbAction::Update => Self::Update,
            PbAction::Delete => Self::Delete,
            PbAction::Usage => Self::Usage,
            PbAction::Create => Self::Create,
            PbAction::Connect => Self::Connect,
            PbAction::Execute => Self::Execute,
        }
    }
}

impl From<Action> for PbAction {
    fn from(action: Action) -> Self {
        match action {
            Action::Insert => Self::Insert,
            Action::Select => Self::Select,
            Action::Update => Self::Update,
            Action::Delete => Self::Delete,
            Action::Usage => Self::Usage,
            Action::Create => Self::Create,
            Action::Connect => Self::Connect,
            Action::Execute => Self::Execute,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "user_privilege")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: PrivilegeId,
    pub dependent_id: Option<PrivilegeId>,
    pub user_id: UserId,
    pub oid: ObjectId,
    pub granted_by: UserId,
    pub action: Action,
    pub with_grant_option: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::Oid",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Object,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::GrantedBy",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    User2,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User1,
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::DependentId",
        to = "Column::Id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SelfRef,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

const FIELDS: [&str; 7] = [
    "_id",
    "dependent_id",
    "user_id",
    "oid",
    "granted_by",
    "action",
    "with_grant_option",
];

pub struct MongoDb {
    pub user_privilege: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = match &self.user_privilege.dependent_id {
            Some(dependent_id) => {
                let mut state  =serializer.serialize_struct("MongoDb", 7)?;
                state.serialize_field("dependent_id", dependent_id)?;
                state
            },
            None => serializer.serialize_struct("MongoDb", 6)?
        };
        state.serialize_field("_id", &self.user_privilege.id)?;
        state.serialize_field("user_id", &self.user_privilege.user_id)?;
        state.serialize_field("oid", &self.user_privilege.oid)?;
        state.serialize_field("granted_by", &self.user_privilege.granted_by)?;
        state.serialize_field("action", &self.user_privilege.action)?;
        state.serialize_field("with_grant_option", &self.user_privilege.with_grant_option)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MongoDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MongoDbVisitor;
        impl<'de> Visitor<'de> for MongoDbVisitor {
            type Value = MongoDb;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("MongoDb")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut id: Option<PrivilegeId> = None;
                let mut dependent_id: Option<PrivilegeId> = None;
                let mut user_id: Option<UserId> = None;
                let mut oid: Option<ObjectId> = None;
                let mut granted_by: Option<UserId> = None;
                let mut action: Option<Action> = None;
                let mut with_grant_option: Option<bool> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => id = Some(i32::deserialize(value).unwrap()),
                        "dependent_id" => dependent_id = Some(i32::deserialize(value).unwrap()),
                        "user_id" => user_id = Some(i32::deserialize(value).unwrap()),
                        "oid" => oid = Some(i32::deserialize(value).unwrap()),
                        "granted_by" => granted_by = Some(i32::deserialize(value).unwrap()),
                        "action" => action = Some(Action::deserialize(value).unwrap()),
                        "with_grant_option" => with_grant_option = Some(bool::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let user_privilege = Model {
                    id: id.ok_or_else(|| Error::missing_field("_id"))?,
                    dependent_id,
                    user_id: user_id.ok_or_else(|| Error::missing_field("user_id"))?,
                    oid: oid.ok_or_else(|| Error::missing_field("oid"))?,
                    granted_by: granted_by.ok_or_else(|| Error::missing_field("granted_by"))?,
                    action: action.ok_or_else(|| Error::missing_field("action"))?,
                    with_grant_option: with_grant_option.ok_or_else(|| Error::missing_field("with_grant_option"))?,
                };
                Ok(Self::Value { user_privilege })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
