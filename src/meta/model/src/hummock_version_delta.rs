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
use risingwave_pb::hummock::PbHummockVersionDelta;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::{Epoch, HummockVersionId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_version_delta")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub max_committed_epoch: Epoch,
    pub safe_epoch: Epoch,
    pub trivial_move: bool,
    pub full_version_delta: FullVersionDelta,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_from_blob!(FullVersionDelta, PbHummockVersionDelta);

impl From<Model> for PbHummockVersionDelta {
    fn from(value: Model) -> Self {
        let ret = value.full_version_delta.to_protobuf();
        assert_eq!(value.id, ret.id as i64);
        assert_eq!(value.prev_id, ret.prev_id as i64);
        assert_eq!(value.trivial_move, ret.trivial_move);
        ret
    }
}

const FIELDS: [&str; 6] = [
    "_id",
    "prev_id",
    "max_committed_epoch",
    "safe_epoch",
    "trivial_move",
    "full_version_delta",
];

pub struct MongoDb {
    hummock_version_delta: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 6)?;
        state.serialize_field("_id", &self.hummock_version_delta.id)?;
        state.serialize_field("worker_type", &self.hummock_version_delta.prev_id)?;
        state.serialize_field("host", &self.hummock_version_delta.max_committed_epoch)?;
        state.serialize_field("port", &self.hummock_version_delta.safe_epoch)?;
        state.serialize_field("status", &self.hummock_version_delta.trivial_move)?;
        state.serialize_field("transaction_id", &self.hummock_version_delta.full_version_delta)?;
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
                let mut id: Option<HummockVersionId> = None;
                let mut prev_id: Option<HummockVersionId> = None;
                let mut max_committed_epoch: Option<Epoch> = None;
                let mut safe_epoch: Option<Epoch> = None;
                let mut trivial_move: Option<bool> = None;
                let mut full_version_delta: Option<FullVersionDelta> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => {
                            id =
                                Some(i64::deserialize(value).unwrap())
                        }
                        "prev_id" => prev_id = Some(i64::deserialize(value).unwrap()),
                        "max_committed_epoch" => max_committed_epoch = Some(i64::deserialize(value).unwrap()),
                        "safe_epoch" => safe_epoch = Some(i64::deserialize(value).unwrap()),
                        "trivial_move" => trivial_move = Some(value.parse::<bool>().unwrap()),
                        "full_version_delta" => {
                            full_version_delta =
                                Some(FullVersionDelta::deserialize(value).unwrap())
                        }
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_version_delta = Model {
                    id: id.ok_or_else(|| Error::missing_field("_id"))?,
                    prev_id: prev_id.ok_or_else(|| Error::missing_field("prev_id"))?,
                    max_committed_epoch: max_committed_epoch.ok_or_else(|| Error::missing_field("max_committed_epoch"))?,
                    safe_epoch: safe_epoch.ok_or_else(|| Error::missing_field("safe_epoch"))?,
                    trivial_move: trivial_move.ok_or_else(|| Error::missing_field("trivial_move"))?,
                    full_version_delta: full_version_delta.ok_or_else(|| Error::missing_field("full_version_delta"))?,
                };
                Ok(Self::Value {hummock_version_delta})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
