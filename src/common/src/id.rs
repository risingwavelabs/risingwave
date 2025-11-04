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

use std::any::type_name;
use std::num::TryFromIntError;

use parse_display::Display;
use sea_orm::sea_query::{ArrayType, ValueTypeErr};
use sea_orm::{ColIdx, ColumnType, DbErr, QueryResult, TryGetError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::catalog::OBJECT_ID_PLACEHOLDER;

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
#[display("{inner}")]
pub struct TypedId<const N: usize> {
    pub(crate) inner: u32,
}

impl<const N: usize> TypedId<N> {
    pub const fn new(inner: u32) -> Self {
        TypedId { inner }
    }

    pub fn as_raw_id(&self) -> u32 {
        self.inner
    }

    pub const fn placeholder() -> Self {
        Self {
            inner: OBJECT_ID_PLACEHOLDER,
        }
    }

    pub fn is_placeholder(&self) -> bool {
        self.inner == OBJECT_ID_PLACEHOLDER
    }

    fn from_i32(inner: i32) -> Self {
        Self {
            inner: inner.try_into().unwrap_or_else(|e: TryFromIntError| {
                if cfg!(debug_assertions) {
                    panic!(
                        "invalid i32 id {} for {}: {:?}",
                        inner,
                        type_name::<Self>(),
                        e.as_report()
                    );
                } else {
                    warn!(
                        "invalid i32 id {} for {}: {:?}",
                        inner,
                        type_name::<Self>(),
                        e.as_report()
                    );
                    inner as _
                }
            }),
        }
    }
}

impl<const N: usize> From<u32> for TypedId<N> {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl<const N: usize> From<&u32> for TypedId<N> {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl<const N: usize> From<TypedId<N>> for u32 {
    fn from(id: TypedId<N>) -> Self {
        id.inner
    }
}

impl<const N: usize> From<&TypedId<N>> for u32 {
    fn from(id: &TypedId<N>) -> Self {
        id.inner
    }
}

impl<const N: usize> From<TypedId<N>> for sea_orm::Value {
    fn from(value: TypedId<N>) -> Self {
        let inner: i32 = value.inner.try_into().unwrap_or_else(|e: TryFromIntError| {
            if cfg!(debug_assertions) {
                panic!(
                    "invalid u32 id {} for {}: {:?}",
                    value.inner,
                    type_name::<Self>(),
                    e.as_report()
                );
            } else {
                warn!(
                    "invalid u32 id {} for {}: {:?}",
                    value.inner,
                    type_name::<Self>(),
                    e.as_report()
                );
                value.inner as _
            }
        });
        sea_orm::Value::from(inner)
    }
}

impl<const N: usize> sea_orm::sea_query::ValueType for TypedId<N> {
    fn try_from(v: sea_orm::Value) -> Result<Self, ValueTypeErr> {
        let inner = <i32 as sea_orm::sea_query::ValueType>::try_from(v)?;
        Ok(Self::from_i32(inner))
    }

    fn type_name() -> String {
        <i32 as sea_orm::sea_query::ValueType>::type_name()
    }

    fn array_type() -> ArrayType {
        <i32 as sea_orm::sea_query::ValueType>::array_type()
    }

    fn column_type() -> ColumnType {
        <i32 as sea_orm::sea_query::ValueType>::column_type()
    }
}

impl<const N: usize> sea_orm::sea_query::Nullable for TypedId<N> {
    fn null() -> sea_orm::Value {
        <i32 as sea_orm::sea_query::Nullable>::null()
    }
}

impl<const N: usize> sea_orm::TryGetable for TypedId<N> {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let inner = <i32 as sea_orm::TryGetable>::try_get_by(res, index)?;
        Ok(Self::from_i32(inner))
    }
}

impl<const N: usize> sea_orm::TryFromU64 for TypedId<N> {
    fn try_from_u64(n: u64) -> Result<Self, DbErr> {
        let inner = <i32 as sea_orm::TryFromU64>::try_from_u64(n)?;
        Ok(Self::from_i32(inner))
    }
}

impl<const N: usize> Serialize for TypedId<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        <u32 as Serialize>::serialize(&self.inner, serializer)
    }
}

impl<'de, const N: usize> Deserialize<'de> for TypedId<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self {
            inner: <u32 as Deserialize>::deserialize(deserializer)?,
        })
    }
}

pub type TableId = TypedId<1>;
pub type JobId = TypedId<2>;
pub type DatabaseId = TypedId<3>;
pub type SchemaId = TypedId<4>;

impl JobId {
    pub fn is_mv_table_id(self, table_id: TableId) -> bool {
        self.inner == table_id.inner
    }

    pub fn as_mv_table_id(self) -> TableId {
        TableId::new(self.inner)
    }
}

impl TableId {
    pub fn as_job_id(self) -> JobId {
        JobId::new(self.inner)
    }
}

macro_rules! impl_into_object {
    ($mod_prefix:ty, $($type_name:ident),+) => {
        $(
            impl From<$type_name> for $mod_prefix {
                fn from(value: $type_name) -> Self {
                    <$mod_prefix>::$type_name(value.inner)
                }
            }
        )+
    };
}

impl_into_object!(
    risingwave_pb::user::grant_privilege::Object,
    DatabaseId,
    TableId,
    SchemaId
);

impl_into_object!(
    risingwave_pb::ddl_service::alter_name_request::Object,
    DatabaseId,
    TableId,
    SchemaId
);

impl_into_object!(
    risingwave_pb::ddl_service::alter_owner_request::Object,
    DatabaseId,
    TableId,
    SchemaId
);
