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
use std::collections::{BTreeMap, HashMap};
use std::fmt::Formatter;
use std::num::TryFromIntError;
use std::ops::{Add, AddAssign};
use std::str::FromStr;

use sea_orm::sea_query::{ArrayType, ValueTypeErr};
use sea_orm::{ColIdx, ColumnType, DbErr, QueryResult, TryGetError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::catalog::source::OptionalAssociatedTableId;
use crate::catalog::table::OptionalAssociatedSourceId;

pub const OBJECT_ID_PLACEHOLDER: u32 = u32::MAX - 1;

#[derive(Clone, Copy, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
#[repr(transparent)]
pub struct TypedId<const N: usize>(pub(crate) u32);

impl<const N: usize> std::fmt::Debug for TypedId<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <u32 as std::fmt::Debug>::fmt(&self.0, f)
    }
}

impl<const N: usize> std::fmt::Display for TypedId<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <u32 as std::fmt::Display>::fmt(&self.0, f)
    }
}

impl<const N: usize> PartialEq<u32> for TypedId<N> {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl<const N: usize> FromStr for TypedId<N> {
    type Err = <u32 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(<u32 as FromStr>::from_str(s)?))
    }
}

impl<const N: usize> TypedId<N> {
    pub const fn new(inner: u32) -> Self {
        TypedId(inner)
    }

    pub fn as_raw_id(self) -> u32 {
        self.0
    }

    pub fn as_i32_id(self) -> i32 {
        self.to_i32()
    }

    pub const fn placeholder() -> Self {
        Self(OBJECT_ID_PLACEHOLDER)
    }

    pub fn is_placeholder(&self) -> bool {
        self.0 == OBJECT_ID_PLACEHOLDER
    }

    fn from_i32(inner: i32) -> Self {
        Self(inner.try_into().unwrap_or_else(|e: TryFromIntError| {
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
        }))
    }

    fn to_i32(self) -> i32 {
        self.0.try_into().unwrap_or_else(|e: TryFromIntError| {
            if cfg!(debug_assertions) {
                panic!(
                    "invalid u32 id {} for {}: {:?}",
                    self.0,
                    type_name::<Self>(),
                    e.as_report()
                );
            } else {
                warn!(
                    "invalid u32 id {} for {}: {:?}",
                    self.0,
                    type_name::<Self>(),
                    e.as_report()
                );
                self.0 as _
            }
        })
    }

    pub fn raw_slice(slice: &[Self]) -> &[u32] {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(slice) }
    }

    pub fn mut_raw_vec(vec: &mut Vec<Self>) -> &mut Vec<u32> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(vec) }
    }

    pub fn raw_hash_map_ref<V>(map: &HashMap<Self, V>) -> &HashMap<u32, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_hash_map_mut_ref<V>(map: &mut HashMap<Self, V>) -> &mut HashMap<u32, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_btree_map_ref<V>(map: &BTreeMap<Self, V>) -> &BTreeMap<u32, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_btree_map_mut_ref<V>(map: &mut BTreeMap<Self, V>) -> &mut BTreeMap<u32, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }
}

impl<const N: usize> From<u32> for TypedId<N> {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl<const N: usize> From<TypedId<N>> for sea_orm::Value {
    fn from(value: TypedId<N>) -> Self {
        sea_orm::Value::from(value.to_i32())
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
        <u32 as Serialize>::serialize(&self.0, serializer)
    }
}

impl<'de, const N: usize> Deserialize<'de> for TypedId<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(<u32 as Deserialize>::deserialize(deserializer)?))
    }
}

impl<const N: usize> Add<u32> for TypedId<N> {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0.checked_add(rhs).unwrap())
    }
}

impl<const N: usize> AddAssign<u32> for TypedId<N> {
    fn add_assign(&mut self, rhs: u32) {
        self.0 = self.0.checked_add(rhs).unwrap()
    }
}

pub type TableId = TypedId<1>;
pub type JobId = TypedId<2>;
pub type DatabaseId = TypedId<3>;
pub type SchemaId = TypedId<4>;
pub type FragmentId = TypedId<5>;
pub type ActorId = TypedId<6>;
pub type WorkerId = TypedId<7>;
pub type SinkId = TypedId<8>;
pub type SourceId = TypedId<9>;

pub type SubscriptionId = TypedId<10>;
pub type IndexId = TypedId<11>;
pub type ViewId = TypedId<12>;
pub type FunctionId = TypedId<13>;
pub type ConnectionId = TypedId<14>;
pub type SecretId = TypedId<15>;

pub type ObjectId = TypedId<256>;

impl JobId {
    pub fn is_mv_table_id(self, table_id: TableId) -> bool {
        self.0 == table_id.0
    }

    pub fn as_mv_table_id(self) -> TableId {
        TableId::new(self.0)
    }

    pub fn as_sink_id(self) -> SinkId {
        SinkId::new(self.0)
    }

    pub fn as_shared_source_id(self) -> SourceId {
        SourceId::new(self.0)
    }

    pub fn as_index_id(self) -> IndexId {
        IndexId::new(self.0)
    }
}

impl TableId {
    pub fn as_job_id(self) -> JobId {
        JobId::new(self.0)
    }
}

impl From<OptionalAssociatedTableId> for TableId {
    fn from(value: OptionalAssociatedTableId) -> Self {
        let OptionalAssociatedTableId::AssociatedTableId(table_id) = value;
        Self(table_id)
    }
}

impl From<TableId> for OptionalAssociatedTableId {
    fn from(value: TableId) -> Self {
        OptionalAssociatedTableId::AssociatedTableId(value.0)
    }
}

impl SinkId {
    pub fn as_job_id(self) -> JobId {
        JobId::new(self.0)
    }
}

impl IndexId {
    pub fn as_job_id(self) -> JobId {
        JobId::new(self.0)
    }
}

impl SourceId {
    pub fn as_share_source_job_id(self) -> JobId {
        JobId::new(self.0)
    }

    pub fn as_cdc_table_id(self) -> TableId {
        TableId::new(self.0)
    }
}

impl From<OptionalAssociatedSourceId> for SourceId {
    fn from(value: OptionalAssociatedSourceId) -> Self {
        let OptionalAssociatedSourceId::AssociatedSourceId(source_id) = value;
        Self(source_id)
    }
}

impl From<SourceId> for OptionalAssociatedSourceId {
    fn from(value: SourceId) -> Self {
        OptionalAssociatedSourceId::AssociatedSourceId(value.0)
    }
}

macro_rules! impl_into_object {
    ($mod_prefix:ty, $($type_name:ident),+) => {
        $(
            impl From<$type_name> for $mod_prefix {
                fn from(value: $type_name) -> Self {
                    <$mod_prefix>::$type_name(value.0)
                }
            }
        )+
    };
}

impl_into_object!(
    crate::user::grant_privilege::Object,
    DatabaseId,
    TableId,
    SchemaId,
    SinkId,
    SourceId,
    SubscriptionId,
    ViewId,
    FunctionId,
    ConnectionId,
    SecretId
);

impl_into_object!(
    crate::ddl_service::alter_name_request::Object,
    DatabaseId,
    TableId,
    SchemaId,
    SinkId,
    SourceId,
    SubscriptionId,
    IndexId,
    ViewId
);

impl_into_object!(
    crate::ddl_service::alter_owner_request::Object,
    DatabaseId,
    TableId,
    SchemaId,
    SinkId,
    SourceId,
    SubscriptionId,
    ViewId,
    ConnectionId
);

impl_into_object!(
    crate::ddl_service::alter_set_schema_request::Object,
    TableId,
    ViewId,
    SourceId,
    SinkId,
    SubscriptionId,
    FunctionId,
    ConnectionId
);

macro_rules! impl_into_rename_object {
    ($($type_name:ident),+) => {
        paste::paste! {
            $(
                impl From<([<$type_name Id>], [<$type_name Id>])> for crate::ddl_service::alter_swap_rename_request::Object {
                    fn from((src_object_id, dst_object_id): ([<$type_name Id>], [<$type_name Id>])) -> Self {
                        crate::ddl_service::alter_swap_rename_request::Object::$type_name(crate::ddl_service::alter_swap_rename_request::ObjectNameSwapPair {
                            src_object_id: src_object_id.as_object_id(),
                            dst_object_id: dst_object_id.as_object_id(),
                        })
                    }
                }
            )+
        }
    };
}

impl_into_rename_object!(Table, View, Source, Sink, Subscription);

macro_rules! impl_object_id_conversion {
    ($($type_name:ident),+) => {
        $(
            impl From<$type_name> for ObjectId {
                fn from(value: $type_name) -> Self {
                    Self::new(value.0)
                }
            }

            impl $type_name {
                pub fn as_object_id(self) -> ObjectId {
                    ObjectId::new(self.0)
                }
            }
        )+

        paste::paste! {
            impl ObjectId {
                $(
                    pub fn [< as_ $type_name:snake>](self) -> $type_name {
                        $type_name::new(self.0)
                    }
                )+
            }
        }
    };
}

impl_object_id_conversion!(
    DatabaseId,
    TableId,
    SchemaId,
    SinkId,
    SourceId,
    JobId,
    SubscriptionId,
    IndexId,
    ViewId,
    FunctionId,
    ConnectionId,
    SecretId
);
