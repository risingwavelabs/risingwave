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
pub struct TypedId<const N: usize, P>(pub(crate) P);

impl<const N: usize, P: std::fmt::Debug> std::fmt::Debug for TypedId<N, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <P as std::fmt::Debug>::fmt(&self.0, f)
    }
}

impl<const N: usize, P: std::fmt::Display> std::fmt::Display for TypedId<N, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <P as std::fmt::Display>::fmt(&self.0, f)
    }
}

impl<const N: usize, P: std::fmt::UpperHex> std::fmt::UpperHex for TypedId<N, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <P as std::fmt::UpperHex>::fmt(&self.0, f)
    }
}

impl<const N: usize, P: PartialEq> PartialEq<P> for TypedId<N, P> {
    fn eq(&self, other: &P) -> bool {
        self.0 == *other
    }
}

impl<const N: usize, P: FromStr> FromStr for TypedId<N, P> {
    type Err = <P as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(<P as FromStr>::from_str(s)?))
    }
}

impl<const N: usize, P> TypedId<N, P>
where
    Self: UniqueTypedIdDeclaration,
{
    pub const fn new(inner: P) -> Self {
        TypedId(inner)
    }

    #[expect(clippy::wrong_self_convention)]
    pub fn as_raw_id(self) -> P {
        self.0
    }

    pub fn raw_slice(slice: &[Self]) -> &[P] {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(slice) }
    }

    pub fn mut_raw_vec(vec: &mut Vec<Self>) -> &mut Vec<P> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(vec) }
    }

    pub fn raw_hash_map_ref<V>(map: &HashMap<Self, V>) -> &HashMap<P, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_hash_map_mut_ref<V>(map: &mut HashMap<Self, V>) -> &mut HashMap<P, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_btree_map_ref<V>(map: &BTreeMap<Self, V>) -> &BTreeMap<P, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }

    pub fn raw_btree_map_mut_ref<V>(map: &mut BTreeMap<Self, V>) -> &mut BTreeMap<P, V> {
        // SAFETY: transparent repr
        unsafe { std::mem::transmute(map) }
    }
}

type TypedU32Id<const N: usize> = TypedId<N, u32>;

impl<const N: usize> TypedU32Id<N>
where
    Self: UniqueTypedIdDeclaration,
{
    pub const fn placeholder() -> Self {
        Self(OBJECT_ID_PLACEHOLDER)
    }

    pub fn is_placeholder(&self) -> bool {
        self.0 == OBJECT_ID_PLACEHOLDER
    }

    pub fn as_i32_id(self) -> i32 {
        self.to_i32()
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
}

impl<const N: usize, P> From<P> for TypedId<N, P> {
    fn from(id: P) -> Self {
        Self(id)
    }
}

impl<const N: usize> From<TypedU32Id<N>> for sea_orm::Value
where
    TypedU32Id<N>: UniqueTypedIdDeclaration,
{
    fn from(value: TypedU32Id<N>) -> Self {
        sea_orm::Value::from(value.to_i32())
    }
}

impl<const N: usize> sea_orm::sea_query::ValueType for TypedU32Id<N>
where
    Self: UniqueTypedIdDeclaration,
{
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

impl<const N: usize> sea_orm::sea_query::Nullable for TypedU32Id<N> {
    fn null() -> sea_orm::Value {
        <i32 as sea_orm::sea_query::Nullable>::null()
    }
}

impl<const N: usize> sea_orm::TryGetable for TypedU32Id<N>
where
    Self: UniqueTypedIdDeclaration,
{
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let inner = <i32 as sea_orm::TryGetable>::try_get_by(res, index)?;
        Ok(Self::from_i32(inner))
    }
}

impl<const N: usize> sea_orm::TryFromU64 for TypedU32Id<N>
where
    Self: UniqueTypedIdDeclaration,
{
    fn try_from_u64(n: u64) -> Result<Self, DbErr> {
        let inner = <i32 as sea_orm::TryFromU64>::try_from_u64(n)?;
        Ok(Self::from_i32(inner))
    }
}

impl<const N: usize, P: Serialize> Serialize for TypedId<N, P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        <P as Serialize>::serialize(&self.0, serializer)
    }
}

impl<'de, const N: usize, P: Deserialize<'de>> Deserialize<'de> for TypedId<N, P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(<P as Deserialize>::deserialize(deserializer)?))
    }
}

impl<const N: usize> Add<u32> for TypedU32Id<N> {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0.checked_add(rhs).unwrap())
    }
}

impl<const N: usize> AddAssign<u32> for TypedU32Id<N> {
    fn add_assign(&mut self, rhs: u32) {
        self.0 = self.0.checked_add(rhs).unwrap()
    }
}

#[expect(dead_code)]
pub trait UniqueTypedIdDeclaration {}

macro_rules! declare_id_type {
    ($name:ident, $primitive:ty, $type_id:expr) => {
        pub type $name = TypedId<{ $type_id }, $primitive>;
        impl UniqueTypedIdDeclaration for $name {}
    };
}

macro_rules! declare_id_types {
    ($primitive:ty $(, $name:ident)+) => {
        declare_id_types! {
            $primitive, 0 $(, $name)+
        }
    };
    ($primitive:ty, $next_type_id:expr) => {};
    ($primitive:ty, $next_type_id:expr, $name:ident $(, $rest:ident)*) => {
        declare_id_type! { $name, $primitive, $next_type_id }
        declare_id_types! {
            $primitive, $next_type_id + 1  $(, $rest)*
        }
    };
    ($($invalid:tt)+) => {
        compile_error!(stringify!($($invalid)+))
    }
}

declare_id_types!(
    u32,
    TableId,
    JobId,
    DatabaseId,
    SchemaId,
    FragmentId,
    ActorId,
    WorkerId,
    SinkId,
    SourceId,
    SubscriptionId,
    IndexId,
    ViewId,
    FunctionId,
    ConnectionId,
    SecretId,
    SubscriberId,
    LocalOperatorId
);

declare_id_type!(ObjectId, u32, 256);

declare_id_types!(
    u64,
    GlobalOperatorId,
    StreamNodeLocalOperatorId,
    ExecutorId,
    PartialGraphId
);

macro_rules! impl_as {
    (@func $target_id_name:ident, $alias_name:ident) => {
        paste::paste! {
            pub fn [< as_ $alias_name >](self) -> $target_id_name {
                $target_id_name::new(self.0)
            }
        }
    };
    (@func $target_id_name:ident) => {
        paste::paste! {
            impl_as! { @func $target_id_name, [< $target_id_name:snake >] }
        }
    };
    ($src_id_name:ident $(,$target_id_name:ident)* $(,{$orig_target_id_name:ident , $alias_name:ident})*) => {
        impl $src_id_name {
            $(
                impl_as! { @func $target_id_name }
            )*
            $(
                impl_as! { @func $orig_target_id_name, $alias_name }
            )*
        }
    }
}

impl JobId {
    pub fn is_mv_table_id(self, table_id: TableId) -> bool {
        self.0 == table_id.0
    }
}

impl_as!(JobId, SinkId, IndexId, SubscriberId, {TableId, mv_table_id}, {SourceId, shared_source_id});
impl_as!(TableId, JobId);

impl From<StreamNodeLocalOperatorId> for LocalOperatorId {
    fn from(value: StreamNodeLocalOperatorId) -> Self {
        assert!(
            value.0 <= u32::MAX as u64,
            "oversized operator id {} in stream node",
            value.0
        );
        Self(value.0 as u32)
    }
}

impl From<LocalOperatorId> for StreamNodeLocalOperatorId {
    fn from(value: LocalOperatorId) -> Self {
        Self(value.0 as u64)
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

impl_as!(SinkId, JobId);
impl_as!(IndexId, JobId);
impl_as!(SourceId, {JobId, share_source_job_id}, {TableId, cdc_table_id});
impl_as!(SubscriptionId, SubscriberId);

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
    ConnectionId,
    FunctionId,
    SecretId
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
