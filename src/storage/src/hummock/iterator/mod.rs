// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Bound, Deref, DerefMut};

use super::{HummockResult, HummockValue};

mod forward_concat;
pub use forward_concat::*;
mod backward_concat;
mod concat_inner;
pub use backward_concat::*;
pub use concat_inner::ConcatIteratorInner;
mod backward_merge;
pub use backward_merge::*;
mod backward_user;
pub use backward_user::*;
mod forward_merge;
pub use forward_merge::*;
pub mod forward_user;
mod merge_inner;
pub use forward_user::*;
pub use merge_inner::{OrderedMergeIteratorInner, UnorderedMergeIteratorInner};

use crate::hummock::iterator::HummockIteratorUnion::{First, Fourth, Second, Third};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::shared_buffer::SharedBufferIteratorType;
use crate::hummock::{BackwardSstableIterator, SstableIterator, SstableIteratorType};

#[cfg(any(test, feature = "test"))]
pub mod test_utils;

use crate::monitor::StoreLocalStatistic;

/// `HummockIterator` defines the interface of all iterators, including `SstableIterator`,
/// `MergeIterator`, `UserIterator` and `ConcatIterator`.
///
/// After creating the iterator instance,
/// - if you want to iterate from the beginning, you need to then call its `rewind` method.
/// - if you want to iterate from some specific position, you need to then call its `seek` method.
pub trait HummockIterator: Send + Sync + 'static {
    type Direction: HummockIteratorDirection;
    type NextFuture<'a>: Future<Output = HummockResult<()>> + Send + 'a
    where
        Self: 'a;
    type RewindFuture<'a>: Future<Output = HummockResult<()>> + Send + 'a
    where
        Self: 'a;
    type SeekFuture<'a>: Future<Output = HummockResult<()>> + Send + 'a
    where
        Self: 'a;
    /// Moves a valid iterator to the next key.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new data by calling `key` and `value`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next(&mut self) -> Self::NextFuture<'_>;

    /// Retrieves the current key.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    // TODO: Add lifetime
    fn key(&self) -> &[u8];

    /// Retrieves the current value, decoded as [`HummockValue`].
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid, or the value cannot be decoded into
    /// [`HummockValue`].
    // TODO: Add lifetime
    fn value(&self) -> HummockValue<&[u8]>;

    /// Indicates whether the iterator can be used.
    ///
    /// Note:
    /// - ONLY call `key`, `value`, and `next` if `is_valid` returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;

    /// Resets the position of the iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn rewind(&mut self) -> Self::RewindFuture<'_>;

    /// Resets iterator and seeks to the first position where the key >= provided key, or key <=
    /// provided key if this is a backward iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a>;

    /// take local statistic info from iterator to report metrics.
    fn collect_local_statistic(&self, _stats: &mut StoreLocalStatistic);
}

/// This is a placeholder trait used in `HummockIteratorUnion`
pub struct PhantomHummockIterator<D: HummockIteratorDirection> {
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> HummockIterator for PhantomHummockIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async { unreachable!() }
    }

    fn key(&self) -> &[u8] {
        unreachable!()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        unreachable!()
    }

    fn is_valid(&self) -> bool {
        unreachable!()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async { unreachable!() }
    }

    fn seek<'a>(&'a mut self, _key: &'a [u8]) -> Self::SeekFuture<'a> {
        async { unreachable!() }
    }

    fn collect_local_statistic(&self, _stats: &mut StoreLocalStatistic) {}
}

/// The `HummockIteratorUnion` acts like a wrapper over multiple types of `HummockIterator`, so that
/// the `MergeIterator`, which previously takes multiple different `HummockIterator`s as input
/// through `Box<dyn HummockIterator>`, can now wrap all its underlying `HummockIterator` over such
/// `HummockIteratorUnion`, and the input type of the `MergeIterator` so that the input type of
/// `HummockIterator` can be determined statically at compile time.
///
/// For example, in `ForwardUserIterator`, it accepts inputs from 4 sources for its underlying
/// `MergeIterator`. First, the shared buffer replicated batches, and second, the shared buffer
/// uncommitted data, and third, the overlapping L0 data, and last, the non-L0 non-overlapping
/// concat-able. These sources used to be passed in as `Box<dyn HummockIterator>`. Now if we want
/// the `MergeIterator` to be statically typed, the input type of `MergeIterator` will become the
/// `HummockIteratorUnion` of these 4 sources.
pub enum HummockIteratorUnion<
    D: HummockIteratorDirection,
    I1: HummockIterator<Direction = D>,
    I2: HummockIterator<Direction = D>,
    I3: HummockIterator<Direction = D> = PhantomHummockIterator<D>,
    I4: HummockIterator<Direction = D> = PhantomHummockIterator<D>,
> {
    First(I1),
    Second(I2),
    Third(I3),
    Fourth(I4),
}

impl<
        D: HummockIteratorDirection,
        I1: HummockIterator<Direction = D>,
        I2: HummockIterator<Direction = D>,
        I3: HummockIterator<Direction = D>,
        I4: HummockIterator<Direction = D>,
    > HummockIterator for HummockIteratorUnion<D, I1, I2, I3, I4>
{
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            match self {
                First(iter) => iter.next().await,
                Second(iter) => iter.next().await,
                Third(iter) => iter.next().await,
                Fourth(iter) => iter.next().await,
            }
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            First(iter) => iter.key(),
            Second(iter) => iter.key(),
            Third(iter) => iter.key(),
            Fourth(iter) => iter.key(),
        }
    }

    fn value(&self) -> HummockValue<&[u8]> {
        match self {
            First(iter) => iter.value(),
            Second(iter) => iter.value(),
            Third(iter) => iter.value(),
            Fourth(iter) => iter.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            First(iter) => iter.is_valid(),
            Second(iter) => iter.is_valid(),
            Third(iter) => iter.is_valid(),
            Fourth(iter) => iter.is_valid(),
        }
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            match self {
                First(iter) => iter.rewind().await,
                Second(iter) => iter.rewind().await,
                Third(iter) => iter.rewind().await,
                Fourth(iter) => iter.rewind().await,
            }
        }
    }

    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async move {
            match self {
                First(iter) => iter.seek(key).await,
                Second(iter) => iter.seek(key).await,
                Third(iter) => iter.seek(key).await,
                Fourth(iter) => iter.seek(key).await,
            }
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        match self {
            First(iter) => iter.collect_local_statistic(stats),
            Second(iter) => iter.collect_local_statistic(stats),
            Third(iter) => iter.collect_local_statistic(stats),
            Fourth(iter) => iter.collect_local_statistic(stats),
        }
    }
}

impl<I: HummockIterator> HummockIterator for Box<I> {
    type Direction = I::Direction;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        (*self).deref_mut().next()
    }

    fn key(&self) -> &[u8] {
        (*self).deref().key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        (*self).deref().value()
    }

    fn is_valid(&self) -> bool {
        (*self).deref().is_valid()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        (*self).deref_mut().rewind()
    }

    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        (*self).deref_mut().seek(key)
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        (*self).deref().collect_local_statistic(stats);
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum DirectionEnum {
    Forward,
    Backward,
}

pub trait HummockIteratorDirection: Sync + Send + 'static {
    fn direction() -> DirectionEnum;
}

pub struct Forward;
impl HummockIteratorDirection for Forward {
    #[inline(always)]
    fn direction() -> DirectionEnum {
        DirectionEnum::Forward
    }
}

pub struct Backward;
impl HummockIteratorDirection for Backward {
    #[inline(always)]
    fn direction() -> DirectionEnum {
        DirectionEnum::Backward
    }
}

pub type MultiSstIterator =
    UnorderedMergeIteratorInner<HummockIteratorUnion<Forward, ConcatIterator, SstableIterator>>;

#[allow(type_alias_bounds)]
pub type UserIteratorPayloadType<
    D: HummockIteratorDirection,
    I: SstableIteratorType<Direction = D>,
> = HummockIteratorUnion<
    D,
    SharedBufferBatchIterator<D>,
    SharedBufferIteratorType<D, I>,
    ConcatIteratorInner<I>,
    I,
>;

pub type ForwardUserIteratorType =
    UnorderedMergeIteratorInner<UserIteratorPayloadType<Forward, SstableIterator>>;
pub type BackwardUserIteratorType =
    UnorderedMergeIteratorInner<UserIteratorPayloadType<Backward, BackwardSstableIterator>>;

pub enum DirectedUserIterator {
    Forward(UserIterator<ForwardUserIteratorType>),
    Backward(BackwardUserIterator<BackwardUserIteratorType>),
}

pub trait DirectedUserIteratorBuilder {
    type Direction: HummockIteratorDirection;
    type SstableIteratorType: SstableIteratorType<Direction = Self::Direction>;
    /// Initialize an `DirectedUserIterator`.
    /// The `key_range` should be from smaller key to larger key.
    fn create(
        iterator_iter: impl IntoIterator<
            Item = UserIteratorPayloadType<Self::Direction, Self::SstableIteratorType>,
        >,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_epoch: u64,
        min_epoch: u64,
        version: Option<PinnedVersion>,
    ) -> DirectedUserIterator;
}

impl DirectedUserIterator {
    #[inline(always)]
    pub async fn next(&mut self) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.next().await,
            Self::Backward(ref mut iter) => iter.next().await,
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Forward(iter) => iter.key(),
            Self::Backward(iter) => iter.key(),
        }
    }

    #[inline(always)]
    pub fn value(&self) -> &[u8] {
        match self {
            Self::Forward(iter) => iter.value(),
            Self::Backward(iter) => iter.value(),
        }
    }

    #[inline(always)]
    pub async fn rewind(&mut self) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.rewind().await,
            Self::Backward(ref mut iter) => iter.rewind().await,
        }
    }

    #[inline(always)]
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.seek(user_key).await,
            Self::Backward(ref mut iter) => iter.seek(user_key).await,
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Forward(iter) => iter.is_valid(),
            Self::Backward(iter) => iter.is_valid(),
        }
    }

    pub fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        match self {
            DirectedUserIterator::Forward(iter) => iter.collect_local_statistic(stats),
            DirectedUserIterator::Backward(iter) => iter.collect_local_statistic(stats),
        }
    }
}
