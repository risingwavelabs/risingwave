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

use risingwave_hummock_sdk::HummockEpoch;

/// `DeleteRangeIterator` defines the interface of all delete-range iterators, which is used to
/// filter keys deleted by some range tombstone
///
/// After creating the iterator instance,
/// - if you want to iterate from the beginning, you need to then call its `rewind` method.
/// - if you want to iterate from some specific position, you need to then call its `seek` method.
pub trait DeleteRangeIterator {
    /// Retrieves the left-endpoint of the current range-tombstone. Our range-tombstones are all
    /// defined by [`start_user_key`, `end_user_key`), which means that `start_user_key` is
    /// inclusive while `end_user_key` is exclusive.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn start_user_key(&self) -> &[u8];

    /// Retrieves the right-endpoint of the current range-tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn end_user_key(&self) -> &[u8];

    /// Retrieves the epoch of the current range-tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn current_epoch(&self) -> HummockEpoch;

    /// Moves a valid iterator to the next tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new tombstone by calling `start_user_key`, `end_user_key` and
    ///   `current_epoch`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next(&mut self);

    /// Resets the position of the iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn rewind(&mut self);

    /// Resets iterator and seeks to the first tombstone whose left-end  >= provided key, or
    /// right-end <= provided key if this is a backward iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn seek(&mut self, _target_user_key: &[u8]) {
        unimplemented!("Support seek operation");
    }

    /// Indicates whether the iterator can be used.
    ///
    /// Note:
    /// - ONLY call `start_user_key`, `end_user_key`, `current_epoch` and `next` if `is_valid`
    ///   returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;
}
