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

use madsim::collections::btree_map;

macro_rules! impl_flush_status {
    ([], $( { $entry_type:ty, $struct_name:ident } ),*) => {
        /// Represents an entry in the `flush_buffer`. No `FlushStatus` associated with a key means no-op.
        ///
        /// ```plain
        /// No-op --(insert)-> Insert --(delete)-> No-op
        ///        \------(delete)-> Delete --(insert)-> DeleteInsert --(delete)-> Delete
        /// ```
        $(
            #[derive(Debug)]
            pub enum $struct_name<T> {
                /// The entry will be deleted.
                Delete,
                /// The entry has been deleted in this epoch, and will be inserted.
                DeleteInsert(T),
                /// The entry will be inserted.
                Insert(T),
            }

            impl<T: std::fmt::Debug> $struct_name<T> {
                pub fn is_delete(&self) -> bool {
                    matches!(self, Self::Delete)
                }

                pub fn is_insert(&self) -> bool {
                    matches!(self, Self::Insert(_))
                }

                pub fn is_delete_insert(&self) -> bool {
                    matches!(self, Self::DeleteInsert(_))
                }

                /// Transform `FlushStatus` into an `Option`. If the last operation in the `FlushStatus` is
                /// `Delete`, return `None`. Otherwise, return the concrete value.
                pub fn into_option(self) -> Option<T> {
                    match self {
                        Self::DeleteInsert(value) | Self::Insert(value) => Some(value),
                        Self::Delete => None,
                    }
                }

                #[allow(dead_code)]
                pub fn as_option(&self) -> Option<&T> {
                    match self {
                        Self::DeleteInsert(value) | Self::Insert(value) => Some(value),
                        Self::Delete => None,
                    }
                }

                /// Insert an entry and modify the corresponding flush state
                pub fn do_insert<K: Ord + std::fmt::Debug>(entry: $entry_type, value: T) {
                    match entry {
                        <$entry_type>::Vacant(e) => {
                            // No-op -> Insert
                            e.insert(Self::Insert(value));
                        }
                        <$entry_type>::Occupied(mut e) => {
                            if e.get().is_delete() {
                                // Delete -> DeleteInsert
                                e.insert(Self::DeleteInsert(value));
                            } else {
                                panic!("invalid flush status: double insert {:?} -> {:?}", e.key(), value);
                            }
                        }
                    }
                }

                    /// Delete an entry and modify the corresponding flush state
                pub fn do_delete<K: Ord + std::fmt::Debug>(entry: $entry_type) {
                    match entry {
                        <$entry_type>::Vacant(e) => {
                            // No-op -> Delete
                            e.insert(Self::Delete);
                        }
                        <$entry_type>::Occupied(mut e) => {
                            if e.get().is_insert() {
                                // Insert -> No-op
                                e.remove();
                            } else if e.get().is_delete_insert() {
                                // DeleteInsert -> Delete
                                e.insert(Self::Delete);
                            } else {
                                panic!("invalid flush status: double delete {:?}", e.key());
                            }
                        }
                    }
                }
            }
        )*
    };
}

macro_rules! for_all_entry {
        ($macro:ident $(, $x:tt)*) => {
                $macro! {
                        [$($x),*],
                        { btree_map::Entry<K, Self>, BtreeMapFlushStatus }
                        // { hash_map::Entry<K, Self>, HashMapFlushStatus }
                }
        };
}

for_all_entry! { impl_flush_status }
