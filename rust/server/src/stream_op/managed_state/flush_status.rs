use std::collections::btree_map;

/// Represents an entry in the `flush_buffer`. No `FlushStatus` associated with a key means no-op.
///
/// ```plain
/// No-op --(insert)-> Insert --(delete)-> No-op
///  \------(delete)-> Delete --(insert)-> DeleteInsert --(delete)-> Delete
/// ```
pub(crate) enum FlushStatus<T> {
    /// The entry will be deleted.
    Delete,
    /// The entry has been deleted in this epoch, and will be inserted.
    DeleteInsert(T),
    /// The entry will be inserted.
    Insert(T),
}

impl<T> FlushStatus<T> {
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

    pub fn as_option(&self) -> Option<&T> {
        match self {
            Self::DeleteInsert(value) | Self::Insert(value) => Some(value),
            Self::Delete => None,
        }
    }

    /// Insert an entry and modify the corresponding flush state
    pub fn do_insert<K: Ord>(entry: btree_map::Entry<K, Self>, value: T) {
        match entry {
            btree_map::Entry::Vacant(e) => {
                // No-op -> Insert
                e.insert(Self::Insert(value));
            }
            btree_map::Entry::Occupied(mut e) => {
                if e.get().is_delete() {
                    // Delete -> DeleteInsert
                    e.insert(Self::DeleteInsert(value));
                } else {
                    panic!("invalid flush status");
                }
            }
        }
    }

    /// Delete an entry and modify the corresponding flush state
    pub fn do_delete<K: Ord>(entry: btree_map::Entry<K, Self>) {
        match entry {
            btree_map::Entry::Vacant(e) => {
                // No-op -> Delete
                e.insert(Self::Delete);
            }
            btree_map::Entry::Occupied(mut e) => {
                if e.get().is_insert() {
                    // Insert -> No-op
                    e.remove();
                } else if e.get().is_delete_insert() {
                    // DeleteInsert -> Delete
                    e.insert(Self::Delete);
                } else {
                    panic!("invalid flush status");
                }
            }
        }
    }
}
