use std::collections::{btree_map, hash_map};

macro_rules! impl_flush_status {
 ([], $( { $entry_type:ty, $struct_name:ident } ),*) => {
   /// Represents an entry in the `flush_buffer`. No `FlushStatus` associated with a key means no-op.
   ///
   /// ```plain
   /// No-op --(insert)-> Insert --(delete)-> No-op
   ///  \------(delete)-> Delete --(insert)-> DeleteInsert --(delete)-> Delete
   /// ```
  $(pub enum $struct_name<T> {
    /// The entry will be deleted.
    Delete,
    /// The entry has been deleted in this epoch, and will be inserted.
    DeleteInsert(T),
    /// The entry will be inserted.
    Insert(T),
  }

  impl<T> $struct_name<T> {
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
    pub fn do_insert<K: Ord>(entry: $entry_type, value: T) {
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
            panic!("invalid flush status");
          }
        }
      }
    }

    /// Delete an entry and modify the corresponding flush state
    pub fn do_delete<K: Ord>(entry: $entry_type) {
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
            panic!("invalid flush status");
          }
        }
      }
    }
  }
  )*
 };
}

macro_rules! for_all_entry {
  ($macro:tt $(, $x:tt)*) => {
    $macro! {
      [$($x),*],
      { btree_map::Entry<K, Self>, BtreeMapFlushStatus },
      { hash_map::Entry<K, Self>, HashMapFlushStatus }
    }
  };
}

for_all_entry! { impl_flush_status }
