use std::marker::PhantomData;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{Scalar, ScalarImpl};
use serde::Serialize;
use smallvec::SmallVec;

pub type ExtremePK = SmallVec<[i64; 1]>;

/// A serializer built specifically for `ManagedExtremeState`
pub struct ExtremeSerializer<K: Scalar> {
    pk_length: usize,
    _phantom: PhantomData<K>,
}

impl<K: Scalar> ExtremeSerializer<K> {
    pub fn new(pk_length: usize) -> Self {
        Self {
            pk_length,
            _phantom: PhantomData,
        }
    }

    /// Serialize key and `row_ids` into a sort key
    ///
    /// TODO: support `&K` instead of `K` as parameter.
    pub fn serialize(&self, key: K, row_ids: &ExtremePK) -> Result<Vec<u8>> {
        let mut serializer = memcomparable::Serializer::default();
        let key: ScalarImpl = key.into();
        key.serialize(&mut serializer)
            .map_err(ErrorCode::MemComparableError)?;
        debug_assert_eq!(row_ids.len(), self.pk_length, "mismatch pk length");
        for i in row_ids {
            (*i).serialize(&mut serializer)
                .map_err(ErrorCode::MemComparableError)?;
        }
        Ok(serializer.into_inner())
    }

    /// Extract the pks from the sort key
    pub fn get_pk(&self, _data: &[u8]) -> Result<ExtremePK> {
        if self.pk_length == 0 {
            Ok(ExtremePK::default())
        } else {
            todo!("get pk from sort key is not supported yet")
        }
    }
}
