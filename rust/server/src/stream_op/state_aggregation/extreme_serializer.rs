use std::marker::PhantomData;

use risingwave_common::error::Result;
use risingwave_common::types::{Scalar, ScalarImpl};
use serde::Serialize;
use smallvec::SmallVec;

pub type ExtremePK = SmallVec<[i64; 1]>;

pub mod variants {
    pub const EXTREME_MIN: usize = 0;
    pub const EXTREME_MAX: usize = 1;
}

/// A serializer built specifically for `ManagedExtremeState`
///
/// The serializer will encode original key and pks one by one. If `EXTREME_TYPE == EXTREME_MAX`,
/// we will flip the bits of the whole encoded data (including pks).
pub struct ExtremeSerializer<K: Scalar, const EXTREME_TYPE: usize> {
    pk_length: usize,
    _phantom: PhantomData<K>,
}

impl<K: Scalar, const EXTREME_TYPE: usize> ExtremeSerializer<K, EXTREME_TYPE> {
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
        let mut serializer = memcomparable::Serializer::new(vec![]);
        let key: ScalarImpl = key.into();
        key.serialize(&mut serializer)?;
        debug_assert_eq!(row_ids.len(), self.pk_length, "mismatch pk length");
        for i in row_ids {
            (*i).serialize(&mut serializer)?;
        }
        let mut encoded_key = serializer.into_inner();
        match EXTREME_TYPE {
            variants::EXTREME_MIN => {}
            variants::EXTREME_MAX => {
                encoded_key.iter_mut().for_each(|byte| {
                    *byte = !(*byte);
                });
            }
            _ => unimplemented!(),
        }
        Ok(encoded_key)
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

// TODO: add unit tests
