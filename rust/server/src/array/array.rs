use std::any::Any;
use std::sync::Arc;

use protobuf::well_known_types::Any as AnyProto;

use crate::array::array_data::ArrayData;
use crate::error::Result;
use crate::types::DataType;

pub(crate) trait Array: Send + Sync + AsRef<dyn Any> + AsMut<dyn Any> {
    fn data_type(&self) -> &dyn DataType;
    fn array_data(&self) -> &ArrayData;
    fn len(&self) -> usize {
        self.array_data().cardinality()
    }

    fn to_protobuf(&self) -> Result<AnyProto>;

    fn is_null(&self, idx: usize) -> Result<bool> {
        self.array_data().is_null(idx)
    }

    unsafe fn is_null_unchecked(&self, idx: usize) -> bool {
        self.array_data().is_null_unchecked(idx)
    }

    #[inline]
    fn check_idx(&self, idx: usize) -> Result<()> {
        self.array_data().check_idx(idx)
    }
}

pub(crate) type ArrayRef = Arc<dyn Array>;
