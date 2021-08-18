use crate::array::array_data::ArrayData;
use crate::error::Result;
use crate::types::DataType;
use protobuf::well_known_types::Any as AnyProto;
use std::any::Any;
use std::sync::Arc;

pub(crate) trait Array: Send + Sync {
    fn data_type(&self) -> &dyn DataType;
    fn array_data(&self) -> &ArrayData;
    fn len(&self) -> usize {
        self.array_data().cardinality()
    }

    fn to_protobuf(&self) -> Result<AnyProto>;

    fn as_any(&self) -> &dyn Any;
}

pub(crate) type ArrayRef = Arc<dyn Array>;
