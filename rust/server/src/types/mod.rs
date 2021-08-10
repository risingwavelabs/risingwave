use std::sync::Arc;

mod numeric;
mod primitive;
pub(crate) use primitive::*;
mod native;
pub(crate) use native::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum DataTypeKind {
  Boolean,
  Int16,
  Int32,
  Int64,
  Float32,
  Float64,
  Decimal,
}

pub(crate) trait DataType {
  fn data_type_kind(&self) -> DataTypeKind;
  fn is_nullable(&self) -> bool;
}

pub(crate) type DataTypeRef = Arc<dyn DataType>;
