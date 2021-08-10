use crate::types::DataType;
use crate::types::DataTypeKind;
use crate::types::PrimitiveDataType;

macro_rules! make_numeric_type {
  ($name:ident, $native_ty:ty, $data_ty:expr) => {
    #[derive(Debug)]
    pub struct $name {
      nullable: bool,
    }

    impl $name {
      pub fn new(nullable: bool) -> Self {
        Self { nullable }
      }
    }

    impl DataType for $name {
      fn data_type_kind(&self) -> DataTypeKind {
        $data_ty
      }

      fn is_nullable(&self) -> bool {
        self.nullable
      }
    }

    impl PrimitiveDataType for $name {
      type N = $native_ty;
    }
  };
}

make_numeric_type!(Int16Type, i16, DataTypeKind::Int16);
make_numeric_type!(Int32Type, i32, DataTypeKind::Int32);
make_numeric_type!(Int64Type, i64, DataTypeKind::Int64);
make_numeric_type!(Float32Type, f32, DataTypeKind::Float32);
make_numeric_type!(Float64Type, f64, DataTypeKind::Float64);
