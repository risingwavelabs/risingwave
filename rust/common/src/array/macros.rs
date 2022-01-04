/// `array` builds an `Array` with `Option`.
#[macro_export]
macro_rules! array {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use $crate::array::Array;
      use $crate::array::ArrayBuilder;
      let mut builder = <$array as Array>::Builder::new(0).unwrap();
      for value in [$($value),*] {
        let value: Option<<$array as Array>::RefItem<'_>> = value.map(Into::into);
        builder.append(value).unwrap();
      }
      builder.finish().unwrap()
    }
  };
}

/// `array_nonnull` builds an `Array` with concrete values.
#[macro_export]
macro_rules! array_nonnull {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use $crate::array::Array;
      use $crate::array::ArrayBuilder;
      let mut builder = <$array as Array>::Builder::new(0).unwrap();
      for value in [$($value),*] {
        let value: <$array as Array>::RefItem<'_> = value.into();
        builder.append(Some(value)).unwrap();
      }
      builder.finish().unwrap()
    }
  };
}

/// `column` builds a `Column` with `Option`.
#[macro_export]
macro_rules! column {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use crate::array::column::Column;
      let arr = array! { $array, [ $( $value ),* ] };
      Column::new(std::sync::Arc::new(arr.into()))
    }
  };
}

/// `column_nonnull` builds a `Column` with concrete values.
#[macro_export]
macro_rules! column_nonnull {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use $crate::array::column::Column;
      let arr = $crate::array_nonnull! { $array, [ $( $value ),* ] };
      Column::new(std::sync::Arc::new(arr.into()))
    }
  };
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, I16Array};

    #[test]
    fn test_build_array() {
        let a = array! { I16Array, [Some(1i16), None, Some(3)] };
        assert_eq!(a.len(), 3);
        let a = array_nonnull! { I16Array, [1i16, 2, 3] };
        assert_eq!(a.len(), 3);
    }

    #[test]
    fn test_build_column() {
        let c = column! { I16Array, [Some(1i16), None, Some(3)] };
        assert_eq!(c.array_ref().len(), 3);
        let c = column_nonnull! { I16Array, [1i16, 2, 3] };
        assert_eq!(c.array_ref().len(), 3);
    }
}
