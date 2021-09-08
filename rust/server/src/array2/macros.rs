/// `array` builds an `Array` with `Option`.
#[macro_export]
macro_rules! array {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use crate::array2::Array;
      use crate::array2::ArrayBuilder;
      let mut builder = <$array as Array>::Builder::new(0).unwrap();
      $(
        builder.append($value).unwrap();
      )*
      builder.finish().unwrap()
    }
  };
}

/// `array_nonnull` builds an `Array` with concrete types.
#[macro_export]
macro_rules! array_nonnull {
  ($array:tt, [$( $value:expr ),*]) => {
    {
      use crate::array2::Array;
      use crate::array2::ArrayBuilder;
      let mut builder = <$array as Array>::Builder::new(0).unwrap();
      $(
        builder.append(Some($value)).unwrap();
      )*
      builder.finish().unwrap()
    }
  };
}

#[cfg(test)]
mod tests {
    use crate::array2::Array;
    use crate::array2::I16Array;

    #[test]
    fn test_build_array() {
        let a = array! { I16Array, [Some(1), Some(2), Some(3)] };
        assert_eq!(a.len(), 3);
        let a = array_nonnull! { I16Array, [1, 2, 3] };
        assert_eq!(a.len(), 3);
    }
}
