pub use paste::paste;

#[macro_export]
macro_rules! make_proto {
  ($proto_type:ty, { $( $key:ident : $value:expr ),* }) => {
    {
      use pb_construct::paste;
      let mut msg_proto = <$proto_type>::new();
      paste! {
        $(
          msg_proto.[< set_$key >]($value);
        )*
      }
      msg_proto
    }
  };
}
