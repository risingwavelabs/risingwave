# prost-helpers

This crate provides a derive macro `prost_helpers::AnyPB` for every prost generated message. It automatically generates
getter methods:

```rust
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
struct FooMessage {
  #[prost(message, optional, tag = "1")]
  pub field: ::core::option::Option<Field>,
  #[prost(enumeration = "foo_message::EnumFieldType", tag = "1")]
  pub enum_field: i32,
}

impl FooMessage {
  pub fn get_field(&self) -> Field {
    self.field.unwrap()
  }

  // // Later we will return Result instead of unwrap.
  // pub fn get_field(&self) -> Result<Field> {
  //  self.field.ok_or_else(InvalidArgument("field is missing in FooMessage"))
  // }

  pub fn get_enum_field(&self) -> foo_message::EnumFieldType {
    self.enum_field
  }
}
```

A distinction of prost is that it represents every optional message field using `Option`, which will lead to many
Some/None matching boilerplate code. This problem is planned to be addressed by returning `Result` specifically for the
getter of `Option` types. Therefore, the presence of fields will be validated before actually used.
