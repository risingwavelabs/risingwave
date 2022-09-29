# prost-helpers

This crate provides a derive macro `prost_helpers::AnyPB` for every prost generated message. It automatically generates
getter methods:
- For optional fields, the getter methods return `Result` and thus can be used to simplify `Option` handling boilerplate code.
- For enum fields, the getter methods can help cast `i32` to the enum type. It also checks the zero value of the enum, which actually means the enum field is not present.
- For other kinds of fields, the getter method is the same as field access.

An example of the generated getter methods:

```rust
#[derive(prost_helpers::AnyPB)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FooMessage {
    #[prost(message, optional, tag="1")]
    pub field: ::core::option::Option<Field>,
    #[prost(enumeration="foo_message::EnumFieldType", tag="2")]
    pub enum_field: i32,
    #[prost(uint32, tag="3")]
    pub uint32_field: u32,
    #[prost(uint32, optional, tag="4")]
    pub optional_uint32_field: ::core::option::Option<u32>,
    #[prost(message, repeated, tag="5")]
    pub repeated_field: ::prost::alloc::vec::Vec<Field>,
}

impl FooMessage {
    pub fn get_field(&self) -> Result<&Field> {
        self.field
            .as_ref()
            .ok_or_else(|| crate::ProstFieldNotFound(stringify!(field)))
    }

    pub fn get_enum_field(&self) -> Result<foo_message::EnumFieldType> {
        if self.enum_field.eq(&0) {
            return Err(crate::ProstFieldNotFound(stringify!(enum_field)));
        }
        foo_message::EnumFieldType::from_i32(self.enum_field)
            .ok_or_else(|| crate::ProstFieldNotFound(stringify!(enum_field)))
    }

    pub fn get_uint32_field(&self) -> u32 {
        self.uint32_field
    }

    pub fn get_optional_uint32_field(&self) -> Result<&u32> {
        self.optional_uint32_field
            .as_ref()
            .ok_or_else(|| crate::ProstFieldNotFound(stringify!(optional_uint32_field)))
    }

    pub fn get_repeated_field(&self) -> &Vec<Field> {
        &self.repeated_field
    }
}
```
