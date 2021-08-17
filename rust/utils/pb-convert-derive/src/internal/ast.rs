use crate::internal::{ContainerAttrs, FieldAttrs, VariantAttrs};
use anyhow::{anyhow, bail};
use proc_macro2::TokenStream as TokenStream2;
use quote::format_ident;
use quote::quote;
use syn::parse_quote;

pub(crate) type TokenStream2s = Vec<TokenStream2>;

/// A source data structure annotated with `#[derive(FromProtobuf)]` and/or `#[derive(ToProtobuf)]`,
/// parsed into an internal data structure.
pub(crate) struct Container {
    /// The struct or enum name (without generics).
    pub id: syn::Ident,
    /// Attributes on the structure, parsed for Serde.
    pub attrs: ContainerAttrs,
    /// The contents of the struct or enum.
    pub data: ContainerData,
}

pub(crate) enum ContainerData {
    Enum { variants: Vec<Variant> },
    Struct { fields: Vec<Field> },
}

pub(crate) struct Field {
    id: syn::Ident,
    ty: syn::Type,
    attrs: FieldAttrs,
}

pub(crate) struct Variant {
    id: syn::Ident,
    attrs: VariantAttrs,
}

impl Container {
    pub(crate) fn from_ast(ast: &syn::DeriveInput) -> anyhow::Result<Container> {
        let id = ast.ident.clone();
        let attrs = ContainerAttrs::from_ast(ast)?;

        let data = match &ast.data {
            syn::Data::Struct(data_struct) => ContainerData::Struct {
                fields: Container::struct_from_ast(data_struct)?,
            },
            syn::Data::Enum(enum_data) => ContainerData::Enum {
                variants: Container::enum_from_ast(enum_data)?,
            },
            syn::Data::Union(_) => bail!("Union is not supported now!"),
        };

        Ok(Self { id, attrs, data })
    }

    fn struct_from_ast(ast: &syn::DataStruct) -> anyhow::Result<Vec<Field>> {
        match &ast.fields {
            syn::Fields::Named(fields_names) => {
                fields_names.named.iter().map(Field::from_ast).collect()
            }
            _ => Err(anyhow!("Only named struct supported now!")),
        }
    }

    fn enum_from_ast(ast: &syn::DataEnum) -> anyhow::Result<Vec<Variant>> {
        ast.variants.iter().map(Variant::from_ast).collect()
    }

    pub(crate) fn expend(&self, is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        match &self.data {
            ContainerData::Enum { variants } => self.expand_as_enum(variants, is_from_pb),
            ContainerData::Struct { fields } => self.expand_as_struct(fields, is_from_pb),
        }
    }

    fn expand_as_struct(&self, fields: &[Field], is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        let pb_type = self.attrs.pb_type();
        let ident = &self.id;
        let fields_code = fields
            .iter()
            .map(|f| f.expand(is_from_pb))
            .collect::<anyhow::Result<TokenStream2s>>()?;

        if is_from_pb {
            Ok(quote! {
              impl<'a> ::pb_convert::FromProtobuf<&'a #pb_type> for #ident {
                fn from_protobuf(pb: &'a #pb_type) -> anyhow::Result<Self> {
                  let mut pb = pb;
                  Ok(Self {
                    #(#fields_code),*
                  })
                }
              }
            })
        } else {
            Ok(quote! {
              impl ::pb_convert::IntoProtobuf<#pb_type> for #ident {
                fn into_protobuf(self) -> anyhow::Result<#pb_type> {
                  let mut pb = <#pb_type>::new();
                  #(#fields_code)*;
                  Ok(pb)
                }
              }
            })
        }
    }

    fn expand_as_enum(
        &self,
        variants: &[Variant],
        is_from_pb: bool,
    ) -> anyhow::Result<TokenStream2> {
        let pb_type = self.attrs.pb_type();
        let ident = &self.id;
        let fields_code = variants
            .iter()
            .map(|v| v.expand(self, is_from_pb))
            .collect::<anyhow::Result<TokenStream2s>>()?;

        if is_from_pb {
            Ok(quote! {
              impl ::pb_convert::FromProtobuf<#pb_type> for #ident {
                fn from_protobuf(pb: #pb_type) -> anyhow::Result<Self> {
                  let ret = match pb {
                    #(#fields_code),*
                  };

                  Ok(ret)
                }
              }
            })
        } else {
            Ok(quote! {
              impl ::pb_convert::IntoProtobuf<#pb_type> for #ident {
                fn into_protobuf(self) -> anyhow::Result<#pb_type> {
                    let ret = match self {
                      #(#fields_code),*
                    };

                    Ok(ret)
                }
              }
            })
        }
    }
}

impl Field {
    pub(crate) fn from_ast(ast: &syn::Field) -> anyhow::Result<Field> {
        let id = ast
            .ident
            .clone()
            .ok_or_else(|| anyhow!("Only supports named struct!"))?;
        let ty = ast.ty.clone();
        let attrs = FieldAttrs::from_ast(ast)?;
        Ok(Self { id, ty, attrs })
    }

    fn expand_skipped(&self, is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        let id = &self.id;
        let field_type = &self.ty;

        if is_from_pb {
            Ok(quote! {
              #id: <#field_type>::default()
            })
        } else {
            Ok(quote! {})
        }
    }

    fn expend_non_skipped(&self, is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        let id = &self.id;
        let field_type = &self.ty;
        let pb_field_name = self.attrs.pb_field().unwrap_or(id);

        if is_from_pb {
            let getter = format_ident!("get_{}", pb_field_name);
            Ok(quote! {
              #id: <#field_type>::from_protobuf(pb.#getter())?
            })
        } else {
            let setter = format_ident!("set_{}", pb_field_name);
            Ok(quote! {
              pb.#setter(<#field_type>::into_protobuf(self.#id)?);
            })
        }
    }

    fn expand(&self, is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        if self.attrs.skipped() {
            self.expand_skipped(is_from_pb)
        } else {
            self.expend_non_skipped(is_from_pb)
        }
    }
}

impl Variant {
    pub(crate) fn from_ast(ast: &syn::Variant) -> anyhow::Result<Self> {
        match &ast.fields {
            syn::Fields::Unit => {
                let id = ast.ident.clone();
                let attrs = VariantAttrs::from_ast(ast)?;
                Ok(Self { id, attrs })
            }
            _ => bail!("Only unit enum supported now!"),
        }
    }

    fn expand(&self, container: &Container, is_from_pb: bool) -> anyhow::Result<TokenStream2> {
        let this_path: syn::Path = syn::parse_str(&format!("{}::{}", container.id, self.id))?;
        let pb_type = container.attrs.pb_type();
        let this_id = &self.id;
        let pb_path = self
            .attrs
            .pb_variant()
            .map(|pb_var| -> syn::Path {
                parse_quote! {
                #pb_type::#pb_var
                }
            })
            .unwrap_or_else(|| -> syn::Path {
                parse_quote! {
                #pb_type::#this_id
                }
            });
        if is_from_pb {
            Ok(quote! {
              #pb_path => #this_path
            })
        } else {
            Ok(quote! {
              #this_path => #pb_path
            })
        }
    }
}
