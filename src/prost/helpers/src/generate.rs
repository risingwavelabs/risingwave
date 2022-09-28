// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use proc_macro_error::abort;
use quote::quote;
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{self, Field, GenericArgument, Lit, Meta, PathArguments, PathSegment, Type};

fn extract_type_from_option(option_segment: &PathSegment) -> Type {
    let generic_arg = match &option_segment.arguments {
        PathArguments::AngleBracketed(params) => params.args.first().unwrap(),
        _ => panic!("Option has no angle bracket"),
    };
    match generic_arg {
        GenericArgument::Type(inner_ty) => inner_ty.clone(),
        _ => panic!("Option's argument must be a type"),
    }
}

/// For example:
/// ```ignore
/// #[prost(enumeration = "data_type::TypeName", tag = "1")]
/// pub type_name: i32,
/// ```
///
/// Returns `data_type::TypeName`.
fn extract_enum_type_from_field(field: &Field) -> Option<Type> {
    use syn::punctuated::Punctuated;
    use syn::Token;

    // The type must be i32.
    match &field.ty {
        Type::Path(path) => {
            if !path.path.segments.first()?.ident.eq("i32") {
                return None;
            }
        }
        _ => return None,
    };

    let attr = field
        .attrs
        .iter()
        .find(|attr| attr.path.is_ident("prost"))?;

    // `enumeration = "data_type::TypeName", tag = "1"`
    let args = attr
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .unwrap();

    let enum_type = args
        .iter()
        .map(|meta| match meta {
            Meta::NameValue(kv) => {
                if kv.path.is_ident("enumeration") {
                    match &kv.lit {
                        // data_type::TypeName
                        Lit::Str(enum_type) => Some(enum_type.to_owned()),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
        .next()??;

    Some(syn::parse_str::<Type>(&enum_type.value()).unwrap())
}

pub fn implement(field: &Field) -> TokenStream2 {
    let field_name = field
        .clone()
        .ident
        .unwrap_or_else(|| abort!(field.span(), "Expected the field to have a name"));

    let getter_fn_name = Ident::new(&format!("get_{}", field_name.unraw()), Span::call_site());

    if let Some(enum_type) = extract_enum_type_from_field(field) {
        return quote! {
            #[inline(always)]
            pub fn #getter_fn_name(&self) -> std::result::Result<#enum_type, crate::ProstFieldNotFound> {
                if self.#field_name.eq(&0) {
                    return Err(crate::ProstFieldNotFound(stringify!(#field_name)));
                }
                #enum_type::from_i32(self.#field_name).ok_or_else(|| crate::ProstFieldNotFound(stringify!(#field_name)))
            }
        };
    };

    let ty = field.ty.clone();
    if let Type::Path(ref type_path) = ty {
        let data_type = type_path.path.segments.last().unwrap();
        if data_type.ident == "Option" {
            // ::core::option::Option<Foo>
            let ty = extract_type_from_option(data_type);
            return quote! {
                #[inline(always)]
                pub fn #getter_fn_name(&self) -> std::result::Result<&#ty, crate::ProstFieldNotFound> {
                    self.#field_name.as_ref().ok_or_else(|| crate::ProstFieldNotFound(stringify!(#field_name)))
                }
            };
        } else if ["u32", "u64", "f32", "f64", "i32", "i64", "bool"]
            .contains(&data_type.ident.to_string().as_str())
        {
            // Primitive types. Return value instead of reference.
            return quote! {
                #[inline(always)]
                pub fn #getter_fn_name(&self) -> #ty {
                    self.#field_name
                }
            };
        }
    }

    quote! {
        #[inline(always)]
        pub fn #getter_fn_name(&self) -> &#ty {
            &self.#field_name
        }
    }
}
