// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::quote;
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{
    AttrStyle, Attribute, Error, Expr, ExprLit, Field, GenericArgument, Lit, Meta, Path,
    PathArguments, PathSegment, Result, Type,
};

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
    use syn::Token;
    use syn::punctuated::Punctuated;

    // The type must be i32.
    match &field.ty {
        Type::Path(path) => {
            if !path.path.segments.first()?.ident.eq("i32") {
                return None;
            }
        }
        _ => return None,
    };

    // find attribute `#[prost(...)]`
    let attr = field
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("prost"))?;

    // `enumeration = "data_type::TypeName", tag = "1"`
    let args = attr
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .unwrap();

    let enum_type = args
        .iter()
        .map(|meta| match meta {
            Meta::NameValue(kv) if kv.path.is_ident("enumeration") => {
                match &kv.value {
                    // data_type::TypeName
                    Expr::Lit(ExprLit {
                        lit: Lit::Str(str), ..
                    }) => Some(str.to_owned()),
                    _ => None,
                }
            }
            _ => None,
        })
        .next()??;

    Some(syn::parse_str::<Type>(&enum_type.value()).unwrap())
}

fn is_deprecated(field: &Field) -> bool {
    field.attrs.iter().any(|attr| match &attr.meta {
        Meta::Path(path) => path.is_ident("deprecated"),
        _ => false,
    })
}

pub fn implement(field: &Field) -> Result<TokenStream2> {
    let field_name = field
        .clone()
        .ident
        .ok_or_else(|| Error::new(field.span(), "Expected the field to have a name"))?;

    let getter_fn_name = Ident::new(&format!("get_{}", field_name.unraw()), Span::call_site());
    let is_deprecated = is_deprecated(field);

    let attr_list: Vec<Attribute> = if is_deprecated {
        vec![Attribute {
            pound_token: Default::default(),
            style: AttrStyle::Outer,
            bracket_token: Default::default(),
            meta: Meta::from(Path::from(Ident::new("deprecated", Span::call_site()))),
        }]
    } else {
        vec![]
    };

    if let Some(enum_type) = extract_enum_type_from_field(field) {
        return Ok(quote! {
            #(#attr_list)*
            #[inline(always)]
            pub fn #getter_fn_name(&self) -> std::result::Result<#enum_type, crate::PbFieldNotFound> {
                if self.#field_name.eq(&0) {
                    return Err(crate::PbFieldNotFound(stringify!(#field_name)));
                }
                #enum_type::from_i32(self.#field_name).ok_or_else(|| crate::PbFieldNotFound(stringify!(#field_name)))
            }
        });
    };

    let ty = field.ty.clone();
    if let Type::Path(ref type_path) = ty {
        let data_type = type_path.path.segments.last().unwrap();
        if data_type.ident == "Option" {
            // ::core::option::Option<Foo>
            let ty = extract_type_from_option(data_type);
            return Ok(quote! {
                #(#attr_list)*
                #[inline(always)]
                pub fn #getter_fn_name(&self) -> std::result::Result<&#ty, crate::PbFieldNotFound> {
                    self.#field_name.as_ref().ok_or_else(|| crate::PbFieldNotFound(stringify!(#field_name)))
                }
            });
        } else if ["u32", "u64", "f32", "f64", "i32", "i64", "bool"]
            .contains(&data_type.ident.to_string().as_str())
        {
            // Primitive types. Return value instead of reference.
            return Ok(quote! {
                #(#attr_list)*
                #[inline(always)]
                pub fn #getter_fn_name(&self) -> #ty {
                    self.#field_name
                }
            });
        }
    }

    Ok(quote! {
        #(#attr_list)*
        #[inline(always)]
        pub fn #getter_fn_name(&self) -> &#ty {
            &self.#field_name
        }
    })
}
