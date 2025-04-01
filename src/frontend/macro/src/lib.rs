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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemFn, ItemStruct, LitStr, Result, Token, parse_macro_input};

#[proc_macro_attribute]
pub fn system_catalog(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = item.clone();
    let attr = parse_macro_input!(attr as Attr);
    let item = parse_macro_input!(item as syn::Item);

    match system_catalog_inner(attr, item) {
        // concat item and generated code
        Ok(output) => {
            input.extend(TokenStream::from(output));
            input
        }
        Err(err) => err.to_compile_error().into(),
    }
}

fn system_catalog_inner(attr: Attr, item: syn::Item) -> Result<TokenStream2> {
    match item {
        syn::Item::Fn(item_fn) => gen_sys_table(attr, item_fn),
        syn::Item::Struct(item_struct) => gen_sys_view(attr, item_struct),
        _ => Err(syn::Error::new_spanned(item, "expect function or struct")),
    }
}

struct Attr {
    kind: Ident,
    schema_name: String,
    table_name: String,
    sql: Option<String>,
}

impl Parse for Attr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let kind = input.parse::<syn::Ident>()?;
        input.parse::<Token![,]>()?;
        let name = input.parse::<LitStr>()?;
        let full_name = name.value();
        let (schema_name, table_name) = full_name
            .split_once('.')
            .ok_or_else(|| syn::Error::new_spanned(name, "expect \"schema.table\""))?;
        let sql = if input.parse::<Token![,]>().is_ok() {
            Some(input.parse::<LitStr>()?.value())
        } else {
            None
        };
        Ok(Attr {
            kind,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            sql,
        })
    }
}

/// Check if the type is `type_<T>` and return `T`.
fn strip_outer_type<'a>(ty: &'a syn::Type, type_: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let seg = path.path.segments.last()?;
    if seg.ident != type_ {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else {
        return None;
    };
    Some(ty)
}

fn gen_sys_table(attr: Attr, item_fn: ItemFn) -> Result<TokenStream2> {
    if attr.kind != "table" {
        return Err(syn::Error::new_spanned(attr.kind, "expect `table`"));
    }

    let schema_name = &attr.schema_name;
    let table_name = &attr.table_name;
    let gen_fn_name = format_ident!("{}_{}", attr.schema_name, attr.table_name);
    let user_fn_name = item_fn.sig.ident;

    let return_type_error =
        || syn::Error::new_spanned(&item_fn.sig.output, "expect `-> Result<Vec<T>>`");
    let syn::ReturnType::Type(_, ty) = &item_fn.sig.output else {
        return Err(return_type_error());
    };
    let (return_result, ty) = match strip_outer_type(ty, "Result") {
        Some(ty) => (true, ty),
        None => (false, ty.as_ref()),
    };
    let struct_type = strip_outer_type(ty, "Vec").ok_or_else(return_type_error)?;
    let _await = item_fn.sig.asyncness.map(|_| quote!(.await));
    let handle_error = return_result.then(|| quote!(?));
    let chunk_size = 1024usize;

    Ok(quote! {
        #[linkme::distributed_slice(crate::catalog::system_catalog::SYS_CATALOGS_SLICE)]
        #[unsafe(no_mangle)]    // to prevent duplicate schema.table name
        fn #gen_fn_name() -> crate::catalog::system_catalog::BuiltinCatalog {
            const _: () = {
                assert!(#struct_type::PRIMARY_KEY.is_some(), "primary key is required for system table");
            };

            #[futures_async_stream::try_stream(boxed, ok = risingwave_common::array::DataChunk, error = risingwave_common::error::BoxedError)]
            async fn function(reader: &crate::catalog::system_catalog::SysCatalogReaderImpl) {
                let rows = #user_fn_name(reader) #_await #handle_error;
                let mut builder = #struct_type::data_chunk_builder(#chunk_size);
                for row in rows {
                    if let Some(chunk) = builder.append_one_row(row.into_owned_row()) {
                        yield chunk;
                    }
                }
                if let Some(chunk) = builder.consume_all() {
                    yield chunk;
                }
            }

            crate::catalog::system_catalog::BuiltinCatalog::Table(crate::catalog::system_catalog::BuiltinTable {
                name: #table_name,
                schema: #schema_name,
                columns: #struct_type::fields(),
                pk: #struct_type::PRIMARY_KEY.unwrap(),
                function,
            })
        }
    })
}

fn gen_sys_view(attr: Attr, item_struct: ItemStruct) -> Result<TokenStream2> {
    if attr.kind != "view" {
        return Err(syn::Error::new_spanned(attr.kind, "expect `view`"));
    }
    let schema_name = &attr.schema_name;
    let table_name = &attr.table_name;
    let gen_fn_name = format_ident!("{}_{}", attr.schema_name, attr.table_name);
    let struct_type = &item_struct.ident;

    let sql = if let Some(sql) = attr.sql {
        quote! { #sql.into() }
    } else {
        quote! { crate::catalog::system_catalog::infer_dummy_view_sql(&fields) }
    };

    Ok(quote! {
        #[linkme::distributed_slice(crate::catalog::system_catalog::SYS_CATALOGS_SLICE)]
        #[unsafe(no_mangle)]    // to prevent duplicate schema.table name
        fn #gen_fn_name() -> crate::catalog::system_catalog::BuiltinCatalog {
            let fields = #struct_type::fields();
            crate::catalog::system_catalog::BuiltinCatalog::View(crate::catalog::system_catalog::BuiltinView {
                name: #table_name,
                schema: #schema_name,
                sql: #sql,
                columns: fields,
            })
        }
    })
}
