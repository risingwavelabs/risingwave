extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro2;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_error::{proc_macro_error, ResultExt};
use syn::{DataStruct, DeriveInput};

#[cfg(not(tarpaulin_include))]
mod generate;

// This attribute will be placed before any pb types, including messages and enums.
#[cfg(not(tarpaulin_include))]
#[proc_macro_derive(AnyPB)]
#[proc_macro_error]
pub fn any_pb(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast: DeriveInput = syn::parse(input).expect_or_abort("Couldn't parse for getters");

    // Build the impl
    let gen = produce(&ast);

    // Return the generated impl
    gen.into()
}

// Procedure macros can not be tested from the same crate.
#[cfg(not(tarpaulin_include))]
fn produce(ast: &DeriveInput) -> TokenStream2 {
    let name = &ast.ident;

    // Is it a struct?
    if let syn::Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let generated = fields.iter().map(generate::implement);
        quote! {
            impl #name {
                #(#generated)*
            }
        }
    } else {
        // Do nothing.
        quote! {}
    }
}
