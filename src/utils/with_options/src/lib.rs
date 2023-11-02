use proc_macro::TokenStream;

/// Annotates that the struct represents the WITH properties for a connector.
#[proc_macro_derive(WithOptions, attributes(with_option))]
pub fn derive_helper_attr(_item: TokenStream) -> TokenStream {
    TokenStream::new()
}
