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

use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::{Error, FnArg, Ident, ItemFn, Pat, PatType, Result, ReturnType, Token, Type, Visibility};

use crate::utils::extend_vis_with_super;

/// See [`super::define_context!`].
#[derive(Debug, Clone)]
pub(super) struct DefineContextField {
    vis: Visibility,
    name: Ident,
    ty: Type,
}

/// See [`super::define_context!`].
#[derive(Debug, Clone)]
pub(super) struct DefineContextAttr {
    fields: Vec<DefineContextField>,
}

impl Parse for DefineContextField {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;

        Ok(Self { vis, name, ty })
    }
}

impl Parse for DefineContextAttr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let fields = input.parse_terminated(DefineContextField::parse, Token![,])?;
        Ok(Self {
            fields: fields.into_iter().collect(),
        })
    }
}

impl DefineContextField {
    pub(super) fn r#gen(self) -> Result<TokenStream> {
        let Self { vis, name, ty } = self;

        // We create a sub mod, so we need to extend the vis of getter.
        let vis: Visibility = extend_vis_with_super(vis);

        {
            let name_s = name.to_string();
            if name_s.to_uppercase() != name_s {
                return Err(Error::new_spanned(
                    name,
                    "the name of context variable should be uppercase",
                ));
            }
        }

        Ok(quote! {
            #[allow(non_snake_case)]
            pub mod #name {
                use super::*;
                pub type Type = #ty;

                tokio::task_local! {
                    static LOCAL_KEY: #ty;
                }

                #vis fn try_with<F, R>(f: F) -> Result<R, risingwave_expr::ExprError>
                where
                    F: FnOnce(&#ty) -> R
                {
                    LOCAL_KEY.try_with(f).map_err(|_| risingwave_expr::ContextUnavailable::new(stringify!(#name))).map_err(Into::into)
                }

                pub fn scope<F>(value: #ty, f: F) -> tokio::task::futures::TaskLocalFuture<#ty, F>
                where
                    F: std::future::Future
                {
                    LOCAL_KEY.scope(value, f)
                }

                pub fn sync_scope<F, R>(value: #ty, f: F) -> R
                where
                    F: FnOnce() -> R
                {
                    LOCAL_KEY.sync_scope(value, f)
                }
            }
        })
    }
}

impl DefineContextAttr {
    pub(super) fn r#gen(self) -> Result<TokenStream> {
        let generated_fields: Vec<TokenStream> = self
            .fields
            .into_iter()
            .map(DefineContextField::r#gen)
            .try_collect()?;
        Ok(quote! {
            #(#generated_fields)*
        })
    }
}

pub struct CaptureContextAttr {
    /// The context variables which are captured.
    captures: Vec<Ident>,
}

impl Parse for CaptureContextAttr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let captures = input.parse_terminated(Ident::parse, Token![,])?;
        Ok(Self {
            captures: captures.into_iter().collect(),
        })
    }
}

pub(super) fn generate_captured_function(
    attr: CaptureContextAttr,
    mut user_fn: ItemFn,
) -> Result<TokenStream> {
    let CaptureContextAttr { captures } = attr;
    let is_async = user_fn.sig.asyncness.is_some();
    let mut orig_user_fn = user_fn.clone();
    if is_async {
        // Modify the return type to impl Future<Output = output> + Send + 'static for the original function.
        let output_type = match &orig_user_fn.sig.output {
            ReturnType::Type(_, ty) => ty.clone(),
            ReturnType::Default => Box::new(syn::parse_quote!(())),
        };
        orig_user_fn.sig.output = ReturnType::Type(
            syn::token::RArrow::default(),
            Box::new(
                syn::parse_quote!(impl std::future::Future<Output = #output_type> + Send + 'static + use<>),
            ),
        );
        orig_user_fn.sig.asyncness = None;

        // Generate clone statements for each input
        let input_def: Vec<TokenStream> = orig_user_fn
            .sig
            .inputs
            .iter()
            .map(|arg| {
                if let FnArg::Typed(PatType { pat, .. }) = arg
                    && let Pat::Ident(ident) = pat.as_ref()
                {
                    let ident_name = &ident.ident;
                    return quote! {
                        let #ident_name = #ident_name.clone();
                    };
                }
                quote! {}
            })
            .collect();

        // Wrap the original function body in async move { ... }.
        let orig_body = &orig_user_fn.block;
        orig_user_fn.block = Box::new(syn::parse_quote!({
            #(#input_def)*
            async move { #orig_body }
        }));
    }

    let sig = &mut user_fn.sig;

    let name = sig.ident.clone();

    // Modify the name.
    {
        let new_name = format!("{}_captured", name);
        let new_name = Ident::new(&new_name, sig.ident.span());
        sig.ident = new_name;
    }

    if is_async {
        // Ensure the function is async
        sig.asyncness = Some(syn::token::Async::default());
    }

    // Modify the inputs of sig.
    let inputs = &mut sig.inputs;
    if inputs.len() < captures.len() {
        return Err(syn::Error::new_spanned(
            inputs,
            format!("expected at least {} inputs", captures.len()),
        ));
    }

    let arg_names: Vec<_> = inputs
        .iter()
        .map(|arg| {
            let FnArg::Typed(arg) = arg else {
                return Err(syn::Error::new_spanned(
                    arg,
                    "receiver is not allowed in captured function",
                ));
            };
            Ok(arg.pat.to_token_stream())
        })
        .try_collect()?;

    let (captured_inputs, remained_inputs) = {
        let mut inputs = inputs.iter().cloned();
        let inputs = inputs.by_ref();
        let captured_inputs = inputs.take(captures.len()).collect_vec();
        let remained_inputs = inputs.collect_vec();
        (captured_inputs, remained_inputs)
    };
    *inputs = remained_inputs.into_iter().collect();

    let call_old_fn = quote! {
        #name(#(#arg_names),*)
    };

    let new_body = {
        let mut scoped = quote! {
            #call_old_fn
        };

        #[allow(clippy::disallowed_methods)]
        for (context, arg) in captures.into_iter().zip(captured_inputs.into_iter()) {
            let FnArg::Typed(arg) = arg else {
                return Err(syn::Error::new_spanned(
                    arg,
                    "receiver is not allowed in captured function",
                ));
            };
            let name = arg.pat.into_token_stream();
            // TODO: Can we add an assertion here that `&<<#context::Type> as Deref>::Target` is same as `#arg.ty`?
            scoped = if is_async {
                quote_spanned! { context.span()=>
                    #context::try_with(|#name| { #scoped })
                }
            } else {
                quote_spanned! { context.span()=>
                    #context::try_with(|#name| { #scoped }).flatten()
                }
            };
        }
        scoped
    };
    let new_user_fn = {
        let vis = user_fn.vis;
        let sig = user_fn.sig;
        if is_async {
            quote! {
                #vis #sig {
                    {#new_body}?.await
                }
            }
        } else {
            quote! {
                #vis #sig {
                    {#new_body}.map_err(Into::into)
                }
            }
        }
    };

    Ok(quote! {
        #orig_user_fn
        #new_user_fn
    })
}
