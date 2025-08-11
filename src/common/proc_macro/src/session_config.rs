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

use bae::FromAttributes;
use proc_macro_error::{OptionExt, ResultExt, abort};
use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::DeriveInput;

#[derive(FromAttributes)]
struct Parameter {
    pub rename: Option<syn::LitStr>,
    pub alias: Option<syn::Expr>,
    pub default: syn::Expr,
    pub flags: Option<syn::LitStr>,
    pub check_hook: Option<syn::Expr>,
}

pub(crate) fn derive_config(input: DeriveInput) -> TokenStream {
    let syn::Data::Struct(syn::DataStruct { fields, .. }) = input.data else {
        abort!(input, "Only struct is supported");
    };

    let mut default_fields = vec![];
    let mut struct_impl_set = vec![];
    let mut struct_impl_get = vec![];
    let mut struct_impl_reset = vec![];
    let mut set_match_branches = vec![];
    let mut get_match_branches = vec![];
    let mut reset_match_branches = vec![];
    let mut show_all_list = vec![];
    let mut list_all_list = vec![];
    let mut alias_to_entry_name_branches = vec![];
    let mut entry_name_flags = vec![];

    for field in fields {
        let field_ident = field.ident.expect_or_abort("Field need to be named");
        let ty = field.ty;

        let mut doc_list = vec![];
        for attr in &field.attrs {
            if attr.path.is_ident("doc") {
                let meta = attr.parse_meta().expect_or_abort("Failed to parse meta");
                if let syn::Meta::NameValue(val) = meta
                    && let syn::Lit::Str(desc) = val.lit
                {
                    doc_list.push(desc.value().trim().to_owned());
                }
            }
        }

        let description: TokenStream = format!("r#\"{}\"#", doc_list.join(" ")).parse().unwrap();

        let attr =
            Parameter::from_attributes(&field.attrs).expect_or_abort("Failed to parse attribute");
        let Parameter {
            rename,
            alias,
            default,
            flags,
            check_hook: check_hook_name,
        } = attr;

        let entry_name = if let Some(rename) = rename {
            if !(rename.value().is_ascii() && rename.value().to_ascii_lowercase() == rename.value())
            {
                abort!(rename, "Expect `rename` to be an ascii lower case string");
            }
            quote! {#rename}
        } else {
            let ident = format_ident!("{}", field_ident.to_string().to_lowercase());
            quote! {stringify!(#ident)}
        };

        if let Some(alias) = alias {
            alias_to_entry_name_branches.push(quote! {
                #alias => #entry_name.to_string(),
            })
        }

        let flags = flags.map(|f| f.value()).unwrap_or_default();
        let flags: Vec<_> = flags.split('|').map(|str| str.trim()).collect();

        default_fields.push(quote_spanned! {
            field_ident.span()=>
            #field_ident: #default.into(),
        });

        let set_func_name = format_ident!("set_{}_str", field_ident);
        let set_t_func_name = format_ident!("set_{}", field_ident);
        let set_t_inner_func_name = format_ident!("set_{}_inner", field_ident);
        let set_t_func_doc: TokenStream =
            format!("r#\"Set parameter {} by a typed value.\"#", entry_name)
                .parse()
                .unwrap();
        let set_func_doc: TokenStream = format!("r#\"Set parameter {} by a string.\"#", entry_name)
            .parse()
            .unwrap();

        let gen_set_func_name = if flags.contains(&"SETTER") {
            set_t_inner_func_name.clone()
        } else {
            set_t_func_name.clone()
        };

        let check_hook = if let Some(check_hook_name) = check_hook_name {
            quote! {
                #check_hook_name(&val).map_err(|e| {
                    SessionConfigError::InvalidValue {
                        entry: #entry_name,
                        value: val.to_string(),
                        source: anyhow::anyhow!(e),
                    }
                })?;
            }
        } else {
            quote! {}
        };

        let report_hook = if flags.contains(&"REPORT") {
            quote! {
                if self.#field_ident != val {
                    reporter.report_status(#entry_name, val.to_string());
                }
            }
        } else {
            quote! {}
        };

        // An easy way to check if the type is bool and use a different parse function.
        let parse = if quote!(#ty).to_string() == "bool" {
            quote!(risingwave_common::cast::str_to_bool)
        } else {
            quote!(<#ty as ::std::str::FromStr>::from_str)
        };

        struct_impl_set.push(quote! {
            #[doc = #set_func_doc]
            pub fn #set_func_name(
                &mut self,
                val: &str,
                reporter: &mut impl ConfigReporter
            ) -> SessionConfigResult<String> {
                let val_t = #parse(val).map_err(|e| {
                    SessionConfigError::InvalidValue {
                        entry: #entry_name,
                        value: val.to_string(),
                        source: anyhow::anyhow!(e),
                    }
                })?;

                self.#set_t_func_name(val_t, reporter).map(|val| val.to_string())
            }

            #[doc = #set_t_func_doc]
            pub fn #gen_set_func_name(
                &mut self,
                val: #ty,
                reporter: &mut impl ConfigReporter
            ) -> SessionConfigResult<#ty> {
                #check_hook
                #report_hook

                self.#field_ident = val.clone();
                Ok(val)
            }

        });

        let reset_func_name = format_ident!("reset_{}", field_ident);
        struct_impl_reset.push(quote! {

        #[allow(clippy::useless_conversion)]
        pub fn #reset_func_name(&mut self, reporter: &mut impl ConfigReporter) -> String {
                let val = #default;
                #report_hook
                self.#field_ident = val.into();
                self.#field_ident.to_string()
            }
        });

        let get_func_name = format_ident!("{}_str", field_ident);
        let get_t_func_name = format_ident!("{}", field_ident);
        let get_func_doc: TokenStream =
            format!("r#\"Get a value string of parameter {} \"#", entry_name)
                .parse()
                .unwrap();
        let get_t_func_doc: TokenStream =
            format!("r#\"Get a typed value of parameter {} \"#", entry_name)
                .parse()
                .unwrap();

        struct_impl_get.push(quote! {
            #[doc = #get_func_doc]
            pub fn #get_func_name(&self) -> String {
                self.#get_t_func_name().to_string()
            }

            #[doc = #get_t_func_doc]
            pub fn #get_t_func_name(&self) -> #ty {
                self.#field_ident.clone()
            }

        });

        get_match_branches.push(quote! {
            #entry_name => Ok(self.#get_func_name()),
        });

        set_match_branches.push(quote! {
            #entry_name => self.#set_func_name(&value, reporter),
        });

        reset_match_branches.push(quote! {
            #entry_name => Ok(self.#reset_func_name(reporter)),
        });

        let var_info = quote! {
            VariableInfo {
                name: #entry_name.to_string(),
                setting: self.#field_ident.to_string(),
                description : #description.to_string(),
            },
        };
        list_all_list.push(var_info.clone());

        let no_show_all = flags.contains(&"NO_SHOW_ALL");
        let no_show_all_flag: TokenStream = no_show_all.to_string().parse().unwrap();
        if !no_show_all {
            show_all_list.push(var_info);
        }

        let no_alter_sys_flag: TokenStream =
            flags.contains(&"NO_ALTER_SYS").to_string().parse().unwrap();

        entry_name_flags.push(
            quote! {
                (#entry_name, ParamFlags {no_show_all: #no_show_all_flag, no_alter_sys: #no_alter_sys_flag})
            }
        );
    }

    let struct_ident = input.ident;
    quote! {
        use std::collections::HashMap;
        use std::sync::LazyLock;
        static PARAM_NAME_FLAGS: LazyLock<HashMap<&'static str, ParamFlags>> = LazyLock::new(|| HashMap::from([#(#entry_name_flags, )*]));

        struct ParamFlags {
            no_show_all: bool,
            no_alter_sys: bool,
        }

        impl Default for #struct_ident {
            #[allow(clippy::useless_conversion)]
            fn default() -> Self {
                Self {
                    #(#default_fields)*
                }
            }
        }

        impl #struct_ident {
            fn new() -> Self {
                Default::default()
            }

            pub fn alias_to_entry_name(key_name: &str) -> String {
                let key_name = key_name.to_ascii_lowercase();
                match key_name.as_str() {
                    #(#alias_to_entry_name_branches)*
                    _ => key_name,
                }
            }

            #(#struct_impl_get)*

            #(#struct_impl_set)*

            #(#struct_impl_reset)*

            /// Set a parameter given it's name and value string.
            pub fn set(&mut self, key_name: &str, value: String, reporter: &mut impl ConfigReporter) -> SessionConfigResult<String> {
                let key_name = Self::alias_to_entry_name(key_name);
                match key_name.as_ref() {
                    #(#set_match_branches)*
                    _ => Err(SessionConfigError::UnrecognizedEntry(key_name.to_string())),
                }
            }

            /// Get a parameter by it's name.
            pub fn get(&self, key_name: &str) -> SessionConfigResult<String> {
                let key_name = Self::alias_to_entry_name(key_name);
                match key_name.as_ref() {
                    #(#get_match_branches)*
                    _ => Err(SessionConfigError::UnrecognizedEntry(key_name.to_string())),
                }
            }

            /// Reset a parameter by it's name.
            pub fn reset(&mut self, key_name: &str, reporter: &mut impl ConfigReporter) -> SessionConfigResult<String> {
                let key_name = Self::alias_to_entry_name(key_name);
                match key_name.as_ref() {
                    #(#reset_match_branches)*
                    _ => Err(SessionConfigError::UnrecognizedEntry(key_name.to_string())),
                }
            }

            /// Show all parameters except those specified `NO_SHOW_ALL`.
            pub fn show_all(&self) -> Vec<VariableInfo> {
                vec![
                    #(#show_all_list)*
                ]
            }

            /// List all parameters
            pub fn list_all(&self) -> Vec<VariableInfo> {
                vec![
                    #(#list_all_list)*
                ]
            }

            /// Check if `SessionConfig` has a parameter.
            pub fn contains_param(key_name: &str) -> bool {
                let key_name = Self::alias_to_entry_name(key_name);
                PARAM_NAME_FLAGS.contains_key(key_name.as_str())
            }

            /// Check if `SessionConfig` has a parameter.
            pub fn check_no_alter_sys(key_name: &str) -> SessionConfigResult<bool> {
                let key_name = Self::alias_to_entry_name(key_name);
                let flags = PARAM_NAME_FLAGS.get(key_name.as_str()).ok_or_else(|| SessionConfigError::UnrecognizedEntry(key_name.to_string()))?;
                Ok(flags.no_alter_sys)
            }
        }
    }
}
