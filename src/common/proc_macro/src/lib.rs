// Copyright 2023 RisingWave Labs
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

#![cfg_attr(coverage, feature(no_coverage))]

use proc_macro_error::proc_macro_error;
use syn::parse_macro_input;

mod config;

/// Sections in the configuration file can use `#[derive(OverrideConfig)]` to generate the
/// implementation of overwriting configs from the file.
///
/// In the struct definition, use #[override_opts(path = ...)] on a field to indicate the field in
/// `RwConfig` to override.
///
/// An example:
///
/// ```ignore
/// #[derive(OverrideConfig)]
/// struct Opts {
///     #[override_opts(path = meta.listen_addr)]
///     listen_addr: Option<String>,
/// }
/// ```
///
/// will generate
///
/// impl OverrideConfig for Opts {
///     fn r#override(self, config: &mut RwConfig) {
///         if let Some(v) = self.required_str {
///             config.meta.listen_addr = v;
///         }
///     }
/// }
/// ```
#[cfg_attr(coverage, no_coverage)]
#[proc_macro_derive(OverrideConfig, attributes(override_opts))]
#[proc_macro_error]
pub fn override_config(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input);

    let gen = config::produce_override_config(input);

    gen.into()
}
