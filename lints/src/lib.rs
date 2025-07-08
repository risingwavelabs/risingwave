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

#![feature(rustc_private)]
#![feature(let_chains)]
#![warn(unused_extern_crates)]

extern crate rustc_ast;
extern crate rustc_data_structures;
extern crate rustc_hir;
extern crate rustc_lexer;
extern crate rustc_lint;
extern crate rustc_session;
extern crate rustc_span;

mod format_error;
mod utils;

dylint_linting::dylint_library!();

#[allow(clippy::no_mangle_with_rust_abi)]
#[unsafe(no_mangle)]
pub fn register_lints(_sess: &rustc_session::Session, lint_store: &mut rustc_lint::LintStore) {
    // -- Begin lint registration --

    // Preparation steps.
    let format_args_storage = clippy_utils::macros::FormatArgsStorage::default();
    let format_args = format_args_storage.clone();
    lint_store.register_early_pass(move || {
        Box::new(utils::format_args_collector::FormatArgsCollector::new(
            format_args.clone(),
        ))
    });

    // Actual lints.
    lint_store.register_lints(&[format_error::FORMAT_ERROR]);
    let format_args = format_args_storage.clone();
    lint_store
        .register_late_pass(move |_| Box::new(format_error::FormatError::new(format_args.clone())));

    // --  End lint registration  --

    // Register lints into groups.
    // Note: use `rw_` instead of `rw::` to avoid "error[E0602]: unknown lint tool: `rw`".
    register_group(lint_store, "rw_all", |_| true);
    register_group(lint_store, "rw_warnings", |l| {
        l.default_level >= rustc_lint::Level::Warn
    });
}

fn register_group(
    lint_store: &mut rustc_lint::LintStore,
    name: &'static str,
    filter_predicate: impl Fn(&rustc_lint::Lint) -> bool,
) {
    let lints = lint_store
        .get_lints()
        .iter()
        .filter(|l| l.name.starts_with("rw::"))
        .filter(|l| filter_predicate(l))
        .map(|l| rustc_lint::LintId::of(l))
        .collect();

    lint_store.register_group(true, name, None, lints);
}
