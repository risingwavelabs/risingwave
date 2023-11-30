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
#[no_mangle]
pub fn register_lints(_sess: &rustc_session::Session, lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_early_pass(|| {
        Box::<utils::format_args_collector::FormatArgsCollector>::default()
    });

    lint_store.register_lints(&[format_error::FORMAT_ERROR]);
    lint_store.register_late_pass(|_| Box::<format_error::FormatError>::default());
}
