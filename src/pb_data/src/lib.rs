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

#![allow(unfulfilled_lint_expectations)]
#![allow(clippy::doc_overindented_list_items)]
// for derived code of `Message`
#![expect(clippy::doc_markdown)]
#![expect(clippy::upper_case_acronyms)]
#![expect(clippy::needless_lifetimes)]
// For tonic::transport::Endpoint::connect
#![expect(clippy::disallowed_methods)]
#![expect(clippy::enum_variant_names)]
#![expect(clippy::module_inception)]
// FIXME: This should be fixed!!! https://github.com/risingwavelabs/risingwave/issues/19906
#![expect(clippy::large_enum_variant)]

use std::str::FromStr;

pub use prost::Message;
use risingwave_error::tonic::ToTonicStatus;
use thiserror::Error;

#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/common.rs")]
pub mod common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/secret.rs")]
pub mod secret;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/expr.rs")]
pub mod expr;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/data.rs")]
pub mod data;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/plan_common.rs")]
pub mod plan_common;

#[rustfmt::skip]
#[path = "common.serde.rs"]
pub mod common_serde;
#[rustfmt::skip]
#[path = "secret.serde.rs"]
pub mod secret_serde;
#[rustfmt::skip]
#[path = "expr.serde.rs"]
pub mod expr_serde;
#[rustfmt::skip]
#[path = "data.serde.rs"]
pub mod data_serde;
#[rustfmt::skip]
#[path = "plan_common.serde.rs"]
pub mod plan_common_serde;

#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[error("field `{0}` not found")]
pub struct PbFieldNotFound(pub &'static str);

impl From<PbFieldNotFound> for tonic::Status {
    fn from(e: PbFieldNotFound) -> Self {
        e.to_status_unnamed(tonic::Code::Internal)
    }
}

impl FromStr for crate::expr::table_function::Type {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}

impl FromStr for crate::expr::agg_call::Kind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}