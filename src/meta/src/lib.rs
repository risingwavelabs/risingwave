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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(extract_if)]
#![feature(hash_extract_if)]
#![feature(btree_extract_if)]
#![feature(result_option_inspect)]
#![feature(lazy_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(assert_matches)]
#![feature(try_blocks)]
#![cfg_attr(coverage, feature(coverage_attribute))]
#![feature(custom_test_frameworks)]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(is_sorted)]
#![feature(impl_trait_in_assoc_type)]
#![feature(type_name_of_val)]

pub mod backup_restore;
pub mod barrier;
pub mod controller;
#[cfg(not(madsim))] // no need in simulation test
pub mod dashboard;
pub mod error;
pub mod hummock;
pub mod manager;
pub mod model;
pub mod rpc;
pub mod serving;
pub mod storage;
pub mod stream;
pub mod telemetry;

pub use error::{MetaError, MetaResult};
pub use rpc::{ElectionClient, ElectionMember, EtcdElectionClient};

use crate::manager::MetaOpts;

#[derive(Debug)]
pub enum MetaStoreBackend {
    Etcd {
        endpoints: Vec<String>,
        credentials: Option<(String, String)>,
    },
    Mem,
}
