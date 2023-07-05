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
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(custom_test_frameworks)]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(bound_map)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_bounds)]

#[cfg(test)]
mod compactor_tests;
#[cfg(all(test, feature = "failpoints"))]
mod failpoint_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod state_store_tests;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
#[cfg(test)]
mod vacuum_tests;

#[cfg(test)]
mod hummock_read_version_tests;

#[cfg(test)]
mod hummock_storage_tests;
mod mock_notification_client;
#[cfg(all(test, feature = "sync_point"))]
mod sync_point_tests;
pub use mock_notification_client::get_notification_client_for_test;
