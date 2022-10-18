#![feature(io_error_other)]
// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![feature(lint_reasons)]
#![expect(clippy::doc_markdown, reason = "FIXME: later")]

pub mod error;
pub mod error_or_notice;
pub mod pg_extended;
pub mod pg_field_descriptor;
pub mod pg_message;
pub mod pg_protocol;
pub mod pg_response;
pub mod pg_server;
pub mod types;
