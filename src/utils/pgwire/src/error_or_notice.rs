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

use risingwave_common::error::code::PostgresErrorCode;

/// ErrorOrNoticeMessage defines messages that can appear in ErrorResponse and NoticeResponse.
pub struct ErrorOrNoticeMessage<'a> {
    pub severity: Severity,
    pub error_code: PostgresErrorCode,
    pub message: &'a str,
}

impl<'a> ErrorOrNoticeMessage<'a> {
    pub fn internal_error(message: &'a str) -> Self {
        Self {
            severity: Severity::Error,
            error_code: PostgresErrorCode::InternalError,
            message,
        }
    }

    pub fn notice(message: &'a str) -> Self {
        Self {
            severity: Severity::Notice,
            error_code: PostgresErrorCode::SuccessfulCompletion,
            message,
        }
    }
}

/// Severity: the field contents are ERROR, FATAL, or PANIC (in an error message), or WARNING,
/// NOTICE, DEBUG, INFO, or LOG (in a notice message), or a localized translation of one of these.
/// Always present.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Notice,
    Warning,
    Debug,
    Log,
    Info,
}

impl Severity {
    pub fn as_str(&self) -> &str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Notice => "NOTICE",
            Severity::Warning => "WARNING",
            Severity::Debug => "DEBUG",
            Severity::Log => "LOG",
            Severity::Info => "INFO",
        }
    }
}
