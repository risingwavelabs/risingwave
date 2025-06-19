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

use std::borrow::Cow;

use risingwave_common::error::code::PostgresErrorCode;
use risingwave_common::error::error_request_copy;
use thiserror_ext::AsReport;

use crate::pg_server::BoxedError;

/// ErrorOrNoticeMessage defines messages that can appear in ErrorResponse and NoticeResponse.
pub struct ErrorOrNoticeMessage<'a> {
    pub severity: Severity,
    pub error_code: PostgresErrorCode,
    pub message: Cow<'a, str>,
}

impl<'a> ErrorOrNoticeMessage<'a> {
    /// Create a Postgres error message from an error, with the error code and message extracted from the error.
    pub fn error(error: &BoxedError) -> Self {
        let message = error.to_report_string_pretty();
        let error_code = error_request_copy::<PostgresErrorCode>(&**error)
            .filter(|e| e.is_error()) // should not be warning or success
            .unwrap_or(PostgresErrorCode::InternalError);

        Self {
            severity: Severity::Error,
            error_code,
            message: Cow::Owned(message),
        }
    }

    /// Create a Postgres notice message from a string.
    pub fn notice(message: &'a str) -> Self {
        Self {
            severity: Severity::Notice,
            error_code: PostgresErrorCode::SuccessfulCompletion,
            message: Cow::Borrowed(message),
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
