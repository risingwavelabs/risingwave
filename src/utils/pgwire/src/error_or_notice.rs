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

/// ErrorOrNoticeMessage defines messages that can appear in ErrorResponse and NoticeResponse.
pub struct ErrorOrNoticeMessage<'a> {
    pub severity: Severity,
    pub state: SqlState,
    pub message: &'a str,
}

impl<'a> ErrorOrNoticeMessage<'a> {
    pub fn internal_error(message: &'a str) -> Self {
        Self {
            severity: Severity::Error,
            state: SqlState::INTERNAL_ERROR,
            message,
        }
    }

    pub fn notice(message: &'a str) -> Self {
        Self {
            severity: Severity::Notice,
            state: SqlState::SUCCESSFUL_COMPLETION,
            message,
        }
    }
}

/// Severity: the field contents are ERROR, FATAL, or PANIC (in an error message), or WARNING,
/// NOTICE, DEBUG, INFO, or LOG (in a notice message), or a localized translation of one of these.
/// Always present.
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

/// Code: the SQLSTATE code for the error (see https://www.postgresql.org/docs/current/errcodes-appendix.html).
/// Not localizable. Always present.
pub enum Code {
    E00000,
    E01000,
    XX000,
}

/// A SQLSTATE error code
pub struct SqlState(Code);

impl SqlState {
    pub const INTERNAL_ERROR: SqlState = SqlState(Code::XX000);
    pub const SUCCESSFUL_COMPLETION: SqlState = SqlState(Code::E00000);
    pub const WARNING: SqlState = SqlState(Code::E01000);

    pub fn code(&self) -> &str {
        match &self.0 {
            Code::E00000 => "00000",
            Code::E01000 => "01000",
            Code::XX000 => "XX000",
        }
    }
}
