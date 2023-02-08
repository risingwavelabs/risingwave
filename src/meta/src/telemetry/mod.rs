// Copyright 2023 Singularity Data
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

pub mod report;

/// Environment Variable that is default to be true
const TELEMETRY_ENV_ENABLE: &str = "ENABLE_TELEMETRY";

/// check whether telemetry is enabled
pub fn telemetry_enabled() -> bool {
    // default to be true
    std::env::var(TELEMETRY_ENV_ENABLE)
        .unwrap_or("true".to_string())
        .trim()
        .to_ascii_lowercase()
        .parse()
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_enabled() {
        assert!(telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "false");
        assert!(!telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "wrong_str");
        assert!(telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "False");
        assert!(!telemetry_enabled());
    }
}
