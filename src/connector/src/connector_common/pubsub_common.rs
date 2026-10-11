// Copyright 2026 RisingWave Labs
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

const DEFAULT_EMULATOR_PROJECT_ID: &str = "local-project";

pub(crate) fn resolve_pubsub_project_id(
    configured: Option<&str>,
    detected: Option<&str>,
    is_emulator: bool,
) -> Option<String> {
    configured
        .or(detected)
        .or(is_emulator.then_some(DEFAULT_EMULATOR_PROJECT_ID))
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_pubsub_project_id() {
        assert_eq!(
            resolve_pubsub_project_id(Some("configured"), Some("detected"), true).as_deref(),
            Some("configured")
        );
        assert_eq!(
            resolve_pubsub_project_id(None, Some("detected"), true).as_deref(),
            Some("detected")
        );
        assert_eq!(
            resolve_pubsub_project_id(None, None, true).as_deref(),
            Some(DEFAULT_EMULATOR_PROJECT_ID)
        );
        assert_eq!(resolve_pubsub_project_id(None, None, false), None);
    }
}
