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

/// Set panic hook to abort the process if we're not catching unwind, without losing the information
/// of stack trace and await-tree.
pub fn set_panic_hook() {
    std::panic::update_hook(|default_hook, info| {
        default_hook(info);

        if let Some(context) = await_tree::current_tree() {
            println!("\n\n*** await tree context of current task ***\n");
            println!("{}\n", context);
        }

        if !risingwave_common::util::panic::is_catching_unwind() {
            std::process::abort();
        }
    });
}
