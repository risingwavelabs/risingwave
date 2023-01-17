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

pub mod telemetry;

use serde::{Deserialize, Serialize};
use sysinfo::{System, SystemExt};

#[derive(Debug, Serialize, Deserialize)]
struct SystemData {
    memory: Memory,
    os: OS,
}

#[derive(Debug, Serialize, Deserialize)]
struct Memory {
    total_mem: u64,
    available_mem: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct OS {
    name: String,
    kernel_version: String,
    version: String,
}

impl SystemData {
    fn new() -> Self {
        let sys = System::new_all();
        SystemData {
            memory: Memory {
                available_mem: sys.available_memory(),
                total_mem: sys.total_memory(),
            },
            os: OS {
                name: sys.name().unwrap_or_default(),
                kernel_version: sys.kernel_version().unwrap_or_default(),
                version: sys.os_version().unwrap_or_default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_data() {
        let sys = SystemData::new();
        println!("{:?}", sys);
    }
}
