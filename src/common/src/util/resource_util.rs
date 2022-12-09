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

pub enum CgroupVersion {
    V1,
    V2,
}

// Current controllers available in immplementation.
pub enum Controller {
    Cpu,
    Memory,
}

// Default constant Cgroup paths and hierarchy.
const DEFAULT_CGROUP_ROOT_HIERARCYHY: &str = "/sys/fs/cgroup";
const DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH: &str = "/sys/fs/cgroup/cgroup.controllers";

mod runtime {
    use std::env;
    use std::path::Path;
    const DEFAULT_DOCKER_ENV_PATH: &str = "/.dockerenv";
    const DEFAULT_LINUX_IDENTIFIER: &str = "linux";
    const DEFAULT_IN_CONTAINER_ENV_VARIABLE: &str = "IN_CONTAINER";
    const DEFAULT_KUBERNETES_SECRETS_PATH: &str = "/var/run/secrets/kubernetes.io";

    pub fn is_linux_machine() -> bool {
        env::consts::OS.eq(DEFAULT_LINUX_IDENTIFIER)
    }

    // checks if is running in a docker container by checking for docker env file, or if it is
    // running in a kubernetes pod.
    pub fn is_running_in_container() -> bool {
        env_var_check_if_running_in_container()
            || docker_env_exists()
            || is_running_in_kubernetes_pod()
    }

    // checks for existence of docker env file
    pub fn docker_env_exists() -> bool {
        Path::new(DEFAULT_DOCKER_ENV_PATH).exists()
    }

    // checks for environment
    pub fn env_var_check_if_running_in_container() -> bool {
        env::var(DEFAULT_IN_CONTAINER_ENV_VARIABLE).is_ok()
    }

    // checks if it is running in a kubernetes pod
    pub fn is_running_in_kubernetes_pod() -> bool {
        Path::new(DEFAULT_KUBERNETES_SECRETS_PATH).exists()
    }
}

pub mod memory {
    use sysinfo::{System, SystemExt};

    // Default paths for memory limtiations and usage for cgroup_v1 and cgroup_v2.
    const V1_MEMORY_LIMIT_HIERARCHY: &str = "/memory/memory.limit_in_bytes";
    const V1_MEMORY_CURRENT_HIERARCHY: &str = "/memory/memory.usage_in_bytes";
    const V2_MEMORY_LIMIT_HIERARCHY: &str = "/memory.max";
    const V2_MEMORY_CURRENT_HIERARCHY: &str = "/memory.current";

    // Returns the system memory.
    fn get_system_memory() -> usize {
        let mut sys = System::new();
        sys.refresh_memory();
        sys.total_memory() as usize
    }

    // Returns the used memory of the system.
    fn get_system_memory_used() -> usize {
        let mut sys = System::new();
        sys.refresh_memory();
        sys.used_memory() as usize
    }

    /// Returns the total memory used by the system in bytes.
    ///
    /// If running in container, this function will read the cgroup interface files for the
    /// memory used, if interface files are not found, will return the memory used in the system
    /// as default. The cgroup mount point is assumed to be at /sys/fs/cgroup by default.
    ///
    /// [`with_capacity`]: #method.with_capacity
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let mem_used = memory::total_memory_used_bytes();
    /// ```
    pub fn total_memory_used_bytes() -> usize {
        if !super::runtime::is_linux_machine() || !super::runtime::is_running_in_container() {
            return get_system_memory_used();
        };
        std::cmp::min(
            get_memory_used_in_container(super::util::get_cgroup_version())
                .unwrap_or_else(get_system_memory_used),
            get_system_memory_used(),
        )
    }

    /// Returns the total memory available by the system in bytes.
    ///
    /// If running in container, this function will read the cgroup interface files for the
    /// memory available/limit, if interface files are not found, will return the system memory
    /// volume by default. The cgroup mount point is assumed to be at /sys/fs/cgroup by default.
    ///
    /// [`with_capacity`]: #method.with_capacity
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let mem_available = memory::total_memory_available_bytes();
    /// ```
    pub fn total_memory_available_bytes() -> usize {
        if !super::runtime::is_linux_machine() || !super::runtime::is_running_in_container() {
            return get_system_memory();
        };
        std::cmp::min(
            get_container_memory_limit(super::util::get_cgroup_version())
                .unwrap_or_else(get_system_memory),
            get_system_memory(),
        )
    }

    // Returns the memory limit of a container if running in a container else returns the system
    // memory available.
    // When the limit is set to max, which is all memory in system, it will return a none,
    // which will be handled in total_memory_available_bytes() to return default system memory.
    fn get_container_memory_limit(cgroup_version: Option<super::CgroupVersion>) -> Option<usize> {
        if !super::util::is_controller_activated(super::Controller::Memory)
            || cgroup_version.is_none()
        {
            return None;
        }
        let limit_path = match cgroup_version.unwrap() {
            super::CgroupVersion::V1 => format!(
                "{}{}",
                super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
                V1_MEMORY_LIMIT_HIERARCHY
            ),
            super::CgroupVersion::V2 => format!(
                "{}{}",
                super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
                V2_MEMORY_LIMIT_HIERARCHY
            ),
        };
        super::util::read_integer_from_file_path(&limit_path)
    }

    // Returns the memory used in a container if running in a container else returns the system
    // memory used.
    fn get_memory_used_in_container(cgroup_version: Option<super::CgroupVersion>) -> Option<usize> {
        if !super::util::is_controller_activated(super::Controller::Memory)
            || cgroup_version.is_none()
        {
            return None;
        }
        let usage_path = match cgroup_version.unwrap() {
            super::CgroupVersion::V1 => format!(
                "{}{}",
                super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
                V1_MEMORY_CURRENT_HIERARCHY
            ),
            super::CgroupVersion::V2 => format!(
                "{}{}",
                super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
                V2_MEMORY_CURRENT_HIERARCHY
            ),
        };
        super::util::read_integer_from_file_path(&usage_path)
    }
}

pub mod cpu {
    use std::thread;

    use super::util;

    // Default constant Cgroup paths and hierarchy.
    const V1_CPU_QUOTA_HIERARCHY: &str = "/cpu/cpu.cfs_quota_us";
    const V1_CPU_PERIOD_HIERARCHY: &str = "/cpu/cpu.cfs_period_us";
    const V2_CPU_LIMIT_HIERARCHY: &str = "/cpu.max";

    /// Returns the total number of cpu available as a float.
    ///
    /// If running in container, this function will return the cpu limit by the container. If not,
    /// it will return the ```available_parallelism``` by the system. A panic will be invoked if
    /// invoking process does not have permission to read appropriate values by
    /// ```std::thread::available_parallelism``` or if the platform is not supported. The cgroup
    /// mount point is assumed to be at /sys/fs/cgroup by default.
    ///
    /// [`with_capacity`]: #method.with_capacity
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let cpu_available = cpu::total_cpu_available();
    /// ```
    pub fn total_cpu_available() -> f32 {
        if !super::runtime::is_linux_machine() || !super::runtime::is_running_in_container() {
            return get_system_cpu();
        }

        get_container_cpu_limit(super::util::get_cgroup_version()).unwrap_or_else(get_system_cpu)
    }

    // Returns the CPU limit of the container.
    fn get_container_cpu_limit(cgroup_version: Option<super::CgroupVersion>) -> Option<f32> {
        if !super::util::is_controller_activated(super::Controller::Cpu) || cgroup_version.is_none()
        {
            return None;
        }

        match cgroup_version.unwrap() {
            super::CgroupVersion::V1 => get_cpu_limit_v1(),
            super::CgroupVersion::V2 => get_cpu_limit_v2(),
        }
    }

    // Returns the total system cpu.
    pub fn get_system_cpu() -> f32 {
        match thread::available_parallelism() {
            Ok(available_parallelism) => available_parallelism.get() as f32,
            Err(e) => panic!("Platform is not supported, error: {}", e),
        }
    }

    // Returns the CPU limit when cgroup_V1 is utilised.
    fn get_cpu_limit_v1() -> Option<f32> {
        let cpu_quota = match super::util::read_integer_from_file_path(&format!(
            "{}{}",
            super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
            V1_CPU_QUOTA_HIERARCHY
        )) {
            Some(quota_val) => quota_val,
            None => return None,
        };

        let cpu_period = match super::util::read_integer_from_file_path(&format!(
            "{}{}",
            super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
            V1_CPU_PERIOD_HIERARCHY
        )) {
            Some(period_val) => period_val,
            None => return None,
        };
        Some((cpu_quota as f32) / (cpu_period as f32))
    }

    // Returns the CPU limit when cgroup_V2 is utilised.
    fn get_cpu_limit_v2() -> Option<f32> {
        util::read_cgroup_v2_cpu_limit_from_file_path(&format!(
            "{}{}",
            super::DEFAULT_CGROUP_ROOT_HIERARCYHY,
            V2_CPU_LIMIT_HIERARCHY
        ))
    }
}

mod util {
    use std::fs;
    use std::path::Path;

    // Returns a cgroup version if it exists, else returns None.
    // Checks for the existence of the root hierarchy directory.
    pub fn get_cgroup_version() -> Option<super::CgroupVersion> {
        // check if cgroup exists.
        if !Path::new(super::DEFAULT_CGROUP_ROOT_HIERARCYHY).is_dir() {
            return None;
        }
        // if cgroup.controllers exist, v2 is used.
        if Path::new(super::DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH).exists() {
            Some(super::CgroupVersion::V2)
        } else {
            Some(super::CgroupVersion::V1)
        }
    }

    // Reads an integer value from a file path.
    // If max is read from file, a none will be returned regardless, which should be handled by
    // wrapper functions.
    pub fn read_integer_from_file_path(file_path: &str) -> Option<usize> {
        match fs::read_to_string(file_path) {
            Ok(limit_str) => match limit_str.trim().parse::<usize>() {
                Ok(limit_val) => Some(limit_val),
                Err(_) => None,
            },
            Err(_) => None,
        }
    }

    // Parses the filepath and checks for the existence of controller_name in the file.
    pub fn parse_controller_enable_file_for_cgroup_v2(
        file_path: &str,
        controller_name: &str,
    ) -> bool {
        return match fs::read_to_string(file_path) {
            Ok(controller_string) => {
                for controller in controller_string.split_whitespace() {
                    if controller.eq(controller_name) {
                        return true;
                    };
                }
                false
            }
            Err(_) => false,
        };
    }

    // Given a certain controller, checks if it is enabled.
    // For cgroup_v1, existence of directory with controller name is checked in cgroup default root
    // hierarchy. e.g if directory "/sys/fs/cgroup"/cpu" exists then CPU controller is enabled.
    // For cgroup_v2, check the controller list path for the controller name.
    pub fn is_controller_activated(controller_type: super::Controller) -> bool {
        match get_cgroup_version() {
            Some(cgroup_version) => {
                let controller_name: &str = match controller_type {
                    super::Controller::Cpu => "cpu",
                    super::Controller::Memory => "memory",
                };
                match cgroup_version {
                    super::CgroupVersion::V1 => Path::new(super::DEFAULT_CGROUP_ROOT_HIERARCYHY)
                        .join(controller_name)
                        .is_dir(),
                    super::CgroupVersion::V2 => parse_controller_enable_file_for_cgroup_v2(
                        super::DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH,
                        controller_name,
                    ),
                }
            }
            None => false,
        }
    }

    // Helper function to parse a cpu limit file path for cgroup_v2.
    // returns the CPU limit when cgroup_V2 is utilised.
    // interface file should have the format as such -> "{cpu_quota} {cpu_period}". e.g "max
    // 1000000".
    pub fn read_cgroup_v2_cpu_limit_from_file_path(file_path: &str) -> Option<f32> {
        match fs::read_to_string(file_path) {
            Ok(cpu_limit_string) => parse_cgroup_v2_cpu_limit_string(&cpu_limit_string),
            Err(_) => None,
        }
    }

    // Helper function to parse the string inside the cgroup cpu limit file.
    pub fn parse_cgroup_v2_cpu_limit_string(cpu_limit_string: &str) -> Option<f32> {
        let cpu_data: Vec<&str> = cpu_limit_string.split_whitespace().collect();
        match cpu_data.get(0..2) {
            Some(cpu_data_values) => {
                let cpu_quota = match cpu_data_values[0].parse::<usize>() {
                    Ok(quota_val) => quota_val,
                    Err(_) => return None,
                };
                let cpu_period = match cpu_data_values[1].parse::<usize>() {
                    Ok(period_val) => period_val,
                    Err(_) => return None,
                };
                Some((cpu_quota as f32) / (cpu_period as f32))
            }
            None => None,
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use std::fs::File;
        use std::io::prelude::*;

        use super::*;
        use crate::util::resource_util::Controller;

        #[test]
        fn test_read_integer_from_file_path() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                expected: Option<usize>,
            }

            let test_cases = HashMap::from([
                (
                    "valid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000"),
                        expected: Some(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-after",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000   "),
                        expected: Some(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-before",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("   10000"),
                        expected: Some(10000),
                    },
                ),
                (
                    "invalid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("test-string"),
                        expected: None,
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        expected: None,
                    },
                ),
                (
                    "max-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("max"),
                        expected: None,
                    },
                ),
            ]);

            let test_file_path = "resource-util-test-1";
            for tc in test_cases {
                let curr_test_case = &tc.1;
                if curr_test_case.file_exists {
                    let mut file = File::create(test_file_path)
                        .expect("Error encountered while creating file!");
                    file.write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                };
                assert_eq!(
                    read_integer_from_file_path(test_file_path),
                    curr_test_case.expected
                );
                if Path::new(test_file_path).exists() {
                    fs::remove_file(test_file_path).expect("File delete failed");
                }
            }
        }

        #[test]
        fn test_parse_cgroup_v2_cpu_limit_string() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                expected: Option<f32>,
            }

            let test_cases = HashMap::from([
                (
                    "valid-values-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000 20000"),
                        expected: Some(10000.0 / 20000.0),
                    },
                ),
                (
                    "Invalid-single-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000"),
                        expected: None,
                    },
                ),
                (
                    "Invalid-string-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000 test-string "),
                        expected: None,
                    },
                ),
                (
                    "max-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("max 20000"),
                        expected: None,
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        expected: None,
                    },
                ),
            ]);

            let test_file_path = "resource-util-test-2";
            for tc in test_cases {
                let curr_test_case = &tc.1;
                if curr_test_case.file_exists {
                    let mut file = File::create(test_file_path)
                        .expect("Error encountered while creating file!");
                    file.write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                };
                assert_eq!(
                    read_cgroup_v2_cpu_limit_from_file_path(test_file_path),
                    curr_test_case.expected
                );
                if Path::new(test_file_path).exists() {
                    fs::remove_file(test_file_path).expect("File delete failed");
                }
            }
        }

        #[test]
        fn test_parse_controller_enable_file_for_cgroup_v2() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                controller_type: Controller,
                expected: bool,
            }

            let test_cases = HashMap::from([
                (
                    "cpu-enabled",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("cpu memory IO"),
                        controller_type: Controller::Cpu,
                        expected: true,
                    },
                ),
                (
                    "memory-enabled",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("cpu memory IO"),
                        controller_type: Controller::Memory,
                        expected: true,
                    },
                ),
                (
                    "memory-disabled",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("cpu IO"),
                        controller_type: Controller::Memory,
                        expected: false,
                    },
                ),
                (
                    "cpu-disabled",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("memory IO"),
                        controller_type: Controller::Cpu,
                        expected: false,
                    },
                ),
                (
                    "Invalid-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("test-string test-string"),
                        controller_type: Controller::Cpu,
                        expected: false,
                    },
                ),
                (
                    "controller-file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        controller_type: Controller::Memory,
                        expected: false,
                    },
                ),
            ]);

            let test_file_path = "resource-util-test-3";
            for tc in test_cases {
                let curr_test_case = &tc.1;
                if curr_test_case.file_exists {
                    let mut file = File::create(test_file_path)
                        .expect("Error encountered while creating file!");
                    file.write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                };
                let controller_name: &str = match curr_test_case.controller_type {
                    Controller::Cpu => "cpu",
                    Controller::Memory => "memory",
                };
                assert_eq!(
                    parse_controller_enable_file_for_cgroup_v2(test_file_path, controller_name),
                    curr_test_case.expected
                );
                if Path::new(test_file_path).exists() {
                    fs::remove_file(test_file_path).expect("File delete failed");
                }
            }
        }
    }
}
