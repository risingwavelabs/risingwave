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

#[derive(Debug, Clone, Copy)]
pub enum CgroupVersion {
    V1,
    V2,
}

/// Current controllers available in implementation.
pub enum Controller {
    Cpu,
    Memory,
}

/// Default constant Cgroup paths and hierarchy.
const DEFAULT_CGROUP_ROOT_HIERARCYHY: &str = "/sys/fs/cgroup";
const DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH: &str = "/sys/fs/cgroup/cgroup.controllers";
const DEFAULT_CGROUP_MAX_INDICATOR: &str = "max";

mod runtime {
    use std::env;
    use std::path::Path;

    use thiserror_ext::AsReport;

    use super::CgroupVersion;
    use super::util::parse_controller_enable_file_for_cgroup_v2;
    const DEFAULT_DOCKER_ENV_PATH: &str = "/.dockerenv";
    const DEFAULT_LINUX_IDENTIFIER: &str = "linux";
    const DEFAULT_IN_CONTAINER_ENV_VARIABLE: &str = "IN_CONTAINER";
    const DEFAULT_KUBERNETES_SECRETS_PATH: &str = "/var/run/secrets/kubernetes.io";

    fn is_linux_machine() -> bool {
        env::consts::OS.eq(DEFAULT_LINUX_IDENTIFIER)
    }

    /// checks if is running in a docker container by checking for docker env file, or if it is
    /// running in a kubernetes pod.
    fn is_running_in_container() -> bool {
        return env_var_check_if_running_in_container()
            || docker_env_exists()
            || is_running_in_kubernetes_pod();

        /// checks for existence of docker env file
        fn docker_env_exists() -> bool {
            Path::new(DEFAULT_DOCKER_ENV_PATH).exists()
        }

        /// checks for environment
        fn env_var_check_if_running_in_container() -> bool {
            env::var(DEFAULT_IN_CONTAINER_ENV_VARIABLE).is_ok()
        }

        /// checks if it is running in a kubernetes pod
        fn is_running_in_kubernetes_pod() -> bool {
            Path::new(DEFAULT_KUBERNETES_SECRETS_PATH).exists()
        }
    }

    /// Given a certain controller, checks if it is enabled.
    /// For cgroup v1, existence of directory with controller name is checked in cgroup default root
    /// hierarchy. e.g if directory "/sys/fs/cgroup"/cpu" exists then CPU controller is enabled.
    /// For cgroup v2, check the controller list path for the controller name.
    pub fn is_controller_activated(
        controller_type: super::Controller,
        cgroup_version: CgroupVersion,
    ) -> bool {
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

    /// If cgroup exists or is enabled in kernel, returnb true, else false.
    fn cgroup_exists() -> bool {
        Path::new(super::DEFAULT_CGROUP_ROOT_HIERARCYHY).is_dir()
    }

    pub fn get_resource<T>(
        desc: &str,
        controller_type: super::Controller,
        get_system: fn() -> T,
        get_container: fn(CgroupVersion) -> Result<T, std::io::Error>,
    ) -> T {
        if !is_linux_machine() || !is_running_in_container() || !cgroup_exists() {
            return get_system();
        };

        // if cgroup.controllers exist, v2 is used.
        let cgroup_version = if Path::new(super::DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH).exists() {
            super::CgroupVersion::V2
        } else {
            super::CgroupVersion::V1
        };
        if !is_controller_activated(controller_type, cgroup_version) {
            return get_system();
        }

        match get_container(cgroup_version) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    error = %err.as_report(),
                    cgroup_version = ?cgroup_version,
                    "failed to get {desc} in container, use system value instead"
                );
                get_system()
            }
        }
    }
}

pub mod memory {
    use sysinfo::System;

    use super::runtime::get_resource;

    /// Default paths for memory limtiations and usage for cgroup v1 and cgroup v2.
    const V1_MEMORY_LIMIT_PATH: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    const V1_MEMORY_CURRENT_PATH: &str = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    const V2_MEMORY_LIMIT_PATH: &str = "/sys/fs/cgroup/memory.max";
    const V2_MEMORY_CURRENT_PATH: &str = "/sys/fs/cgroup/memory.current";

    /// Returns the system memory.
    pub fn get_system_memory() -> usize {
        let mut sys = System::new();
        sys.refresh_memory();
        sys.total_memory() as usize
    }

    /// Returns the used memory of the system.
    pub fn get_system_memory_used() -> usize {
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
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let mem_used = memory::total_memory_used_bytes();
    /// ```
    pub fn total_memory_used_bytes() -> usize {
        get_resource(
            "memory used",
            super::Controller::Memory,
            get_system_memory_used,
            get_container_memory_used,
        )
    }

    /// Returns the total memory available by the system in bytes.
    ///
    /// If running in container, this function will read the cgroup interface files for the
    /// memory available/limit, if interface files are not found, will return the system memory
    /// volume by default. The cgroup mount point is assumed to be at /sys/fs/cgroup by default.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let mem_available = memory::system_memory_available_bytes();
    /// ```
    pub fn system_memory_available_bytes() -> usize {
        get_resource(
            "memory available",
            super::Controller::Memory,
            get_system_memory,
            get_container_memory_limit,
        )
    }

    /// Returns the memory limit of a container if running in a container else returns the system
    /// memory available.
    /// When the limit is set to max, [`system_memory_available_bytes()`] will return default system
    /// memory.
    fn get_container_memory_limit(
        cgroup_version: super::CgroupVersion,
    ) -> Result<usize, std::io::Error> {
        let limit_path = match cgroup_version {
            super::CgroupVersion::V1 => V1_MEMORY_LIMIT_PATH,
            super::CgroupVersion::V2 => V2_MEMORY_LIMIT_PATH,
        };
        let system = get_system_memory();
        let value = super::util::read_usize_or_max(limit_path, system)?;
        Ok(std::cmp::min(value, system))
    }

    /// Returns the memory used in a container if running in a container else returns the system
    /// memory used.
    fn get_container_memory_used(
        cgroup_version: super::CgroupVersion,
    ) -> Result<usize, std::io::Error> {
        let usage_path = match cgroup_version {
            super::CgroupVersion::V1 => V1_MEMORY_CURRENT_PATH,
            super::CgroupVersion::V2 => V2_MEMORY_CURRENT_PATH,
        };
        let system = get_system_memory_used();
        let value = super::util::read_usize_or_max(usage_path, system)?;
        Ok(std::cmp::min(value, system))
    }
}

pub mod cpu {
    use std::thread;

    use thiserror_ext::AsReport;

    use super::runtime::get_resource;
    use super::util::parse_error;

    /// Default constant Cgroup paths and hierarchy.
    const V1_CPU_QUOTA_PATH: &str = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    const V1_CPU_PERIOD_PATH: &str = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
    const V2_CPU_LIMIT_PATH: &str = "/sys/fs/cgroup/cpu.max";

    /// Returns the total number of cpu available as a float.
    ///
    /// If running in container, this function will return the cpu limit by the container. If not,
    /// it will return the ```available_parallelism``` by the system. A panic will be invoked if
    /// invoking process does not have permission to read appropriate values by
    /// ```std::thread::available_parallelism``` or if the platform is not supported. The cgroup
    /// mount point is assumed to be at /sys/fs/cgroup by default.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ``` ignore
    /// let cpu_available = cpu::total_cpu_available();
    /// ```
    pub fn total_cpu_available() -> f32 {
        get_resource(
            "cpu quota",
            super::Controller::Cpu,
            get_system_cpu,
            get_container_cpu_limit,
        )
    }

    /// Returns the CPU limit of the container.
    fn get_container_cpu_limit(
        cgroup_version: super::CgroupVersion,
    ) -> Result<f32, std::io::Error> {
        let max_cpu = get_system_cpu();
        match cgroup_version {
            super::CgroupVersion::V1 => {
                get_cpu_limit_v1(V1_CPU_QUOTA_PATH, V1_CPU_PERIOD_PATH, max_cpu)
            }
            super::CgroupVersion::V2 => get_cpu_limit_v2(V2_CPU_LIMIT_PATH, max_cpu),
        }
    }

    /// Returns the total system cpu.
    pub fn get_system_cpu() -> f32 {
        match thread::available_parallelism() {
            Ok(available_parallelism) => available_parallelism.get() as f32,
            Err(e) => panic!(
                "Failed to get available parallelism, error: {}",
                e.as_report()
            ),
        }
    }

    /// Returns the CPU limit when cgroup v1 is utilised.
    pub fn get_cpu_limit_v1(
        quota_path: &str,
        period_path: &str,
        max_value: f32,
    ) -> Result<f32, std::io::Error> {
        let content = std::fs::read_to_string(quota_path)?;
        let cpu_quota = content
            .trim()
            .parse::<i64>()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "not a number"))?;
        // According to the kernel documentation, if the value is negative, it means no limit.
        // https://docs.kernel.org/scheduler/sched-bwc.html#management
        if cpu_quota < 0 {
            return Ok(max_value);
        }

        let cpu_period = super::util::read_usize(period_path)?;

        Ok((cpu_quota as f32) / (cpu_period as f32))
    }

    /// Returns the CPU limit when cgroup v2 is utilised.
    pub fn get_cpu_limit_v2(limit_path: &str, max_value: f32) -> Result<f32, std::io::Error> {
        let cpu_limit_string = fs_err::read_to_string(limit_path)?;

        let cpu_data: Vec<&str> = cpu_limit_string.split_whitespace().collect();
        match cpu_data.get(0..2) {
            Some(cpu_data_values) => {
                if cpu_data_values[0] == super::DEFAULT_CGROUP_MAX_INDICATOR {
                    return Ok(max_value);
                }
                let cpu_quota = cpu_data_values[0]
                    .parse::<usize>()
                    .map_err(|e| parse_error(limit_path, &cpu_limit_string, e))?;
                let cpu_period = cpu_data_values[1]
                    .parse::<usize>()
                    .map_err(|e| parse_error(limit_path, &cpu_limit_string, e))?;
                Ok((cpu_quota as f32) / (cpu_period as f32))
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid format in Cgroup CPU interface file, path: {limit_path}, content: {cpu_limit_string}"
                ),
            )),
        }
    }
}

mod util {
    /// Parses the filepath and checks for the existence of `controller_name` in the file.
    pub fn parse_controller_enable_file_for_cgroup_v2(
        file_path: &str,
        controller_name: &str,
    ) -> bool {
        match fs_err::read_to_string(file_path) {
            Ok(controller_string) => {
                for controller in controller_string.split_whitespace() {
                    if controller.eq(controller_name) {
                        return true;
                    };
                }
                false
            }
            Err(_) => false,
        }
    }

    pub fn parse_error(
        file_path: &str,
        content: &str,
        e: impl std::fmt::Display,
    ) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse, path: {file_path}, content: {content}, error: {e}"),
        )
    }

    /// Reads an integer value from a file path.
    pub fn read_usize(file_path: &str) -> Result<usize, std::io::Error> {
        let content = fs_err::read_to_string(file_path)?;
        let limit_val = content
            .trim()
            .parse::<usize>()
            .map_err(|e| parse_error(file_path, &content, e))?;
        Ok(limit_val)
    }

    /// Helper function that helps to retrieve value in file, if value is "max", `max_value` will be
    /// returned instead.
    pub fn read_usize_or_max(file_path: &str, max_value: usize) -> Result<usize, std::io::Error> {
        let content = fs_err::read_to_string(file_path)?;
        if content.trim() == super::DEFAULT_CGROUP_MAX_INDICATOR {
            return Ok(max_value);
        }
        let limit_val = content
            .trim()
            .parse::<usize>()
            .map_err(|e| parse_error(file_path, &content, e))?;
        Ok(limit_val)
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use std::io::prelude::*;
        use std::thread;

        use super::*;
        use crate::cpu::{self, get_system_cpu};
        use crate::memory::get_system_memory;
        use crate::{Controller, DEFAULT_CGROUP_MAX_INDICATOR};
        const DEFAULT_NON_EXISTENT_PATH: &str = "default-non-existent-path";

        #[test]
        fn test_read_integer_from_file_path() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                expected: Result<usize, std::io::Error>,
            }

            let test_cases = HashMap::from([
                (
                    "valid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000"),
                        expected: Ok(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-after",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000   "),
                        expected: Ok(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-before",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("   10000"),
                        expected: Ok(10000),
                    },
                ),
                (
                    "invalid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("test-string"),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "not a number",
                        )),
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "File not found",
                        )),
                    },
                ),
                (
                    "max-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from(DEFAULT_CGROUP_MAX_INDICATOR),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "not a number",
                        )),
                    },
                ),
            ]);

            for tc in test_cases {
                let curr_test_case = &tc.1;
                let mut file: tempfile::NamedTempFile;
                let mut test_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                if curr_test_case.file_exists {
                    file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    file.as_file_mut()
                        .write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                    test_file_path = String::from(file.path().to_str().unwrap())
                }
                match read_usize(&test_file_path) {
                    Ok(int_val) => assert_eq!(&int_val, curr_test_case.expected.as_ref().unwrap()),
                    Err(e) => assert_eq!(
                        e.kind(),
                        curr_test_case.expected.as_ref().unwrap_err().kind()
                    ),
                }
            }
        }

        #[test]
        fn test_get_value_from_file() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                expected: Result<usize, std::io::Error>,
            }

            let test_cases = HashMap::from([
                (
                    "valid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000"),
                        expected: Ok(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-after",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000   "),
                        expected: Ok(10000),
                    },
                ),
                (
                    "valid-integer-value-in-file-with-spaces-before",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("   10000"),
                        expected: Ok(10000),
                    },
                ),
                (
                    "invalid-integer-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("test-string"),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "not a number",
                        )),
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "File not found",
                        )),
                    },
                ),
                (
                    "max-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from(DEFAULT_CGROUP_MAX_INDICATOR),
                        expected: Ok(get_system_memory()),
                    },
                ),
            ]);

            for tc in test_cases {
                let curr_test_case = &tc.1;
                let mut file: tempfile::NamedTempFile;
                let mut test_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                if curr_test_case.file_exists {
                    file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    file.as_file_mut()
                        .write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                    test_file_path = String::from(file.path().to_str().unwrap())
                }
                match read_usize_or_max(&test_file_path, get_system_memory()) {
                    Ok(int_val) => assert_eq!(&int_val, curr_test_case.expected.as_ref().unwrap()),
                    Err(e) => assert_eq!(
                        e.kind(),
                        curr_test_case.expected.as_ref().unwrap_err().kind()
                    ),
                }
            }
        }

        #[test]
        fn test_get_cpu_limit_v1() {
            #[derive(Debug)]
            struct TestCase {
                file_exists: bool,
                value_in_quota_file: String,
                value_in_period_file: String,
                expected: Result<f32, std::io::Error>,
            }

            let test_cases = HashMap::from([
                (
                    "default-values",
                    TestCase {
                        file_exists: true,
                        value_in_quota_file: String::from("-1"),
                        value_in_period_file: String::from("10000"),
                        expected: Ok(thread::available_parallelism().unwrap().get() as f32),
                    },
                ),
                (
                    "valid-values-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_quota_file: String::from("10000"),
                        value_in_period_file: String::from("20000"),
                        expected: Ok(10000.0 / 20000.0),
                    },
                ),
                (
                    "empty-value-in-files",
                    TestCase {
                        file_exists: true,
                        value_in_quota_file: String::from(""),
                        value_in_period_file: String::from(""),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid format in Cgroup CPU interface file",
                        )),
                    },
                ),
                (
                    "Invalid-string-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_quota_file: String::from("10000"),
                        value_in_period_file: String::from("test-string "),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "not a number",
                        )),
                    },
                ),
                (
                    "negative-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_quota_file: String::from("-2"),
                        value_in_period_file: String::from("20000"),
                        expected: Ok(thread::available_parallelism().unwrap().get() as f32),
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_quota_file: String::from("10000 20000"),
                        value_in_period_file: String::from("10000 20000"),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "File not found",
                        )),
                    },
                ),
            ]);
            for tc in test_cases {
                let curr_test_case = &tc.1;
                let mut quota_file: tempfile::NamedTempFile;
                let mut period_file: tempfile::NamedTempFile;
                let mut test_quota_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                let mut test_period_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                if curr_test_case.file_exists {
                    quota_file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    quota_file
                        .as_file_mut()
                        .write_all(curr_test_case.value_in_quota_file.as_bytes())
                        .expect("Error while writing to file");
                    test_quota_file_path = String::from(quota_file.path().to_str().unwrap());

                    period_file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    period_file
                        .as_file_mut()
                        .write_all(curr_test_case.value_in_period_file.as_bytes())
                        .expect("Error while writing to file");
                    test_period_file_path = String::from(period_file.path().to_str().unwrap());
                }
                match cpu::get_cpu_limit_v1(
                    &test_quota_file_path,
                    &test_period_file_path,
                    get_system_cpu(),
                ) {
                    Ok(int_val) => assert_eq!(
                        &int_val,
                        curr_test_case.expected.as_ref().unwrap(),
                        "{:?}",
                        tc
                    ),
                    Err(e) => assert_eq!(
                        e.kind(),
                        curr_test_case.expected.as_ref().unwrap_err().kind()
                    ),
                }
            }
        }

        #[test]
        fn test_get_cpu_limit_v2() {
            struct TestCase {
                file_exists: bool,
                value_in_file: String,
                expected: Result<f32, std::io::Error>,
            }

            let test_cases = HashMap::from([
                (
                    "valid-values-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000 20000"),
                        expected: Ok(10000.0 / 20000.0),
                    },
                ),
                (
                    "Invalid-single-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000"),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid format in Cgroup CPU interface file",
                        )),
                    },
                ),
                (
                    "Invalid-string-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("10000 test-string "),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "not a number",
                        )),
                    },
                ),
                (
                    "max-value-in-file",
                    TestCase {
                        file_exists: true,
                        value_in_file: String::from("max 20000"),
                        expected: Ok(thread::available_parallelism().unwrap().get() as f32),
                    },
                ),
                (
                    "file-not-exist",
                    TestCase {
                        file_exists: false,
                        value_in_file: String::from(""),
                        expected: Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "File not found",
                        )),
                    },
                ),
            ]);
            for tc in test_cases {
                let curr_test_case = &tc.1;
                let mut file: tempfile::NamedTempFile;
                let mut test_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                if curr_test_case.file_exists {
                    file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    file.as_file_mut()
                        .write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                    test_file_path = String::from(file.path().to_str().unwrap())
                }
                match cpu::get_cpu_limit_v2(&test_file_path, get_system_cpu()) {
                    Ok(int_val) => assert_eq!(&int_val, curr_test_case.expected.as_ref().unwrap()),
                    Err(e) => assert_eq!(
                        e.kind(),
                        curr_test_case.expected.as_ref().unwrap_err().kind()
                    ),
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

            for tc in test_cases {
                let curr_test_case = &tc.1;
                let controller_name: &str = match curr_test_case.controller_type {
                    Controller::Cpu => "cpu",
                    Controller::Memory => "memory",
                };
                let mut file: tempfile::NamedTempFile;
                let mut test_file_path = String::from(DEFAULT_NON_EXISTENT_PATH);
                if curr_test_case.file_exists {
                    file = tempfile::NamedTempFile::new()
                        .expect("Error encountered while creating file!");
                    file.as_file_mut()
                        .write_all(curr_test_case.value_in_file.as_bytes())
                        .expect("Error while writing to file");
                    test_file_path = String::from(file.path().to_str().unwrap())
                }
                assert_eq!(
                    parse_controller_enable_file_for_cgroup_v2(&test_file_path, controller_name),
                    curr_test_case.expected
                );
            }
        }
    }
}
