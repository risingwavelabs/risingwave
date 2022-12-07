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

// Container aware resource utility functions
pub mod resource_util {
    use std::path::Path;
    use std::fs;
    use std::env;

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
    const DEFAULT_CGROUP_ROOT_HIERARCYHY:  &str = "/sys/fs/cgroup";
    const DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH: &str = "/sys/fs/cgroup/cgroup.controllers";
    const DEFAULT_DOCKER_ENV_PATH: &str = "/.dockerenv";
    const DEFAULT_KUBERNETES_SECRETS_PATH: &str = "/var/run/secrets/kubernetes.io";
    const DEFAULT_LINUX_IDENTIFIER: &str = "linux";
    const DEFAULT_IN_CONTAINER_ENV_VARIABLE: &str = "IN_CONTAINER";

    pub fn get_default_root_hierarchy() -> String {
        String::from(DEFAULT_CGROUP_ROOT_HIERARCYHY)
    }

    fn is_linux_machine() -> bool {
        if env::consts::OS.eq(DEFAULT_LINUX_IDENTIFIER){
            return true;
        }
        false
    }

    // returns a cgroup version if it exists, else returns None.
    // Checks for the existence of the root hierarchy directory.
    fn get_cgroup_version() -> Option<CgroupVersion> {
        // check if cgroup exists.
        if  !Path::new(DEFAULT_CGROUP_ROOT_HIERARCYHY).is_dir(){
            return None
        }
        // if cgroup.controllers exist, v2 is used.
        if Path::new(DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH).exists(){
            return Some(CgroupVersion::V2)
        }else{
            return Some(CgroupVersion::V1)
        }
    }

    // checks if is running in a docker container by checking for docker env file, or if it is running in a kubernetes pod.
    pub fn is_running_in_container() -> bool {
        env_var_check_if_running_in_container() || docker_env_exists() || is_running_in_kubernetes_pod()
    }

    // checks for existance of docker env file
    fn docker_env_exists() -> bool {
        Path::new(DEFAULT_DOCKER_ENV_PATH).exists()
    }

    // checks for environment 
    fn env_var_check_if_running_in_container() -> bool {
        match env::var(DEFAULT_IN_CONTAINER_ENV_VARIABLE) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    // checks if it is running in a kubernetes pod
    fn is_running_in_kubernetes_pod() -> bool {
        Path::new(DEFAULT_KUBERNETES_SECRETS_PATH).exists()
    }

    // parses the filepath and checks for existence of controller_name in file.
    fn parse_controller_enable_file_for_cgroup_v2(file_path: &str, controller_name: &str) -> bool {
        match fs::read_to_string(file_path){
            Ok(controller_string) => {
                for controller in controller_string.split_whitespace() {
                    if controller.eq(controller_name){
                        return true;
                    };
                }
                return false;
            },
            Err(_) => {
                return false;
            }
        };
    }

    // Given a certain controller, check if it is enabled
    // For cgroup_v1, existence of directory with controller name is checked in cgroup default root hierarchy. e.g if directory "/sys/fs/cgroup"/cpu" exists then CPU controller is enabled.
    // For cgroup_v2, check the controller list path for the controller name.
    fn is_controller_activated(controller_type: Controller) -> bool {
        match get_cgroup_version(){
            Some(cgroup_version) => {
                let controller_name: &str  = match controller_type {
                    Controller::Cpu => "cpu",
                    Controller::Memory => "memory",
                };
                match cgroup_version{
                    CgroupVersion::V1 => return Path::new(DEFAULT_CGROUP_ROOT_HIERARCYHY).join(controller_name).is_dir(),
                    CgroupVersion::V2 => return parse_controller_enable_file_for_cgroup_v2(DEFAULT_CGROUP_V2_CONTROLLER_LIST_PATH, controller_name),
                }
            },
            None => return false,
        }
    }

    // Reads an integer value from a file path.
    fn read_integer_from_file_path(file_path: &str) -> Option<usize> {
        match fs::read_to_string(file_path){
            Ok(limit_str) => {
                match limit_str.trim().parse::<usize>(){
                    Ok(limit_val) => return Some(limit_val),
                    Err(_) => {
                        return None;
                    },
                }
            },
            Err(_) => {
                return None;
            }
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
        fn get_system_memory() -> usize{
            let mut sys = System::new();
            sys.refresh_memory();
            sys.total_memory() as usize
        }

        // Returns the used memory of the system
        fn get_system_memory_used() -> usize {
            let mut sys = System::new();
            sys.refresh_memory();
            sys.used_memory() as usize
        }

         // Returns total memory used, if running in container, will return total memory used in container that process runs in.
         pub fn total_memory_used_bytes() -> usize{
            if !super::is_linux_machine() || ! super::is_running_in_container(){
                return get_system_memory_used();
            }
            match super::get_cgroup_version() {
                Some(cgroup_version) => { 
                    match   get_memory_used_in_container(cgroup_version){
                        Some(mem_usage) => return mem_usage,
                        None => return get_system_memory_used(),
                    };
                },
                None => return get_system_memory_used(),
            }
        }

        // Returns total memory available, if running in container, will return total memory limit in container that process runs in.
        pub fn total_memory_available_bytes() -> usize {
            if !super::is_linux_machine() || ! super::is_running_in_container(){
                return get_system_memory();
            }
            match super::get_cgroup_version() {
                Some(cgroup_version) => { 
                    match get_container_memory_limit(cgroup_version){
                        Some(mem_limit) => return mem_limit,
                        None => return get_system_memory(),
                    };
                },
                None => return get_system_memory(),
            }
        }

        // Returns the memory limit of a container if running in a container else returns the system memory available.
        fn get_container_memory_limit(cgroup_version: super::CgroupVersion) -> Option<usize> {
            if !super::is_controller_activated(super::Controller::Memory){
                    return None;
            }
            let limit_path = match cgroup_version{
                super::CgroupVersion::V1 => format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V1_MEMORY_LIMIT_HIERARCHY),
                super::CgroupVersion::V2 =>  format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V2_MEMORY_LIMIT_HIERARCHY),
            };
            return super::read_integer_from_file_path(&limit_path);
        }

        // Returns the memory used in a container if running in a container else returns the system memory used.
        fn get_memory_used_in_container(cgroup_version: super::CgroupVersion) -> Option<usize> {
            if !super::is_controller_activated(super::Controller::Memory){
                    return None
            }
            let usage_path = match cgroup_version{
                super::CgroupVersion::V1 => format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V1_MEMORY_CURRENT_HIERARCHY),
                super::CgroupVersion::V2 =>  format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V2_MEMORY_CURRENT_HIERARCHY),
            };
            return super::read_integer_from_file_path(& usage_path);
        }
    }

    pub mod cpu{
        use std::fs;
        use num_cpus;

         // Default constant Cgroup paths and hierarchy.
        const V1_CPU_QUOTA_HIERARCHY: &str = "/cpu/cpu.cfs_quota_us";
        const V1_CPU_PERIOD_HIERARCHY: &str = "/cpu/cpu.cfs_period_us";
        const V2_CPU_LIMIT_HIERARCHY: &str = "/cpu.max";

        // Returns the total number of CPU available, will return cpu limit if running in container.
        pub fn total_cpu_available()-> f32 {
            if !super::is_linux_machine() || !super::is_running_in_container(){
                return get_system_cpu();
            }

            match super::get_cgroup_version() {
                Some(cgroup_version) => { 
                    match get_container_cpu_limit(cgroup_version){
                        Some(cpu_limit) => return cpu_limit,
                        None => get_system_cpu(),
                    }
                },
                None => get_system_cpu(),
            }
        }

        // Returns the CPU limit of the container.
        fn get_container_cpu_limit(cgroup_version: super::CgroupVersion) -> Option<f32> {
            if !super::is_controller_activated(super::Controller::Cpu){
                return None;
            }

            match cgroup_version{
                super::CgroupVersion::V1 => return get_cpu_limit_v1(),
                super::CgroupVersion::V2 =>  return get_cpu_limit_v2(),
            };
        }

        // Returns the total system cpu.
        fn get_system_cpu() -> f32 {
            return num_cpus::get() as f32;
        }

        // Returns the CPU limit when cgroup_V1 is utilised.
        fn get_cpu_limit_v1() -> Option<f32> {
            let cpu_quota: usize;
            let cpu_period: usize;

            match super::read_integer_from_file_path(&format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V1_CPU_QUOTA_HIERARCHY)){
                Some(quota_val) => cpu_quota = quota_val,
                None => return None,
            }
            match super::read_integer_from_file_path(&format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V1_CPU_PERIOD_HIERARCHY)){
                Some(period_val) => cpu_period = period_val,
                None => return None,
            }
            return Some((cpu_quota as f32)/(cpu_period as f32));
        }

         // Returns the CPU limit when cgroup_V2 is utilised.
        fn get_cpu_limit_v2() -> Option<f32> {
            read_cgroup_v2_cpu_limit_from_file_path(&format!("{}{}", super::DEFAULT_CGROUP_ROOT_HIERARCYHY, V2_CPU_LIMIT_HIERARCHY))
        }

        // Helper function to parse a cpu limit file path for cgroup_v2.
        // returns the CPU limit when cgroup_V2 is utilised.
        // interface file should have the format as such -> "{cpu_quota} {cpu_period}". e.g "max 1000000".
        fn read_cgroup_v2_cpu_limit_from_file_path(file_path: &str) -> Option<f32>{
            match fs::read_to_string(file_path){
                Ok(cpu_limit_string) => return parse_cgroup_v2_cpu_limit_string(&cpu_limit_string),
                Err(_) => return None,
            };
        }

        fn parse_cgroup_v2_cpu_limit_string(cpu_limit_string: &str) -> Option<f32>{
            let cpu_data: Vec<&str> = cpu_limit_string.trim().split_whitespace().collect();
            match cpu_data.get(0..2){
                Some(cpu_data_values) => {
                    let cpu_quota: usize;
                    let cpu_period: usize;
                    match cpu_data_values[0].parse::<usize>(){
                        Ok(quota_val) => cpu_quota = quota_val,
                        Err(_) => return None,
                    };
                    match cpu_data_values[1].parse::<usize>(){
                        Ok(period_val) => cpu_period = period_val,
                        Err(_) => return None,
                    };
                    return Some((cpu_quota as f32)/(cpu_period as f32));
                },
                None => {
                    return None;
                },
            }
        }
    }
}