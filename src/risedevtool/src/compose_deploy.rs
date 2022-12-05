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

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::os::unix::prelude::PermissionsExt;
use std::path::Path;

use anyhow::Result;

use crate::ComposeConfig;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct Ec2Instance {
    pub id: String,
    pub dns_host: String,
    pub public_ip: String,
    pub r#type: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct ComposeDeployConfig {
    pub instances: Vec<Ec2Instance>,
    pub risingwave_image_override: Option<String>,
    pub risedev_extra_args: HashMap<String, String>,
}

impl ComposeDeployConfig {
    pub fn lookup_instance_by_id(&self, id: &str) -> &Ec2Instance {
        self.instances.iter().find(|i| i.id == id).unwrap()
    }

    pub fn lookup_instance_by_host(&self, host: &str) -> &Ec2Instance {
        self.instances.iter().find(|i| i.dns_host == host).unwrap()
    }
}

pub fn compose_deploy(
    output_directory: &Path,
    steps: &[String],
    ec2_instances: &[Ec2Instance],
    compose_config: &ComposeConfig,
    service_on_node: &BTreeMap<String, String>,
) -> Result<()> {
    let shell_script = {
        use std::fmt::Write;
        let ssh_extra_args = "-o \"UserKnownHostsFile=/dev/null\" -o \"StrictHostKeyChecking=no\" -o \"LogLevel=ERROR\"";
        let mut x = String::new();
        writeln!(x, "#!/bin/bash -e")?;
        writeln!(x)?;
        writeln!(
            x,
            r#"
DIR="$( cd "$( dirname "${{BASH_SOURCE[0]}}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR""#
        )?;

        writeln!(x)?;

        writeln!(
            x,
            r#"
DO_ALL_STEPS=1

while getopts '1234' OPT; do
    case $OPT in
        1)
            DO_SYNC=1
            DO_ALL_STEPS=0
            ;;
        2)
            DO_TEAR_DOWN=1
            DO_ALL_STEPS=0
            ;;
        3)
            DO_START=1
            DO_ALL_STEPS=0
            ;;
        4)
            DO_CHECK=1
            DO_ALL_STEPS=0
        ;;
    esac
done

if [[ "$DO_ALL_STEPS" -eq 1 ]]; then
DO_SYNC=1
DO_TEAR_DOWN=1
DO_START=1
DO_CHECK=1
fi
"#
        )?;
        writeln!(x, r#"if [[ "$DO_SYNC" -eq 1 ]]; then"#)?;
        writeln!(x, "# --- Sync Config ---")?;
        writeln!(x, r#"echo "$(tput setaf 2)(1/4) sync config$(tput sgr0)""#)?;
        writeln!(x, "echo -e \"If this step takes too long time, maybe EC2 IP has been changed. You'll need to re-run:\\n* $(tput setaf 2)terraform apply$(tput sgr0) to get the latest IP,\\n* and then $(tput setaf 2)./risedev compose-deploy <profile>$(tput sgr0) again to update the deploy script.\"")?;
        writeln!(x, "parallel --linebuffer bash << EOF")?;
        for instance in ec2_instances {
            let host = &instance.dns_host;
            let public_ip = &instance.public_ip;
            let id = &instance.id;
            let base_folder = "~/risingwave-deploy";
            let mut y = String::new();
            writeln!(y, "#!/bin/bash -e")?;
            writeln!(y)?;
            writeln!(
                y,
                r#"echo "{id}: $(tput setaf 2)start sync config$(tput sgr0)""#,
            )?;
            writeln!(
                y,
                "rsync -azh -e \"ssh {ssh_extra_args}\" ./ ubuntu@{public_ip}:{base_folder} --exclude 'deploy.sh' --exclude '*.partial.sh'",
            )?;
            writeln!(
                y,
                "scp {ssh_extra_args} ./{host}.yml ubuntu@{public_ip}:{base_folder}/docker-compose.yaml"
            )?;
            writeln!(
                y,
                r#"echo "{id}: $(tput setaf 2)done sync config$(tput sgr0)""#,
            )?;
            let sh = format!("_deploy.{id}.partial.sh");
            std::fs::write(Path::new(output_directory).join(&sh), y)?;
            writeln!(x, "{sh}")?;
        }
        writeln!(x, "EOF")?;
        writeln!(x, r#"fi"#)?;
        writeln!(x)?;
        writeln!(x, r#"if [[ "$DO_TEAR_DOWN" -eq 1 ]]; then"#)?;
        writeln!(x, "# --- Tear Down Services ---")?;
        writeln!(
            x,
            r#"echo "$(tput setaf 2)(2/4) stop services and pull latest image$(tput sgr0)""#,
        )?;
        writeln!(x, "parallel --linebuffer bash << EOF")?;
        for instance in ec2_instances {
            let id = &instance.id;
            let mut y = String::new();
            writeln!(y, "#!/bin/bash -e")?;
            writeln!(
                y,
                r#"echo "{id}: $(tput setaf 2)stopping and pulling$(tput sgr0)""#,
            )?;
            let public_ip = &instance.public_ip;
            let base_folder = "~/risingwave-deploy";
            let tear_down_volumes = instance.r#type == "source" || instance.r#type == "compute";
            let down_extra_arg = if tear_down_volumes {
                // tear down volumes for source and compute
                " -v"
            } else {
                ""
            };
            writeln!(y, "ssh {ssh_extra_args} ubuntu@{public_ip} \"bash -c 'cd {base_folder} && docker compose kill && docker compose down --remove-orphans{down_extra_arg} && docker pull {}'\"",
                compose_config.image.risingwave
            )?;
            if tear_down_volumes {
                writeln!(
                    y,
                    r#"echo "{id}: $(tput setaf 2)done tear down (along with volumes)$(tput sgr0)""#
                )?;
            } else {
                writeln!(
                    y,
                    r#"echo "{id}: $(tput setaf 2)done tear down$(tput sgr0)""#
                )?;
            }

            let sh = format!("_stop.{id}.partial.sh");
            std::fs::write(Path::new(output_directory).join(&sh), y)?;
            writeln!(x, "{sh}")?;
        }
        writeln!(x, "EOF")?;
        writeln!(x, r#"fi"#)?;
        writeln!(x)?;
        writeln!(x, r#"if [[ "$DO_START" -eq 1 ]]; then"#)?;
        writeln!(x, "# --- Start Services ---")?;
        writeln!(
            x,
            r#"echo "$(tput setaf 2)(3/4) start services$(tput sgr0)""#,
        )?;
        for step in steps {
            let dns_host = if let Some(dns_host) = service_on_node.get(step) {
                dns_host
            } else {
                // pseudo-services like s3, skip.
                continue;
            };
            let instance = ec2_instances
                .iter()
                .find(|ec2| &ec2.dns_host == dns_host)
                .unwrap();
            let id = &instance.id;
            writeln!(
                x,
                r#"echo "{id}: $(tput setaf 2)start service {step}$(tput sgr0)""#,
            )?;
            let public_ip = &instance.public_ip;
            let base_folder = "~/risingwave-deploy";
            writeln!(x, "ssh {ssh_extra_args} ubuntu@{public_ip} \"bash -c 'cd {base_folder} && docker compose up -d {step}'\"")?;
        }
        writeln!(x, r#"fi"#)?;
        writeln!(x)?;
        writeln!(x, r#"if [[ "$DO_CHECK" -eq 1 ]]; then"#)?;
        writeln!(x, "# --- Check Services ---")?;
        writeln!(
            x,
            r#"echo "$(tput setaf 2)(4/4) check service started$(tput sgr0)""#
        )?;
        writeln!(x, "parallel --linebuffer bash << EOF")?;
        for instance in ec2_instances {
            let id = &instance.id;
            let mut y = String::new();
            writeln!(y, "#!/bin/bash -e")?;
            writeln!(y, r#"echo "{id}: $(tput setaf 2)check status$(tput sgr0)""#,)?;
            let public_ip = &instance.public_ip;
            let base_folder = "~/risingwave-deploy";
            writeln!(y, "ssh {ssh_extra_args} ubuntu@{public_ip} \"bash -c 'cd {base_folder} && docker compose ps'\"")?;

            let sh = format!("_check.{id}.partial.sh");
            std::fs::write(Path::new(output_directory).join(&sh), y)?;
            writeln!(x, "{sh}")?;
        }
        writeln!(x, "EOF")?;
        writeln!(x, r#"fi"#)?;
        x
    };
    let deploy_sh = Path::new(output_directory).join("deploy.sh");
    fs::write(&deploy_sh, &shell_script)?;
    let mut perms = fs::metadata(&deploy_sh)?.permissions();
    perms.set_mode(perms.mode() | 0o755);
    fs::set_permissions(&deploy_sh, perms)?;
    Ok(())
}
