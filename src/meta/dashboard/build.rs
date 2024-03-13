use std::path::{Path, PathBuf};

use cargo_emit::{rerun_if_changed, rustc_cfg};
use npm_rs::{NodeEnv, NpmEnv};

fn env_var_is_true(key: &str) -> bool {
    cargo_emit::rerun_if_env_changed!(key);

    std::env::var(key)
        .map(|value| {
            ["1", "t", "true"]
                .iter()
                .any(|&s| value.eq_ignore_ascii_case(s))
        })
        .unwrap_or(false)
}

const DASHBOARD_DIR: &str = "../../../dashboard";

fn dest_dir() -> PathBuf {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    Path::new(&out_dir).join("assets")
}

fn build() -> anyhow::Result<()> {
    rerun_if_changed!(format!("{DASHBOARD_DIR}/components")); // TODO

    let exit_status = NpmEnv::default()
        .with_node_env(&NodeEnv::Production)
        .set_path(DASHBOARD_DIR)
        .init_env()
        .install(None)
        .run("build")
        .exec()?;

    if !exit_status.success() {
        anyhow::bail!("dashboard build failed with status: {}", exit_status);
    }

    let dest = dest_dir();
    let src = Path::new(DASHBOARD_DIR).join("out");
    dircpy::copy_dir(&src, &dest)?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let should_build = env_var_is_true("ENABLE_BUILD_DASHBOARD") // asked to build by RiseDev
        || (std::env::var("PROFILE").unwrap() == "release" && !env_var_is_true("RISINGWAVE_CI")); // release build and not in CI

    if should_build {
        build()?;
        rustc_cfg!("dashboard_built");
    } else {
        std::fs::create_dir_all(dest_dir())?;
    }

    Ok(())
}
