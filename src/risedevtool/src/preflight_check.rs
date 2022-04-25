use std::env;
use std::process::Command;

use anyhow::Result;
use console::style;

fn preflight_check_proxy() -> Result<()> {
    if env::var("http_proxy").is_ok()
        || env::var("https_proxy").is_ok()
        || env::var("HTTP_PROXY").is_ok()
        || env::var("HTTPS_PROXY").is_ok()
        || env::var("all_proxy").is_ok()
        || env::var("ALL_PROXY").is_ok()
    {
        if let Ok(x) = env::var("no_proxy") && x.contains("127.0.0.1") && x.contains("::1") {
            println!(
                "[{}] {} - You are using proxies for all RisingWave components. Please make sure that `no_proxy` is set for all worker nodes within the cluster.",
                style("risedev-preflight-check").bold(),
                style("INFO").green().bold()
            );
        } else {
            println!(
                "[{}] {} - `no_proxy` is not set correctly, which might cause failure in RiseDev and RisingWave. Consider {}.",
                style("risedev-preflight-check").bold(),
                style("WARN").yellow().bold(),
                style("`export no_proxy=localhost,127.0.0.1,::1`").blue().bold()
            );
        }
    }

    Ok(())
}

fn preflight_check_ulimit() -> Result<()> {
    let ulimit = Command::new("sh")
        .args(["-c", "ulimit -n"])
        .output()?
        .stdout;
    let ulimit = String::from_utf8(ulimit)?;
    let ulimit: usize = ulimit.trim().parse()?;
    if ulimit < 8192 {
        println!(
            "[{}] {} - ulimit for file handler is too low (currently {}). If you meet too many open files error, considering changing the ulimit.",
            style("risedev-preflight-check").bold(),
            style("WARN").yellow().bold(),
            ulimit
        );
    }
    Ok(())
}

pub fn preflight_check() -> Result<()> {
    if let Err(e) = preflight_check_proxy() {
        println!(
            "[{}] {} - failed to run proxy preflight check: {}",
            style("risedev-preflight-check").bold(),
            style("WARN").yellow().bold(),
            e
        );
    }

    if let Err(e) = preflight_check_ulimit() {
        println!(
            "[{}] {} - failed to run ulimit preflight check: {}",
            style("risedev-preflight-check").bold(),
            style("WARN").yellow().bold(),
            e
        );
    }

    Ok(())
}
