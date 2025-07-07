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

use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use console::style;

pub fn wait(
    mut wait_func: impl FnMut() -> Result<()>,
    f: &mut impl std::io::Write,
    p: impl AsRef<Path>,
    id: &str,
    timeout: Option<std::time::Duration>,
    detect_failure: bool,
) -> anyhow::Result<()> {
    let p = p.as_ref();
    let start_time = std::time::Instant::now();

    writeln!(f, "Waiting for online")?;

    let mut last_error;

    loop {
        match wait_func() {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                last_error = Some(err);
            }
        }

        if let Some(ref timeout) = timeout
            && std::time::Instant::now() - start_time >= *timeout
        {
            let context = "timeout when trying to connect";

            return Err(if let Some(last_error) = last_error {
                last_error.context(context)
            } else {
                anyhow!(context)
            });
        }

        if detect_failure && p.exists() {
            let mut buf = String::new();
            fs_err::File::open(p)?.read_to_string(&mut buf)?;

            let context = format!(
                "{} exited while waiting for connection: {}",
                style(id).red().bold(),
                buf.trim(),
            );

            return Err(if let Some(last_error) = last_error {
                last_error.context(context)
            } else {
                anyhow!(context)
            });
        }

        sleep(Duration::from_millis(100));
    }
}

pub fn wait_tcp_available(
    server: impl AsRef<str>,
    timeout: Option<std::time::Duration>,
) -> anyhow::Result<()> {
    let server = server.as_ref();
    let addr = server
        .to_socket_addrs()?
        .next()
        .with_context(|| format!("failed to resolve {}", server))?;
    let start_time = std::time::Instant::now();

    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
            Ok(_) => {}
            Err(_) => {
                return Ok(());
            }
        }

        if let Some(ref timeout) = timeout
            && std::time::Instant::now() - start_time >= *timeout
        {
            return Err(anyhow!(
                "Failed to wait for port closing on {}. The port may still be in use by another process or application. Please ensure the port is not being used elsewhere and try again.",
                server
            ));
        }

        sleep(Duration::from_millis(100));
    }
}
