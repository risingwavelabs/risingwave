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

use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Result};
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

        if let Some(ref timeout) = timeout {
            if std::time::Instant::now() - start_time >= *timeout {
                return Err(anyhow!("failed to connect, last error: {:?}", last_error));
            }
        }

        if detect_failure && p.exists() {
            let mut buf = String::new();
            fs_err::File::open(p)?.read_to_string(&mut buf)?;

            return Err(anyhow!(
                "{} exited while waiting for connection: {}",
                style(id).red().bold(),
                buf,
            ));
        }

        sleep(Duration::from_millis(30));
    }
}

pub fn wait_tcp_available(
    server: impl AsRef<str>,
    timeout: Option<std::time::Duration>,
) -> anyhow::Result<()> {
    let server = server.as_ref();
    let addr = server.parse()?;
    let start_time = std::time::Instant::now();

    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
            Ok(_) => {}
            Err(_) => {
                return Ok(());
            }
        }

        if let Some(ref timeout) = timeout {
            if std::time::Instant::now() - start_time >= *timeout {
                return Err(anyhow!("failed to wait for closing"));
            }
        }

        sleep(Duration::from_millis(50));
    }
}
