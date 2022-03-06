use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::anyhow;
use console::style;
use isahc::prelude::*;
use isahc::{Body, Request};

pub fn wait_tcp(
    server: impl AsRef<str>,
    f: &mut impl std::io::Write,
    p: impl AsRef<Path>,
    id: &str,
    timeout: Option<std::time::Duration>,
    detect_failure: bool,
) -> anyhow::Result<()> {
    let server = server.as_ref();
    let p = p.as_ref();
    let addr = server.parse()?;
    let start_time = std::time::Instant::now();

    writeln!(f, "Waiting for online: {}", server)?;

    let mut last_error;

    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
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
            std::fs::File::open(p)?.read_to_string(&mut buf)?;

            return Err(anyhow!(
                "{} exited while waiting for connection: {}",
                style(id).red().bold(),
                buf,
            ));
        }

        sleep(Duration::from_millis(30));
    }
}

pub fn wait_http_with_cb(
    server: impl AsRef<str>,
    f: &mut impl std::io::Write,
    p: impl AsRef<Path>,
    id: &str,
    timeout: Option<std::time::Duration>,
    detect_failure: bool,
    resp_cb: impl Fn(Body) -> bool,
) -> anyhow::Result<()> {
    let server = server.as_ref();
    let p = p.as_ref();
    let start_time = std::time::Instant::now();

    writeln!(f, "Waiting for online: {}", server)?;

    let mut last_error;

    loop {
        match Request::get(server)
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(1))
            .body("")
            .unwrap()
            .send()
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    let body = resp.into_body();
                    if resp_cb(body) {
                        return Ok(());
                    }
                    last_error = Some(anyhow!("health check callback failed."))
                } else {
                    last_error = Some(anyhow!("http failed with status: {}", resp.status()));
                }
            }
            Err(err) => {
                last_error = Some(err.into());
            }
        }

        if let Some(ref timeout) = timeout {
            if std::time::Instant::now() - start_time >= *timeout {
                return Err(anyhow!("failed to connect, last error: {:?}", last_error));
            }
        }

        if detect_failure && p.exists() {
            let mut buf = String::new();
            std::fs::File::open(p)?.read_to_string(&mut buf)?;
            return Err(anyhow!(
                "{} exited while waiting for connection: {}",
                id,
                buf
            ));
        }

        sleep(Duration::from_millis(30));
    }
}

// TODO: unify this function with `wait_tcp`.
pub fn wait_http(
    server: impl AsRef<str>,
    f: &mut impl std::io::Write,
    p: impl AsRef<Path>,
    id: &str,
    timeout: Option<std::time::Duration>,
    detect_failure: bool,
) -> anyhow::Result<()> {
    wait_http_with_cb(server, f, p, id, timeout, detect_failure, |_| true)
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
