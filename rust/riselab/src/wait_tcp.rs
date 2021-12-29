use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::anyhow;

pub fn wait_tcp(
    server: impl AsRef<str>,
    f: &mut impl std::io::Write,
    p: impl AsRef<Path>,
    id: &str,
) -> anyhow::Result<()> {
    let server = server.as_ref();
    let p = p.as_ref();
    let addr = server.parse()?;
    let mut remaining_retries = 30;

    writeln!(f, "Waiting for online: {}", server)?;

    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                writeln!(
                    f,
                    "Retrying connecting to {}, {:?}, {} trials remaining",
                    server, err, remaining_retries
                )?;
            }
        }
        remaining_retries -= 1;
        if remaining_retries == 0 {
            return Err(anyhow!("failed to connect"));
        }

        if p.exists() {
            let mut buf = String::new();
            std::fs::File::open(p)?.read_to_string(&mut buf)?;
            return Err(anyhow!(
                "{} exited while waiting for connection: {}",
                id,
                buf
            ));
        }

        sleep(Duration::from_secs(1));
    }
}
