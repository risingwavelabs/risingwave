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

use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::anyhow;
use risingwave_pb::common::HostAddress as ProstHostAddress;

/// General host address and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddr {
    pub host: String,
    pub port: u16,
}

impl std::fmt::Display for HostAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
impl From<SocketAddr> for HostAddr {
    fn from(addr: SocketAddr) -> Self {
        HostAddr {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}

impl TryFrom<&str> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let addr =
            url::Url::parse(&format!("http://{}", s)).map_err(|e| anyhow!("{}: {}", e, s))?;
        Ok(HostAddr {
            host: addr
                .host()
                .ok_or_else(|| anyhow!("invalid host"))?
                .to_string(),
            port: addr.port().ok_or_else(|| anyhow!("invalid port"))?,
        })
    }
}

impl TryFrom<&String> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl FromStr for HostAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl From<&ProstHostAddress> for HostAddr {
    fn from(addr: &ProstHostAddress) -> Self {
        HostAddr {
            host: addr.get_host().to_string(),
            port: addr.get_port() as u16,
        }
    }
}

impl HostAddr {
    pub fn to_protobuf(&self) -> ProstHostAddress {
        ProstHostAddress {
            host: self.host.clone(),
            port: self.port as i32,
        }
    }
}

pub fn is_local_address(server_addr: &HostAddr, peer_addr: &HostAddr) -> bool {
    server_addr == peer_addr
}

#[cfg(test)]
mod tests {
    use crate::util::addr::{is_local_address, HostAddr};

    #[test]
    fn test_is_local_address() {
        let check_local = |a: &str, b: &str, result: bool| {
            assert_eq!(
                is_local_address(&a.parse().unwrap(), &b.parse().unwrap()),
                result
            );
        };
        check_local("localhost:3456", "localhost:3456", true);
        check_local("10.11.12.13:3456", "10.11.12.13:3456", true);
        check_local("some.host.in.k8s:3456", "some.host.in.k8s:3456", true);
        check_local("some.host.in.k8s:3456", "other.host.in.k8s:3456", false);
        check_local("some.host.in.k8s:3456", "some.host.in.k8s:4567", false);
    }

    #[test]
    fn test_host_addr_convert() {
        let addr = "1.2.3.4:567";
        assert_eq!(
            addr.parse::<HostAddr>().unwrap(),
            HostAddr {
                host: String::from("1.2.3.4"),
                port: 567
            }
        );
        let addr = "test.test:12345";
        assert_eq!(
            addr.parse::<HostAddr>().unwrap(),
            HostAddr {
                host: String::from("test.test"),
                port: 12345
            }
        );
        let addr = "test.test";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:65537";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:12345:12345";
        assert!(addr.parse::<HostAddr>().is_err());
    }
}
