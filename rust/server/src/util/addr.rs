use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::error::RwError;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;

pub fn is_local_address(server_addr: &SocketAddr, peer_addr: &SocketAddr) -> bool {
    let peer_ip = peer_addr.ip();
    if peer_ip.is_loopback() || peer_ip.is_unspecified() || (peer_addr.ip() == server_addr.ip()) {
        return peer_addr.port() == server_addr.port();
    }
    false
}

pub fn get_host_port(addr: &str) -> Result<SocketAddr> {
    if let Ok(a) = SocketAddrV4::from_str(addr) {
        return Ok(SocketAddr::V4(a));
    }
    SocketAddrV6::from_str(addr)
        .map(SocketAddr::V6)
        .map_err(|e| RwError::from(InternalError(format!("failed to resolve address: {}", e))))
}

#[cfg(test)]
mod tests {
    use crate::util::addr::{get_host_port, is_local_address};

    #[test]
    fn test_get_host_port() {
        let addr = get_host_port("127.0.0.1:5688").unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1".to_string());
        assert_eq!(addr.port(), 5688);
    }

    #[test]
    fn test_is_local_address() {
        let check_local = |a: &str, b: &str| {
            assert!(is_local_address(
                &get_host_port(a).unwrap(),
                &get_host_port(b).unwrap()
            ));
        };
        check_local("127.0.0.1:3456", "0.0.0.0:3456");
        check_local("10.11.12.13:3456", "10.11.12.13:3456");
        check_local("10.11.12.13:3456", "0.0.0.0:3456");
        check_local("10.11.12.13:3456", "127.0.0.1:3456");
    }
}
