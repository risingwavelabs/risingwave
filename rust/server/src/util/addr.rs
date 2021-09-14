use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::error::RwError;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;

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
    use crate::util::addr::get_host_port;

    #[test]
    fn test_get_host_port() {
        let addr = get_host_port("127.0.0.1:5688").unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1".to_string());
        assert_eq!(addr.port(), 5688);
    }
}
