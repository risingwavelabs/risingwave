use std::fmt::{self, Formatter};
use std::str::FromStr;

use itertools::Itertools;

const META_ADDRESS_LOAD_BALANCE_MODE_PREFIX: &'static str = "load-balance+";

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum MetaAddressStrategy {
    LoadBalance(http::Uri),
    List(Vec<http::Uri>),
}

#[derive(thiserror::Error, Debug, thiserror_ext::Construct)]
pub enum MetaAddressParseError {
    #[error("empty meta addresses")]
    Empty,
    #[error("there should be only one load balance address")]
    MultipleLoadBalance,
    #[error("failed to parse meta address `{1}`: {0}")]
    UrlParse(#[source] http::uri::InvalidUri, String),
}

impl FromStr for MetaAddressStrategy {
    type Err = MetaAddressParseError;

    fn from_str(meta_addr: &str) -> Result<Self, Self::Err> {
        if let Some(addr) = meta_addr.strip_prefix(META_ADDRESS_LOAD_BALANCE_MODE_PREFIX) {
            let addr = addr
                .split(',')
                .exactly_one()
                .map_err(|_| MetaAddressParseError::MultipleLoadBalance)?;

            let uri = addr
                .parse()
                .map_err(|e| MetaAddressParseError::url_parse(e, addr))?;

            Ok(Self::LoadBalance(uri))
        } else {
            let addrs = meta_addr.split(',').peekable();

            let uris: Vec<_> = addrs
                .map(|addr| {
                    addr.parse()
                        .map_err(|e| MetaAddressParseError::url_parse(e, addr))
                })
                .try_collect()?;

            if uris.is_empty() {
                return Err(MetaAddressParseError::Empty);
            }

            Ok(Self::List(uris))
        }
    }
}

impl fmt::Display for MetaAddressStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MetaAddressStrategy::LoadBalance(addr) => {
                write!(f, "{}{}", META_ADDRESS_LOAD_BALANCE_MODE_PREFIX, addr)?;
            }
            MetaAddressStrategy::List(addrs) => {
                write!(f, "{}", addrs.iter().format(","))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meta_addr() {
        let results = vec![
            (
                "load-balance+http://abc",
                Some(MetaAddressStrategy::LoadBalance(
                    "http://abc".parse().unwrap(),
                )),
            ),
            ("load-balance+http://abc,http://def", None),
            ("load-balance+http://abc:xxx", None),
            ("", None),
            (
                "http://abc,http://def",
                Some(MetaAddressStrategy::List(vec![
                    "http://abc".parse().unwrap(),
                    "http://def".parse().unwrap(),
                ])),
            ),
            ("http://abc:xx,http://def", None),
        ];
        for (addr, result) in results {
            let parsed_result = addr.parse();
            match result {
                None => {
                    assert!(parsed_result.is_err());
                }
                Some(strategy) => {
                    assert_eq!(strategy, parsed_result.unwrap());
                    assert_eq!(addr, strategy.to_string());
                }
            }
        }
    }
}
