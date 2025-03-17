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

use std::fmt::{self, Formatter};
use std::str::FromStr;

use itertools::Itertools;

const META_ADDRESS_LOAD_BALANCE_MODE_PREFIX: &str = "load-balance+";

/// The strategy for meta client to connect to meta node.
///
/// Used in the command line argument `--meta-address`.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum MetaAddressStrategy {
    LoadBalance(http::Uri),
    List(Vec<http::Uri>),
}

/// Error type for parsing meta address strategy.
#[derive(thiserror::Error, Debug, thiserror_ext::ContextInto)]
pub enum MetaAddressStrategyParseError {
    #[error("empty meta addresses")]
    Empty,
    #[error("there should be only one load balance address")]
    MultipleLoadBalance,
    #[error("failed to parse meta address `{1}`: {0}")]
    UrlParse(#[source] http::uri::InvalidUri, String),
}

impl FromStr for MetaAddressStrategy {
    type Err = MetaAddressStrategyParseError;

    fn from_str(meta_addr: &str) -> Result<Self, Self::Err> {
        if let Some(addr) = meta_addr.strip_prefix(META_ADDRESS_LOAD_BALANCE_MODE_PREFIX) {
            let addr = addr
                .split(',')
                .exactly_one()
                .map_err(|_| MetaAddressStrategyParseError::MultipleLoadBalance)?;

            let uri = addr.parse().into_url_parse(addr)?;

            Ok(Self::LoadBalance(uri))
        } else {
            let addrs = meta_addr.split(',').peekable();

            let uris: Vec<_> = addrs
                .map(|addr| addr.parse().into_url_parse(addr))
                .try_collect()?;

            if uris.is_empty() {
                return Err(MetaAddressStrategyParseError::Empty);
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

impl MetaAddressStrategy {
    /// Returns `Some` if there's exactly one address.
    pub fn exactly_one(&self) -> Option<&http::Uri> {
        match self {
            MetaAddressStrategy::LoadBalance(lb) => Some(lb),
            MetaAddressStrategy::List(list) => {
                if list.len() == 1 {
                    list.first()
                } else {
                    None
                }
            }
        }
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
            ("", None),
            (
                "http://abc",
                Some(MetaAddressStrategy::List(vec![
                    "http://abc".parse().unwrap(),
                ])),
            ),
            (
                "http://abc,http://def",
                Some(MetaAddressStrategy::List(vec![
                    "http://abc".parse().unwrap(),
                    "http://def".parse().unwrap(),
                ])),
            ),
        ];
        for (addr, result) in results {
            let parsed_result = addr.parse();
            match result {
                None => {
                    assert!(parsed_result.is_err(), "{parsed_result:?}");
                }
                Some(strategy) => {
                    assert_eq!(strategy, parsed_result.unwrap());
                }
            }
        }
    }
}
