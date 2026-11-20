// Copyright 2026 RisingWave Labs
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

//! A per-URL cache of the JWK Sets that OAuth login verifies tokens against.
//!
//! Without it, every OAuth login fetches the JWK Set from the identity provider before the
//! token is checked. The cache:
//!
//! - keeps each set for [`JWKS_TTL`] after a successful fetch;
//! - refetches early when a token names a `kid` the cached set does not have (key rotation),
//!   at most once per [`JWKS_MIN_REFETCH_INTERVAL`] per URL, so tokens with unknown `kid`s
//!   cannot turn into a stream of fetches;
//! - keeps serving the last good set, for up to [`JWKS_MAX_STALENESS`] after it was fetched,
//!   when a refetch fails, so a brief outage of the JWKS endpoint does not fail logins whose
//!   keys have not changed;
//! - lets one fetch per URL run at a time; concurrent logins for the same URL wait for it
//!   and share its result.
//!
//! Token validation itself (issuer, audience, expiry, algorithm allow-list) is unchanged and
//! runs on every login.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use crate::pg_server::{BoxedError, Jwks};

/// How long a fetched JWK Set is used before it is fetched again.
pub(crate) const JWKS_TTL: Duration = Duration::from_secs(5 * 60);
/// The least time between two fetch attempts for one URL, other than the first.
pub(crate) const JWKS_MIN_REFETCH_INTERVAL: Duration = Duration::from_secs(30);
/// How long after its fetch a JWK Set may still be served while refetching it fails.
pub(crate) const JWKS_MAX_STALENESS: Duration = Duration::from_secs(60 * 60);
/// The timeout of one JWKS HTTP request, so a hanging endpoint cannot hold logins forever.
pub(crate) const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

// `JwksCache::get` relies on the refetch interval being shorter than the TTL; checked at
// compile time for the production durations.
const _: () = assert!(JWKS_MIN_REFETCH_INTERVAL.as_nanos() < JWKS_TTL.as_nanos());

/// Fetches the JWK Set at a URL.
pub(crate) type JwksFetcher =
    Box<dyn Fn(String) -> BoxFuture<'static, Result<Jwks, BoxedError>> + Send + Sync>;

/// The production fetcher: an HTTP GET with a timeout, which fails on a non-success status.
///
/// One HTTP client is built here and shared by every fetch, so fetches reuse its connection
/// pool. If building it fails, every fetch returns that error.
pub(crate) fn http_jwks_fetcher() -> JwksFetcher {
    let client = reqwest::Client::builder()
        .timeout(JWKS_FETCH_TIMEOUT)
        .build()
        .map_err(Arc::new);
    Box::new(move |url| {
        let client = client.clone();
        Box::pin(async move {
            let jwks = client?
                .get(&url)
                .send()
                .await?
                .error_for_status()?
                .json::<Jwks>()
                .await?;
            Ok(jwks)
        })
    })
}

struct Cached {
    jwks: Arc<Jwks>,
    fetched_at: Instant,
}

#[derive(Default)]
struct Slot {
    cached: Option<Cached>,
    /// When the last fetch that finished was started, successful or not. It is recorded only
    /// once the fetch returns, so a fetch whose login was dropped midway does not count.
    last_attempt: Option<Instant>,
}

pub(crate) struct JwksCache {
    fetch: JwksFetcher,
    ttl: Duration,
    min_refetch_interval: Duration,
    max_staleness: Duration,
    /// One slot per URL. The async mutex is held across a fetch, which is what makes
    /// concurrent logins for one URL share a single fetch.
    slots: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Slot>>>>,
}

impl JwksCache {
    pub(crate) fn new(fetch: JwksFetcher) -> Self {
        Self::with_durations(
            fetch,
            JWKS_TTL,
            JWKS_MIN_REFETCH_INTERVAL,
            JWKS_MAX_STALENESS,
        )
    }

    pub(crate) fn with_durations(
        fetch: JwksFetcher,
        ttl: Duration,
        min_refetch_interval: Duration,
        max_staleness: Duration,
    ) -> Self {
        // `get` relies on this: when the last attempt was within the refetch interval and the
        // set is nevertheless expired, that attempt must have failed.
        assert!(
            min_refetch_interval < ttl,
            "the JWKS refetch interval must be shorter than its TTL"
        );
        Self {
            fetch,
            ttl,
            min_refetch_interval,
            max_staleness,
            slots: Default::default(),
        }
    }

    fn slot(&self, url: &str) -> Arc<tokio::sync::Mutex<Slot>> {
        self.slots.lock().entry(url.to_owned()).or_default().clone()
    }

    /// Returns the JWK Set for `url`, fetching it when there is none or it is older than the
    /// TTL. `now` is taken by the caller, so tests control the clock.
    pub(crate) async fn get(&self, url: &str, now: Instant) -> Result<Arc<Jwks>, BoxedError> {
        let slot = self.slot(url);
        let mut slot = slot.lock().await;
        if let Some(cached) = &slot.cached
            && now.saturating_duration_since(cached.fetched_at) < self.ttl
        {
            return Ok(cached.jwks.clone());
        }
        if let Some(last) = slot.last_attempt
            && now.saturating_duration_since(last) < self.min_refetch_interval
        {
            // The set is missing or expired although a fetch was just attempted, so that
            // attempt failed. Do not retry it on every login.
            return self.serve_stale(&slot, now).ok_or_else(|| {
                format!("fetching the JWKS from {url} failed recently; not retrying yet").into()
            });
        }
        self.refresh(&mut slot, url, now).await
    }

    /// Refetches the JWK Set for `url` because a token named a `kid` it does not have, unless
    /// a fetch for `url` was attempted within the refetch interval. Returns the set to verify
    /// against: the new one, or, when rate-limited, the current one (which a concurrent
    /// login may just have refreshed).
    pub(crate) async fn refetch_for_unknown_kid(
        &self,
        url: &str,
        now: Instant,
    ) -> Result<Arc<Jwks>, BoxedError> {
        let slot = self.slot(url);
        let mut slot = slot.lock().await;
        if let Some(last) = slot.last_attempt
            && now.saturating_duration_since(last) < self.min_refetch_interval
        {
            return self.serve_stale(&slot, now).ok_or_else(|| {
                format!("fetching the JWKS from {url} failed recently; not retrying yet").into()
            });
        }
        self.refresh(&mut slot, url, now).await
    }

    async fn refresh(
        &self,
        slot: &mut Slot,
        url: &str,
        now: Instant,
    ) -> Result<Arc<Jwks>, BoxedError> {
        let fetched = (self.fetch)(url.to_owned()).await;
        // Recorded after the fetch returns, not before it: if this future is dropped while
        // fetching, no attempt is recorded, and the next login fetches instead of reporting a
        // failure that never happened.
        slot.last_attempt = Some(now);
        match fetched {
            Ok(jwks) => {
                let jwks = Arc::new(jwks);
                slot.cached = Some(Cached {
                    jwks: jwks.clone(),
                    fetched_at: now,
                });
                Ok(jwks)
            }
            Err(error) => match self.serve_stale(slot, now) {
                Some(stale) => {
                    tracing::warn!(
                        url,
                        error = %error,
                        "failed to refresh the JWKS; using the last fetched set",
                    );
                    Ok(stale)
                }
                None => Err(error),
            },
        }
    }

    /// The cached set, if it was fetched within the maximum staleness.
    fn serve_stale(&self, slot: &Slot, now: Instant) -> Option<Arc<Jwks>> {
        slot.cached
            .as_ref()
            .filter(|cached| now.saturating_duration_since(cached.fetched_at) < self.max_staleness)
            .map(|cached| cached.jwks.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::FutureExt;

    use super::*;
    use crate::pg_server::Jwk;

    fn jwks_with_kids(kids: &[&str]) -> Jwks {
        Jwks {
            keys: kids
                .iter()
                .map(|kid| Jwk {
                    kty: Some("RSA".to_owned()),
                    kid: Some((*kid).to_owned()),
                    alg: None,
                    n: Some("n".to_owned()),
                    e: Some("AQAB".to_owned()),
                })
                .collect(),
        }
    }

    fn kids(jwks: &Jwks) -> Vec<String> {
        jwks.keys.iter().filter_map(|k| k.kid.clone()).collect()
    }

    /// A fetcher that answers from a script, one entry per fetch, and counts its calls.
    /// `Ok(kids)` serves a set with those kids; `Err(())` fails the fetch.
    fn scripted(script: Vec<Result<Vec<&'static str>, ()>>) -> (JwksFetcher, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let script = Arc::new(parking_lot::Mutex::new(VecDeque::from(script)));
        let counter = calls.clone();
        let fetcher: JwksFetcher = Box::new(move |_url| {
            counter.fetch_add(1, Ordering::SeqCst);
            let next = script.lock().pop_front().expect("unexpected JWKS fetch");
            Box::pin(async move {
                // Yield once, so concurrent callers really do overlap with the fetch.
                tokio::task::yield_now().await;
                match next {
                    Ok(kids) => Ok(jwks_with_kids(&kids)),
                    Err(()) => Err("injected fetch failure".into()),
                }
            })
        });
        (fetcher, calls)
    }

    const TTL: Duration = Duration::from_secs(300);
    const INTERVAL: Duration = Duration::from_secs(30);
    const STALE: Duration = Duration::from_secs(3600);
    const URL: &str = "https://idp.example/jwks";

    fn cache(script: Vec<Result<Vec<&'static str>, ()>>) -> (JwksCache, Arc<AtomicUsize>) {
        let (fetcher, calls) = scripted(script);
        (
            JwksCache::with_durations(fetcher, TTL, INTERVAL, STALE),
            calls,
        )
    }

    #[tokio::test]
    async fn test_jwks_is_reused_within_ttl() {
        let (cache, calls) = cache(vec![Ok(vec!["a"])]);
        let t0 = Instant::now();
        cache.get(URL, t0).await.unwrap();
        let again = cache
            .get(URL, t0 + TTL - Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(kids(&again), ["a"]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_jwks_is_refetched_after_ttl() {
        let (cache, calls) = cache(vec![Ok(vec!["a"]), Ok(vec!["b"])]);
        let t0 = Instant::now();
        cache.get(URL, t0).await.unwrap();
        let later = cache.get(URL, t0 + TTL).await.unwrap();
        assert_eq!(kids(&later), ["b"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_each_url_is_cached_separately() {
        let (cache, calls) = cache(vec![Ok(vec!["a"]), Ok(vec!["b"])]);
        let t0 = Instant::now();
        assert_eq!(kids(&cache.get(URL, t0).await.unwrap()), ["a"]);
        assert_eq!(
            kids(&cache.get("https://other.example/jwks", t0).await.unwrap()),
            ["b"]
        );
        assert_eq!(kids(&cache.get(URL, t0).await.unwrap()), ["a"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_unknown_kid_refetch_is_rate_limited() {
        let (cache, calls) = cache(vec![Ok(vec!["a"]), Ok(vec!["a", "b"])]);
        let t0 = Instant::now();
        cache.get(URL, t0).await.unwrap();

        // Within the interval of the first fetch: no fetch, the current set is returned.
        let limited = cache
            .refetch_for_unknown_kid(URL, t0 + INTERVAL / 2)
            .await
            .unwrap();
        assert_eq!(kids(&limited), ["a"]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // After it: the rotated set is fetched, before the TTL expires.
        let rotated = cache
            .refetch_for_unknown_kid(URL, t0 + INTERVAL)
            .await
            .unwrap();
        assert_eq!(kids(&rotated), ["a", "b"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // And a burst of unknown kids right after does not fetch again.
        for _ in 0..10 {
            cache
                .refetch_for_unknown_kid(URL, t0 + INTERVAL + Duration::from_secs(1))
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_last_good_set_is_served_when_refetch_fails() {
        let (cache, calls) = cache(vec![Ok(vec!["a"]), Err(())]);
        let t0 = Instant::now();
        cache.get(URL, t0).await.unwrap();
        let stale = cache.get(URL, t0 + TTL).await.unwrap();
        assert_eq!(kids(&stale), ["a"]);
        // The failed attempt is rate-limited too: logins right after it do not refetch.
        cache
            .get(URL, t0 + TTL + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_stale_set_is_not_served_past_max_staleness() {
        let (cache, _calls) = cache(vec![Ok(vec!["a"]), Err(())]);
        let t0 = Instant::now();
        cache.get(URL, t0).await.unwrap();
        let err = cache.get(URL, t0 + STALE).await.unwrap_err();
        assert!(err.to_string().contains("injected fetch failure"), "{err}");
    }

    #[tokio::test]
    async fn test_failed_first_fetch_is_an_error_and_is_not_retried_immediately() {
        let (cache, calls) = cache(vec![Err(()), Ok(vec!["a"])]);
        let t0 = Instant::now();
        let err = cache.get(URL, t0).await.unwrap_err();
        assert!(err.to_string().contains("injected fetch failure"), "{err}");

        let err = cache.get(URL, t0 + INTERVAL / 2).await.unwrap_err();
        assert!(err.to_string().contains("not retrying yet"), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert_eq!(kids(&cache.get(URL, t0 + INTERVAL).await.unwrap()), ["a"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_a_fetch_dropped_midway_is_not_counted_as_a_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        // The first fetch never completes; the second serves a set.
        let fetcher: JwksFetcher = Box::new(move |_url| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                futures::future::pending().boxed()
            } else {
                async { Ok(jwks_with_kids(&["a"])) }.boxed()
            }
        });
        let cache = JwksCache::with_durations(fetcher, TTL, INTERVAL, STALE);
        let t0 = Instant::now();

        // A login starts the first fetch and is dropped while it is in flight.
        assert!(cache.get(URL, t0).now_or_never().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // The next login, within the refetch interval, fetches rather than failing.
        let jwks = cache.get(URL, t0 + INTERVAL / 2).await.unwrap();
        assert_eq!(kids(&jwks), ["a"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_concurrent_logins_share_one_fetch() {
        let (cache, calls) = cache(vec![Ok(vec!["a"])]);
        let t0 = Instant::now();
        let results = futures::future::join_all((0..16).map(|_| cache.get(URL, t0))).await;
        for result in results {
            assert_eq!(kids(&result.unwrap()), ["a"]);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
