## Organization of the meta crates

We split the meta module into smaller crates in order to speed up compilation.

- `meta/node` is the final meta node server
- `meta/service` is tonic grpc service implementations. We may further split this into parallel sub-crates.
- The remaining part `meta/src` is the implementation details imported by services. In the future, we can also try to re-organize this into smaller units.

Refer to [#12924](https://github.com/risingwavelabs/risingwave/pull/12924) for more details.
