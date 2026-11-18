| Arm | Source complete midpoint s | Source checkpoint s | MV s | Source GETs | Source GiB | Whole GETs | Whole GiB | Source denied / shrunk | Source budget peak MiB | Source RSS peak MiB | Whole RSS peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| p1-gap0 | 84.51 | 85.01 | 679.04 | 42,941 | 12.357 | 123,709 | 36.348 | 0 / 0 | 3841.6 | 5841.1 | 5841.1 |
| p1-gap64k | 79.50 | 80.00 | 533.82 | 39,525 | 12.423 | 123,176 | 36.285 | 0 / 0 | 3857.4 | 5784.1 | 5858.5 |
| p2-gap64k | 84.51 | 85.01 | 690.11 | 40,405 | 12.373 | 121,752 | 36.332 | 0 / 0 | 3863.5 | 5796.3 | 5796.3 |
| p2-gap0 | 95.51 | 96.00 | 757.73 | 42,251 | 12.357 | 123,274 | 36.308 | 0 / 0 | 3847.2 | 5398.5 | 5398.5 |
| p3-gap0 | 87.51 | 88.01 | 506.11 | 44,419 | 12.319 | 127,360 | 36.380 | 0 / 0 | 3849.8 | 5754.9 | 5765.4 |
| p3-gap64k | 83.51 | 84.01 | 638.08 | 38,871 | 12.407 | 119,654 | 36.377 | 0 / 0 | 3853.2 | 5742.1 | 5742.1 |

| Metric (cohort median) | Gap 0 | Gap 64 KiB |
| --- | ---: | ---: |
| Source completion midpoint, s | 87.51 | 83.51 |
| Source checkpoint, s | 88.01 | 84.01 |
| MV wall, s | 679.04 | 638.08 |
| Source GETs | 42,941 | 39,525 |
| Source GiB | 12.357 | 12.407 |
| Whole GETs | 123,709 | 121,752 |
| Whole GiB | 36.348 | 36.332 |
| Source denied | 0 | 0 |
| Source shrunk | 0 | 0 |
| Source charged peak MiB | 3847.2 | 3857.4 |
| Source CN RSS peak MiB | 5754.9 | 5784.1 |
| Whole CN RSS peak MiB | 5765.4 | 5796.3 |
| Source physical mean ms | 3.927 | 3.368 |
| Source physical p99 ms | 32.00 | 32.00 |
| Source physical p99.9 ms | 64.00 | 64.00 |
| Source caller p99 ms | 378.77 | 378.77 |
| Source caller p99.9 ms | 568.15 | 568.15 |
| Source overread MiB | 0.000 | 86.275 |
| Source merged members | 3,099 | 7,017 |
| Source bounded requested MiB | 13319.290 | 13302.547 |

| Pair (64 KiB minus 0) | Source checkpoint s | Source completion midpoint s | MV s | Source GETs | Source MiB | Whole GETs | Whole MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | -5.01 | -5.00 | -145.22 | -3,416 | +67.407 | -533 | -64.959 |
| 2 | -11.00 | -11.00 | -67.62 | -1,846 | +16.357 | -1,522 | +24.262 |
| 3 | -4.00 | -4.00 | +131.97 | -5,548 | +89.824 | -7,706 | -3.537 |

Percentiles are histogram bucket upper bounds. All RSS/charged peaks are sampled. Each source checkpoint is the first completed full one-second source scrape.
