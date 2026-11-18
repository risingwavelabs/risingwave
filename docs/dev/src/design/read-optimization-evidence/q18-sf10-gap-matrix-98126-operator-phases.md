# Operator-phase attribution

Elapsed differences after source scanning include changes in generated SST candidate work. These counters locate the difference but cannot isolate an end-to-end speedup from the gap policy.

| Arm | Interval | Wall s | CN CPU s | Physical GET service s | GETs | MiB | SST candidates table 38 M | SST candidates table 32 M |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| p1-gap0 | through_source | 85.01 | 378 | 147.73 | 42,941 | 12653.76 | 0.00 | 0.00 |
| p1-gap0 | first_to_24 | 98.01 | 615 | 30.64 | 9,229 | 5538.26 | 0.00 | 0.00 |
| p1-gap0 | 24_to_22 | 232.01 | 1015 | 46.81 | 48,335 | 5406.13 | 195.00 | 0.00 |
| p1-gap0 | 22_to_20 | 170.00 | 754 | 21.84 | 13,838 | 7022.16 | 0.00 | 123.20 |
| p1-gap0 | agg20_to_merge | 91.86 | 312 | 18.59 | 9,212 | 6532.08 | 0.00 | 0.00 |
| p1-gap64k | through_source | 80.00 | 341 | 133.12 | 39,525 | 12721.17 | 0.00 | 0.00 |
| p1-gap64k | first_to_24 | 84.00 | 544 | 33.85 | 10,947 | 5799.91 | 1.03 | 0.00 |
| p1-gap64k | 24_to_22 | 155.00 | 710 | 46.94 | 49,432 | 5280.45 | 119.21 | 0.00 |
| p1-gap64k | 22_to_20 | 132.00 | 590 | 22.06 | 13,603 | 6780.58 | 0.00 | 78.88 |
| p1-gap64k | agg20_to_merge | 80.69 | 294 | 15.69 | 9,661 | 6572.95 | 0.00 | 0.59 |
| p2-gap64k | through_source | 85.01 | 359 | 143.81 | 40,405 | 12670.36 | 0.00 | 0.00 |
| p2-gap64k | first_to_24 | 90.02 | 586 | 37.48 | 11,205 | 5842.63 | 0.42 | 0.00 |
| p2-gap64k | 24_to_22 | 240.99 | 912 | 59.94 | 48,418 | 5147.15 | 194.60 | 0.00 |
| p2-gap64k | 22_to_20 | 151.04 | 646 | 28.45 | 13,623 | 6793.24 | 0.17 | 60.78 |
| p2-gap64k | agg20_to_merge | 120.87 | 328 | 29.61 | 7,999 | 6692.99 | 0.00 | 0.00 |
| p2-gap0 | through_source | 96.00 | 374 | 203.42 | 42,251 | 12654.00 | 0.00 | 0.00 |
| p2-gap0 | first_to_24 | 111.07 | 595 | 59.10 | 12,518 | 5902.82 | 0.60 | 0.00 |
| p2-gap0 | 24_to_22 | 229.00 | 907 | 45.53 | 46,895 | 5135.33 | 194.40 | 0.00 |
| p2-gap0 | 22_to_20 | 207.11 | 671 | 40.09 | 13,795 | 6918.33 | 0.00 | 89.77 |
| p2-gap0 | agg20_to_merge | 113.31 | 339 | 23.17 | 7,794 | 6568.68 | 0.00 | 0.00 |
| p3-gap0 | through_source | 88.01 | 379 | 174.42 | 44,419 | 12614.55 | 0.00 | 0.00 |
| p3-gap0 | first_to_24 | 91.00 | 576 | 46.89 | 12,180 | 5666.88 | 0.00 | 0.00 |
| p3-gap0 | 24_to_22 | 111.00 | 602 | 49.03 | 47,593 | 5442.72 | 44.91 | 0.00 |
| p3-gap0 | 22_to_20 | 133.00 | 604 | 24.55 | 14,287 | 7231.93 | 0.09 | 60.70 |
| p3-gap0 | agg20_to_merge | 82.07 | 289 | 16.26 | 8,881 | 6297.44 | 0.00 | 0.00 |
| p3-gap64k | through_source | 84.01 | 374 | 129.72 | 38,871 | 12704.37 | 0.00 | 0.00 |
| p3-gap64k | first_to_24 | 88.02 | 586 | 46.40 | 12,446 | 5790.80 | 0.23 | 0.00 |
| p3-gap64k | 24_to_22 | 227.00 | 966 | 43.40 | 47,192 | 5330.79 | 194.77 | 0.26 |
| p3-gap64k | 22_to_20 | 135.00 | 622 | 23.51 | 13,789 | 7011.11 | 0.00 | 75.24 |
| p3-gap64k | agg20_to_merge | 102.94 | 326 | 17.73 | 7,336 | 6412.30 | 0.00 | 0.00 |

Physical GET service is an aggregate of parallel reads and cannot be subtracted from wall time. Phase boundaries use provider logs, while counters use the preceding full scrape. SST candidate counters are CN-wide for the named table. Source counters use the first source-complete one-second scrape.
