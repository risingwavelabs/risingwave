| Metric | Old ColdRanges 2w (median of 3) | ColdRanges 6w 16 GiB gap 0 | ColdRanges 6w 16 GiB gap 64 KiB |
| --- | ---: | ---: | ---: |
| whole scheduler overread, MiB | 0.000 | 0.000 | 79.971 |
| whole scheduler merged requests | 3,313 | 3,250 | 6,916 |
| whole requests served by in-flight reuse | 62 | 109 | 57 |
| whole cache gap count | 0 | 8,485 | 6,752 |
| whole cache gap bytes, MiB | 0.000 | 187.083 | 151.493 |
| through_source scheduler overread, MiB | 0.000 | 0.000 | 79.909 |
| through_source scheduler merged requests | 3,250 | 3,091 | 6,846 |
| through_source requests served by in-flight reuse | 52 | 108 | 56 |
| through_source cache gap count | 0 | 8,485 | 6,752 |
| through_source cache gap bytes, MiB | 0.000 | 187.083 | 151.493 |
| after_source scheduler overread, MiB | 0.000 | 0.000 | 0.062 |
| after_source scheduler merged requests | 83 | 159 | 70 |
| after_source requests served by in-flight reuse | 1 | 1 | 1 |
| after_source cache gap count | 0 | 0 | 0 |
| after_source cache gap bytes, MiB | 0.000 | 0.000 | 0.000 |
| Total wall, s | 587.01 | 693.08 | 658.97 |
| Total physical GETs | 117,111 | 122,941 | 118,014 |
| Total physical read, GiB | 36.264 | 35.391 | 36.245 |
| Total physical GET service sum, s | 257.543 | 269.720 | 253.352 |
| Total physical GET mean, ms | 2.196 | 2.194 | 2.147 |
| Total physical GET p99, ms | 16 | 16 | 16 |
| Total CN CPU-s | 2739 | 3172 | 3018 |
| Total prefetch buffer sampled peak, MiB | 3000.37 | 11364.69 | 11303.34 |
| Through source wall, s | 86.25 | 81.04 | 85.96 |
| Through source physical GETs | 35,824 | 44,532 | 39,504 |
| Through source physical read, GiB | 12.529 | 12.291 | 12.597 |
| Through source physical GET service sum, s | 137.676 | 152.065 | 141.606 |
| Through source physical GET mean, ms | 3.843 | 3.415 | 3.585 |
| Through source physical GET p99, ms | 32 | 32 | 32 |
| Through source CN CPU-s | 374 | 358 | 369 |
| Through source prefetch buffer sampled peak, MiB | 3000.37 | 11364.69 | 11303.34 |
| After source wall, s | 500.76 | 612.04 | 573.00 |
| After source physical GETs | 81,114 | 78,409 | 78,510 |
| After source physical read, GiB | 23.642 | 23.099 | 23.648 |
| After source physical GET service sum, s | 116.008 | 117.655 | 111.745 |
| After source physical GET mean, ms | 1.430 | 1.501 | 1.423 |
| After source physical GET p99, ms | 8 | 16 | 8 |
| After source CN CPU-s | 2365 | 2814 | 2649 |
| After source prefetch buffer sampled peak, MiB | 60.96 | 222.15 | 237.42 |
| Provider 24→22 wall, s | 209.00 | 268.06 | 224.01 |
| Provider 24→22 physical GETs | 47,842 | 46,710 | 46,411 |
| Provider 24→22 physical read, GiB | 5.065 | 5.140 | 5.094 |
| Provider 24→22 physical GET service sum, s | 42.918 | 44.016 | 43.045 |
| Provider 24→22 physical GET mean, ms | 0.902 | 0.942 | 0.927 |
| Provider 24→22 physical GET p99, ms | 4 | 4 | 4 |
| Provider 24→22 CN CPU-s | 894 | 1131 | 964 |
| Provider 24→22 prefetch buffer sampled peak, MiB | 36.85 | 117.45 | 144.51 |
| Provider 22→20 wall, s | 134.00 | 143.00 | 153.00 |
| Provider 22→20 physical GETs | 13,763 | 13,507 | 13,436 |
| Provider 22→20 physical read, GiB | 6.690 | 6.567 | 6.584 |
| Provider 22→20 physical GET service sum, s | 22.746 | 23.552 | 22.433 |
| Provider 22→20 physical GET mean, ms | 1.671 | 1.744 | 1.670 |
| Provider 22→20 physical GET p99, ms | 8 | 8 | 8 |
| Provider 22→20 CN CPU-s | 623 | 665 | 694 |
| Provider 22→20 prefetch buffer sampled peak, MiB | 16.91 | 32.04 | 47.55 |
| Whole physical_get mean, ms | 2.196 | 2.194 | 2.147 |
| Whole physical_get 99, ms | 16.000 | 16.000 | 16.000 |
| Whole physical_get 99.9, ms | 64.000 | 32.000 | 64.000 |
| Whole all_object_reads mean, ms | 12.253 | 11.240 | 11.549 |
| Whole all_object_reads 99, ms | 378.768 | 252.512 | 378.768 |
| Whole all_object_reads 99.9, ms | 568.151 | 378.768 | 378.768 |
| Whole admission mean, ms | 8.610 | 7.525 | 7.856 |
| Whole admission 99, ms | 327.680 | 327.680 | 327.680 |
| Whole admission 99.9, ms | 655.360 | 655.360 | 655.360 |
| Whole dispatch mean, ms | 1.281 | 1.175 | 1.149 |
| Whole dispatch 99, ms | 40.960 | 40.960 | 40.960 |
| Whole dispatch 99.9, ms | 81.920 | 81.920 | 40.960 |
| Whole p_reserved_to_first_poll mean, ms | 0.041 | 0.042 | 0.035 |
| Whole p_reserved_to_first_poll 99, ms | 0.640 | 0.640 | 0.640 |
| Whole p_reserved_to_first_poll 99.9, ms | 2.560 | 2.560 | 2.560 |
| startup physical_get p99, ms | 64.00 | 64.00 | 64.00 |
| startup admission p99, ms | 655.36 | 655.36 | 655.36 |
| startup dispatch p99, ms | 81.92 | 81.92 | 81.92 |
| after_startup physical_get p99, ms | 16.00 | 16.00 | 16.00 |
| after_startup admission p99, ms | 0.01 | 0.01 | 0.01 |
| after_startup dispatch p99, ms | 5.12 | 5.12 | 5.12 |
| Peak CN RSS, MiB | 5988 | 5736 | 5630 |
| 24_to_22 table 38 SST candidates | 179,174,635 | 224,208,845 | 194,252,140 |
| 24_to_22 table 38 direct prefetch segments | 42,195 | 1 | 1 |
| 24_to_22 table 38 ordinary data misses | 441 | 42,621 | 42,700 |
| 22_to_20 table 32 SST candidates | 74,910,506 | 78,114,039 | 90,818,622 |
| 22_to_20 table 32 direct prefetch segments | 5,359 | 0 | 0 |
| 22_to_20 table 32 ordinary data misses | 504 | 5,750 | 5,728 |
| whole direct_runs | — | 70,664 | 69,641 |
| through_source direct_runs | — | 45,680 | 44,537 |
| after_source direct_runs | — | 24,984 | 25,104 |
| whole singleton_registrations | — | 49,955 | 49,806 |
| through_source singleton_registrations | — | 1,028 | 814 |
| after_source singleton_registrations | — | 48,927 | 48,992 |
| whole budget_denied | — | 0 | 0 |
| through_source budget_denied | — | 0 | 0 |
| after_source budget_denied | — | 0 | 0 |
| whole partial_overlap_dispatch | — | 300 | 302 |
| through_source partial_overlap_dispatch | — | 228 | 255 |
| after_source partial_overlap_dispatch | — | 72 | 47 |
| whole partial_overlap_bytes | — | 21,132,816 | 22,778,602 |
| through_source partial_overlap_bytes | — | 16,288,342 | 19,692,354 |
| after_source partial_overlap_bytes | — | 4,844,474 | 3,086,248 |
| whole outer_fetches | — | 52,451 | 52,335 |
| through_source outer_fetches | — | — | — |
| after_source outer_fetches | — | — | — |
| whole buffered overlap fill reads / bytes | — | — | 415 / 13,716,428 |
| whole buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| whole buffered overlap disable reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap fill reads / bytes | — | — | 259 / 4,518,524 |
| through_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap fill reads / bytes | — | — | 156 / 9,197,904 |
| after_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
