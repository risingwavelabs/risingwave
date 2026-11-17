| Metric | Old ColdRanges 2w (median of 3) | Fixed ColdRanges 6w 4 GiB | Fixed ColdRanges 6w 16 GiB |
| --- | ---: | ---: | ---: |
| Total wall, s | 587.01 | 677.11 | 693.08 |
| Total physical GETs | 117,111 | 304,407 | 122,941 |
| Total physical read, GiB | 36.264 | 36.779 | 35.391 |
| Total physical GET service sum, s | 257.543 | 717.847 | 269.720 |
| Total physical GET mean, ms | 2.196 | 2.358 | 2.194 |
| Total physical GET p99, ms | 16 | 16 | 16 |
| Total CN CPU-s | 2739 | 3089 | 3172 |
| Total prefetch buffer sampled peak, MiB | 3000.37 | 4096.00 | 11364.69 |
| Through source wall, s | 86.25 | 96.25 | 81.04 |
| Through source physical GETs | 35,824 | 221,199 | 44,532 |
| Through source physical read, GiB | 12.529 | 12.851 | 12.291 |
| Through source physical GET service sum, s | 137.676 | 597.336 | 152.065 |
| Through source physical GET mean, ms | 3.843 | 2.700 | 3.415 |
| Through source physical GET p99, ms | 32 | 32 | 32 |
| Through source CN CPU-s | 374 | 421 | 358 |
| Through source prefetch buffer sampled peak, MiB | 3000.37 | 4096.00 | 11364.69 |
| After source wall, s | 500.76 | 580.86 | 612.04 |
| After source physical GETs | 81,114 | 83,208 | 78,409 |
| After source physical read, GiB | 23.642 | 23.929 | 23.099 |
| After source physical GET service sum, s | 116.008 | 120.511 | 117.655 |
| After source physical GET mean, ms | 1.430 | 1.448 | 1.501 |
| After source physical GET p99, ms | 8 | 8 | 16 |
| After source CN CPU-s | 2365 | 2668 | 2814 |
| After source prefetch buffer sampled peak, MiB | 60.96 | 283.51 | 222.15 |
| Provider 24→22 wall, s | 209.00 | 256.00 | 268.06 |
| Provider 24→22 physical GETs | 47,842 | 49,101 | 46,710 |
| Provider 24→22 physical read, GiB | 5.065 | 5.225 | 5.140 |
| Provider 24→22 physical GET service sum, s | 42.918 | 46.166 | 44.016 |
| Provider 24→22 physical GET mean, ms | 0.902 | 0.940 | 0.942 |
| Provider 24→22 physical GET p99, ms | 4 | 4 | 4 |
| Provider 24→22 CN CPU-s | 894 | 1063 | 1131 |
| Provider 24→22 prefetch buffer sampled peak, MiB | 36.85 | 155.03 | 117.45 |
| Provider 22→20 wall, s | 134.00 | 143.01 | 143.00 |
| Provider 22→20 physical GETs | 13,763 | 14,173 | 13,507 |
| Provider 22→20 physical read, GiB | 6.690 | 7.016 | 6.567 |
| Provider 22→20 physical GET service sum, s | 22.746 | 24.454 | 23.552 |
| Provider 22→20 physical GET mean, ms | 1.671 | 1.725 | 1.744 |
| Provider 22→20 physical GET p99, ms | 8 | 8 | 8 |
| Provider 22→20 CN CPU-s | 623 | 671 | 665 |
| Provider 22→20 prefetch buffer sampled peak, MiB | 16.91 | 56.80 | 32.04 |
| Whole physical_get mean, ms | 2.196 | 2.358 | 2.194 |
| Whole physical_get 99, ms | 16.000 | 16.000 | 16.000 |
| Whole physical_get 99.9, ms | 64.000 | 32.000 | 32.000 |
| Whole all_object_reads mean, ms | 12.253 | 8.124 | 11.240 |
| Whole all_object_reads 99, ms | 378.768 | 252.512 | 252.512 |
| Whole all_object_reads 99.9, ms | 568.151 | 378.768 | 378.768 |
| Whole admission mean, ms | 8.610 | 3.824 | 7.525 |
| Whole admission 99, ms | 327.680 | 327.680 | 327.680 |
| Whole admission 99.9, ms | 655.360 | 655.360 | 655.360 |
| Whole dispatch mean, ms | 1.281 | 1.684 | 1.175 |
| Whole dispatch 99, ms | 40.960 | 40.960 | 40.960 |
| Whole dispatch 99.9, ms | 81.920 | 81.920 | 81.920 |
| Whole p_reserved_to_first_poll mean, ms | 0.041 | 0.057 | 0.042 |
| Whole p_reserved_to_first_poll 99, ms | 0.640 | 1.280 | 0.640 |
| Whole p_reserved_to_first_poll 99.9, ms | 2.560 | 5.120 | 2.560 |
| startup physical_get p99, ms | 64.00 | 32.00 | 64.00 |
| startup admission p99, ms | 655.36 | 655.36 | 655.36 |
| startup dispatch p99, ms | 81.92 | 40.96 | 81.92 |
| after_startup physical_get p99, ms | 16.00 | 16.00 | 16.00 |
| after_startup admission p99, ms | 0.01 | 40.96 | 0.01 |
| after_startup dispatch p99, ms | 5.12 | 40.96 | 5.12 |
| Peak CN RSS, MiB | 5988 | 4371 | 5736 |
| 24_to_22 table 38 SST candidates | 179,174,635 | 239,149,757 | 224,208,845 |
| 24_to_22 table 38 direct prefetch segments | 42,195 | 1 | 1 |
| 24_to_22 table 38 ordinary data misses | 441 | 42,646 | 42,621 |
| 22_to_20 table 32 SST candidates | 74,910,506 | 81,293,763 | 78,114,039 |
| 22_to_20 table 32 direct prefetch segments | 5,359 | 0 | 0 |
| 22_to_20 table 32 ordinary data misses | 504 | 5,781 | 5,750 |
| whole direct_runs | — | 56,570 | 70,664 |
| through_source direct_runs | — | 27,356 | 45,680 |
| after_source direct_runs | — | 29,214 | 24,984 |
| whole singleton_registrations | — | 49,828 | 49,955 |
| through_source singleton_registrations | — | 619 | 1,028 |
| after_source singleton_registrations | — | 49,209 | 48,927 |
| whole budget_denied | — | 199,920 | 0 |
| through_source budget_denied | — | 199,920 | 0 |
| after_source budget_denied | — | 0 | 0 |
| whole partial_overlap_dispatch | — | 111 | 300 |
| through_source partial_overlap_dispatch | — | 61 | 228 |
| after_source partial_overlap_dispatch | — | 50 | 72 |
| whole partial_overlap_bytes | — | 7,030,955 | 21,132,816 |
| through_source partial_overlap_bytes | — | 3,776,816 | 16,288,342 |
| after_source partial_overlap_bytes | — | 3,254,139 | 4,844,474 |
| whole outer_fetches | — | 246,399 | 52,451 |
| through_source outer_fetches | — | — | — |
| after_source outer_fetches | — | — | — |
| whole buffered overlap fill reads / bytes | — | — | 513 / 16,474,181 |
| whole buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| whole buffered overlap disable reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap fill reads / bytes | — | — | 323 / 5,301,413 |
| through_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap fill reads / bytes | — | — | 190 / 11,172,768 |
| after_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
