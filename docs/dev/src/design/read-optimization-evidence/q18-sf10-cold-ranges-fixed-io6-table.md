| Metric | Old ColdRanges 2w (median of 3) | Old ColdRanges 6w | Fixed ColdRanges 6w |
| --- | ---: | ---: | ---: |
| Total wall, s | 587.01 | 717.82 | 677.11 |
| Total physical GETs | 117,111 | 118,369 | 304,407 |
| Total physical read, GiB | 36.264 | 36.350 | 36.779 |
| Total physical GET service sum, s | 257.543 | 272.001 | 717.847 |
| Total physical GET mean, ms | 2.196 | 2.298 | 2.358 |
| Total physical GET p99, ms | 16 | 32 | 16 |
| Total CN CPU-s | 2739 | 3286 | 3089 |
| Total prefetch buffer sampled peak, MiB | 3000.37 | 3000.73 | 4096.00 |
| Through source wall, s | 86.25 | 86.03 | 96.25 |
| Through source physical GETs | 35,824 | 35,938 | 221,199 |
| Through source physical read, GiB | 12.529 | 12.548 | 12.851 |
| Through source physical GET service sum, s | 137.676 | 147.089 | 597.336 |
| Through source physical GET mean, ms | 3.843 | 4.093 | 2.700 |
| Through source physical GET p99, ms | 32 | 32 | 32 |
| Through source CN CPU-s | 374 | 371 | 421 |
| Through source prefetch buffer sampled peak, MiB | 3000.37 | 3000.73 | 4096.00 |
| After source wall, s | 500.76 | 631.79 | 580.86 |
| After source physical GETs | 81,114 | 82,431 | 83,208 |
| After source physical read, GiB | 23.642 | 23.802 | 23.929 |
| After source physical GET service sum, s | 116.008 | 124.913 | 120.511 |
| After source physical GET mean, ms | 1.430 | 1.515 | 1.448 |
| After source physical GET p99, ms | 8 | 16 | 8 |
| After source CN CPU-s | 2365 | 2915 | 2668 |
| After source prefetch buffer sampled peak, MiB | 60.96 | 62.77 | 283.51 |
| Provider 24→22 wall, s | 209.00 | 256.96 | 256.00 |
| Provider 24→22 physical GETs | 47,842 | 49,404 | 49,101 |
| Provider 24→22 physical read, GiB | 5.065 | 5.266 | 5.225 |
| Provider 24→22 physical GET service sum, s | 42.918 | 49.881 | 46.166 |
| Provider 24→22 physical GET mean, ms | 0.902 | 1.010 | 0.940 |
| Provider 24→22 physical GET p99, ms | 4 | 4 | 4 |
| Provider 24→22 CN CPU-s | 894 | 1116 | 1063 |
| Provider 24→22 prefetch buffer sampled peak, MiB | 36.85 | 38.70 | 155.03 |
| Provider 22→20 wall, s | 134.00 | 182.00 | 143.01 |
| Provider 22→20 physical GETs | 13,763 | 13,568 | 14,173 |
| Provider 22→20 physical read, GiB | 6.690 | 6.666 | 7.016 |
| Provider 22→20 physical GET service sum, s | 22.746 | 22.452 | 24.454 |
| Provider 22→20 physical GET mean, ms | 1.671 | 1.655 | 1.725 |
| Provider 22→20 physical GET p99, ms | 8 | 8 | 8 |
| Provider 22→20 CN CPU-s | 623 | 790 | 671 |
| Provider 22→20 prefetch buffer sampled peak, MiB | 16.91 | 15.48 | 56.80 |
| Whole physical_get mean, ms | 2.196 | 2.298 | 2.358 |
| Whole physical_get 99, ms | 16.000 | 32.000 | 16.000 |
| Whole physical_get 99.9, ms | 64.000 | 64.000 | 32.000 |
| Whole all_object_reads mean, ms | 12.253 | 12.211 | 8.124 |
| Whole all_object_reads 99, ms | 378.768 | 378.768 | 252.512 |
| Whole all_object_reads 99.9, ms | 568.151 | 568.151 | 378.768 |
| Whole admission mean, ms | 8.610 | 8.339 | 3.824 |
| Whole admission 99, ms | 327.680 | 327.680 | 327.680 |
| Whole admission 99.9, ms | 655.360 | 655.360 | 655.360 |
| Whole dispatch mean, ms | 1.281 | 1.242 | 1.684 |
| Whole dispatch 99, ms | 40.960 | 40.960 | 40.960 |
| Whole dispatch 99.9, ms | 81.920 | 40.960 | 81.920 |
| Whole p_reserved_to_first_poll mean, ms | 0.041 | 0.041 | 0.057 |
| Whole p_reserved_to_first_poll 99, ms | 0.640 | 0.640 | 1.280 |
| Whole p_reserved_to_first_poll 99.9, ms | 2.560 | 5.120 | 5.120 |
| startup physical_get p99, ms | 64.00 | 64.00 | 32.00 |
| startup admission p99, ms | 655.36 | 655.36 | 655.36 |
| startup dispatch p99, ms | 81.92 | 81.92 | 40.96 |
| after_startup physical_get p99, ms | 16.00 | 16.00 | 16.00 |
| after_startup admission p99, ms | 0.01 | 0.01 | 40.96 |
| after_startup dispatch p99, ms | 5.12 | 5.12 | 40.96 |
| Peak CN RSS, MiB | 5988 | 6014 | 4371 |
| 24_to_22 table 38 SST candidates | 179,174,635 | 209,998,025 | 239,149,757 |
| 24_to_22 table 38 direct prefetch segments | 42,195 | 42,462 | 1 |
| 24_to_22 table 38 ordinary data misses | 441 | 401 | 42,646 |
| 22_to_20 table 32 SST candidates | 74,910,506 | 121,015,585 | 81,293,763 |
| 22_to_20 table 32 direct prefetch segments | 5,359 | 5,354 | 0 |
| 22_to_20 table 32 ordinary data misses | 504 | 411 | 5,781 |
| whole direct_runs | — | 115,950 | 56,570 |
| through_source direct_runs | — | 38,122 | 27,356 |
| after_source direct_runs | — | 77,828 | 29,214 |
| whole singleton_registrations | — | — | 49,828 |
| through_source singleton_registrations | — | — | 619 |
| after_source singleton_registrations | — | — | 49,209 |
| whole budget_denied | — | — | 199,920 |
| through_source budget_denied | — | — | 199,920 |
| after_source budget_denied | — | — | 0 |
| whole partial_overlap_dispatch | — | — | 111 |
| through_source partial_overlap_dispatch | — | — | 61 |
| after_source partial_overlap_dispatch | — | — | 50 |
| whole partial_overlap_bytes | — | — | 7,030,955 |
| through_source partial_overlap_bytes | — | — | 3,776,816 |
| after_source partial_overlap_bytes | — | — | 3,254,139 |
| whole outer_fetches | — | 2,703 | 246,399 |
| through_source outer_fetches | — | — | — |
| after_source outer_fetches | — | — | — |
| whole buffered overlap fill reads / bytes | — | — | 6,328 / 132,193,671 |
| whole buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| whole buffered overlap disable reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap fill reads / bytes | — | — | 6,093 / 117,336,266 |
| through_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| through_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap fill reads / bytes | — | — | 235 / 14,857,405 |
| after_source buffered overlap not_fill reads / bytes | — | — | 0 / 0 |
| after_source buffered overlap disable reads / bytes | — | — | 0 / 0 |
