#!/usr/bin/env bash

set -euo pipefail

./risedev psql -c "show jobs;"
./risedev psql -c "create view if not exists rw_hummock_sst_delete_ratio as select sstable_id, level_id, sub_level_id, stale_key_count * 1.0 /total_key_count as delete_ratio, jsonb_array_elements_text(table_ids) as tid from rw_catalog.rw_hummock_sstables;"
./risedev psql -c "select * from rw_hummock_sst_delete_ratio where tid='1001' order by delete_ratio desc limit 20;"
