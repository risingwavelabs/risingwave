#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "$SCRIPT_DIR"/../e2e_test/batch/tpch || exit
all_files=("create_tables" "insert_customer" "insert_lineitem" "insert_nation" "insert_orders" "insert_part"
"insert_partsupp" "insert_supplier" "insert_region" "q1" "q3" "q6" "drop_tables")

rm -f tpch.slt

for file in "${all_files[@]}"; do
  cat "${file}".slt.part >> tpch.slt
  echo "" >> tpch.slt
done
