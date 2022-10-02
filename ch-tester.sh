#!/bin/bash
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/create_tables.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_customer.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_district.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_history.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_item.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_neworder.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_order.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_orderline.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_stock.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpcc/insert_warehouse.slt.part

sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/create_tables.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_lineitem.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_nation.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_orders.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_part.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_partsupp.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_region.slt.part
sqllogictest -h localhost -p 4566 -d dev e2e_test/tpch/insert_supplier.slt.part


