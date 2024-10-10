package com.risingwave.nimtable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkIcebergCompaction {
    public static void main(String[] args) {
        String warehousePath = args[0];
        String database = args[1];
        String table = args[2];
        SparkSession session = SparkSession.builder()
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.nimtable", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.nimtable.type", "hadoop")
                .config("spark.sql.catalog.nimtable.warehouse", warehousePath)
                .getOrCreate();
        List<Row> rows = session.sql(String.format("CALL nimtable.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all', 'true'))", database, table)).collectAsList();
        System.out.printf("compaction success: %s/%s/%s, output: %s%n", warehousePath, database, table, rows);
        try {
            List<Row> expireSnapshotOutputRows = session.sql(String.format("CALL nimtable.system.expire_snapshots(table => '%s.%s')", database, table)).collectAsList();
            System.out.printf("expire_snapshots success: %s/%s/%s, output: %s%n", warehousePath, database, table, expireSnapshotOutputRows);
        } catch (Throwable e) {
            System.err.printf("failed to run expire snapshot: %s%n", e);
        }
    }
}
