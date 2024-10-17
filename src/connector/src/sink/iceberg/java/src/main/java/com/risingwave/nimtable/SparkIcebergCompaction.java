package com.risingwave.nimtable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkIcebergCompaction {
    public static void main(String[] args) {
        String database = args[0];
        String table = args[1];
        SparkSession session = SparkSession.builder()
                .config("spark.sql.extensions", "nimtable.shaded.org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.nimtable", "nimtable.shaded.org.apache.iceberg.spark.SparkCatalog")
                .getOrCreate();
        session.sparkContext().
        List<Row> rows = session.sql(String.format("CALL nimtable.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all', 'true'))", database, table)).collectAsList();
        System.out.printf("compaction success: %s/%s, output: %s%n", database, table, rows);
        try {
            List<Row> expireSnapshotOutputRows = session.sql(String.format("CALL nimtable.system.expire_snapshots(table => '%s.%s')", database, table)).collectAsList();
            System.out.printf("expire_snapshots success: %s/%s, output: %s%n", database, table, expireSnapshotOutputRows);
        } catch (Throwable e) {
            System.err.printf("failed to run expire snapshot: %s%n", e);
        }
    }
}
