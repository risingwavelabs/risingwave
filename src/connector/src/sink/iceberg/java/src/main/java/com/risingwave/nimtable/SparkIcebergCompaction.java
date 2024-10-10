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
//        long count = session.sql("create table nimtable.db.table (id int, name string)").count();
        // session.sql("insert into nimtable.db.table values (1, 'aaa'), (2, 'bbb')").count();
        // session.sql("insert into nimtable.db.table values (3, 'ccc'), (4, 'ddd')").count();
        // System.out.println(session.sql("select * from nimtable.db.table").javaRDD().collect());
        session.sql(String.format("CALL nimtable.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all', 'true'))", database, table)).count();
        System.out.println(String.format("compaction success: %s/%s/%s", warehousePath, database, table));
        // System.out.println(session.sql("select * from nimtable.db.table").javaRDD().collect());
    }
}
