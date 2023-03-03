package com.risingwave.connector;

import static com.risingwave.proto.Data.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkrow;
import com.risingwave.proto.Data;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class IcebergSinkPartitionTest {
    static String warehousePath = "/tmp/rw-sinknode/iceberg-sink/warehouse";
    static String databaseName = "demo_db";
    static String tableName = "demo_table_partitioned";
    static Schema icebergTableSchema =
            new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.required(3, "part", Types.StringType.get()));
    static TableSchema tableSchema =
            new TableSchema(
                    Lists.newArrayList("id", "name", "part"),
                    Lists.newArrayList(
                            Data.DataType.TypeName.INT32,
                            Data.DataType.TypeName.VARCHAR,
                            Data.DataType.TypeName.VARCHAR),
                    Lists.newArrayList("id"));

    private void createMockTable() throws IOException {
        if (!Paths.get(warehousePath).toFile().isDirectory()) {
            Files.createDirectories(Paths.get(warehousePath));
        }
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath);
        TableIdentifier tableIdent = TableIdentifier.of(databaseName, tableName);
        try {
            catalog.dropTable(tableIdent);
        } catch (Exception e) {
            // Ignored.
        }
        PartitionSpec spec = PartitionSpec.builderFor(icebergTableSchema).identity("part").build();
        catalog.createTable(tableIdent, icebergTableSchema, spec, Map.of("format-version", "2"));
        catalog.close();
    }

    private void validateTableWithIceberg(Set<Record> expected) {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath);
        TableIdentifier tableIdent = TableIdentifier.of(databaseName, tableName);
        Table icebergTable = catalog.loadTable(tableIdent);
        CloseableIterable<Record> iter = IcebergGenerics.read(icebergTable).build();
        Set<Record> actual = Sets.newHashSet(iter);
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    private void validateTableWithSpark(Set<Record> expected) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.demo.type", "hadoop");
        sparkConf.set("spark.sql.catalog.demo.warehouse", warehousePath);
        sparkConf.set("spark.sql.catalog.defaultCatalog", "demo");
        SparkSession spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate();
        List<Row> rows =
                spark.read()
                        .format("iceberg")
                        .load(String.format("demo.%s.%s", databaseName, tableName))
                        .collectAsList();
        spark.close();
        Set<Record> actual = new HashSet<>();
        for (Row row : rows) {
            int id = row.getInt(0);
            String name = row.getString(1);
            String part = row.getString(2);
            Record record = GenericRecord.create(icebergTableSchema);
            record.setField("id", id);
            record.setField("name", name);
            record.setField("part", part);
            actual.add(record);
        }
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    @Test
    public void testSync() throws IOException {
        createMockTable();
        Configuration hadoopConf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        IcebergSink sink =
                new IcebergSink(
                        tableSchema,
                        hadoopCatalog,
                        hadoopCatalog.loadTable(tableIdentifier),
                        FileFormat.PARQUET);

        try {
            sink.write(Iterators.forArray(new ArraySinkrow(Op.INSERT, 1, "Alice", "aaa")));
            sink.sync();

            Record record1 = GenericRecord.create(icebergTableSchema);
            record1.setField("id", 1);
            record1.setField("name", "Alice");
            record1.setField("part", "aaa");
            Set<Record> expected = Sets.newHashSet(record1);
            validateTableWithIceberg(expected);
            validateTableWithSpark(expected);

            sink.write(Iterators.forArray(new ArraySinkrow(Op.INSERT, 2, "Bob", "bbb")));
            validateTableWithIceberg(expected);
            validateTableWithSpark(expected);

            sink.sync();

            Record record2 = GenericRecord.create(icebergTableSchema);
            record2.setField("id", 2);
            record2.setField("name", "Bob");
            record2.setField("part", "bbb");
            expected.add(record2);
            validateTableWithIceberg(expected);
            validateTableWithSpark(expected);
        } catch (Exception e) {
            fail("Exception: " + e);
        } finally {
            sink.drop();
        }
    }

    @Test
    public void testWrite() throws IOException {
        createMockTable();
        Configuration hadoopConf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        IcebergSink sink =
                new IcebergSink(
                        tableSchema,
                        hadoopCatalog,
                        hadoopCatalog.loadTable(tableIdentifier),
                        FileFormat.PARQUET);

        try {
            sink.write(
                    Iterators.forArray(
                            new ArraySinkrow(Op.INSERT, 1, "Alice", "aaa"),
                            new ArraySinkrow(Op.INSERT, 2, "Bob", "bbb")));
            sink.sync();

            Record record1 = GenericRecord.create(icebergTableSchema);
            record1.setField("id", 1);
            record1.setField("name", "Alice");
            record1.setField("part", "aaa");
            Record record2 = GenericRecord.create(icebergTableSchema);
            record2.setField("id", 2);
            record2.setField("name", "Bob");
            record2.setField("part", "bbb");
            Set<Record> expected = Sets.newHashSet(record1, record2);
            validateTableWithIceberg(expected);
            validateTableWithSpark(expected);
        } catch (Exception e) {
            fail("Exception: " + e);
        } finally {
            sink.drop();
        }
    }

    @Test
    public void testDrop() throws IOException {
        createMockTable();
        Configuration hadoopConf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        IcebergSink sink =
                new IcebergSink(
                        tableSchema,
                        hadoopCatalog,
                        hadoopCatalog.loadTable(tableIdentifier),
                        FileFormat.PARQUET);

        sink.drop();

        assertTrue(sink.isClosed());
        assertTrue(Files.exists(Paths.get(warehousePath)));
    }
}
