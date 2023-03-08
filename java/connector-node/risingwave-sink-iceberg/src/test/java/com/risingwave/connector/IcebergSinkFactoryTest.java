package com.risingwave.connector;

import static org.junit.Assert.*;

import com.risingwave.connector.api.TableSchema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class IcebergSinkFactoryTest {
    static String warehousePath = "/tmp/rw-sinknode/iceberg-sink/warehouse";
    static String databaseName = "demo_db";
    static String tableName = "demo_table";
    static String locationType = "local";
    static String sinkMode = "append-only";
    static Schema icebergTableSchema =
            new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()));

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
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createTable(tableIdent, icebergTableSchema, spec, Map.of("format-version", "2"));
        catalog.close();
    }

    @Test
    public void testCreate() throws IOException {
        createMockTable();
        IcebergSinkFactory sinkFactory = new IcebergSinkFactory();
        IcebergSink sink =
                (IcebergSink)
                        sinkFactory.create(
                                TableSchema.getMockTableSchema(),
                                Map.of(
                                        IcebergSinkFactory.SINK_MODE_PROP,
                                        sinkMode,
                                        IcebergSinkFactory.LOCATION_TYPE_PROP,
                                        locationType,
                                        IcebergSinkFactory.WAREHOUSE_PATH_PROP,
                                        warehousePath,
                                        IcebergSinkFactory.DATABASE_NAME_PROP,
                                        databaseName,
                                        IcebergSinkFactory.TABLE_NAME_PROP,
                                        tableName));
        try {
            assertTrue(
                    sink.getHadoopCatalog()
                            .tableExists(TableIdentifier.of(databaseName, tableName)));
            assertEquals(
                    sink.getIcebergTable().location(),
                    warehousePath + "/" + databaseName + "/" + tableName);
        } catch (Exception e) {
            fail("Exception: " + e);
        } finally {
            sink.drop();
        }
    }
}
