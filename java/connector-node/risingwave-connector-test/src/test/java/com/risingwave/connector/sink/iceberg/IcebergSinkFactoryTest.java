// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.sink.iceberg;

import static org.junit.Assert.*;

import com.risingwave.connector.IcebergSink;
import com.risingwave.connector.IcebergSinkFactory;
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
    static String warehousePath = "file:///tmp/rw-sinknode/iceberg-sink/warehouse";
    static String databaseName = "demo_db";
    static String tableName = "demo_table";
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
                                        "type",
                                        sinkMode,
                                        "warehouse.path",
                                        warehousePath,
                                        "database.name",
                                        databaseName,
                                        "table.name",
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
