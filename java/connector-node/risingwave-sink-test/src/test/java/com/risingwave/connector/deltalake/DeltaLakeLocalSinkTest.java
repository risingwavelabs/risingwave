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

package com.risingwave.connector.deltalake;

import static com.risingwave.proto.Data.*;
import static org.apache.spark.sql.types.DataTypes.*;

import com.google.common.collect.Iterators;
import com.risingwave.connector.DeltaLakeSink;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import io.delta.standalone.DeltaLog;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class DeltaLakeLocalSinkTest {
    static String location = "/tmp/rw-sinknode/delta-lake/delta";

    private static DeltaLakeSink createMockSink(String location) {
        DeltaLakeSinkFactoryTest.createMockTable(location);
        Configuration conf = new Configuration();
        DeltaLog log = DeltaLog.forTable(conf, location);
        return new DeltaLakeSink(TableSchema.getMockTableSchema(), conf, log);
    }

    private void validateTableWithSpark(String location, List<Row> rows, StructType schema) {
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        .getOrCreate();
        Dataset<Row> df = spark.read().format("delta").load(location);
        Dataset<Row> expect = spark.createDataFrame(rows, schema);
        assert (expect.exceptAll(df).isEmpty());
    }

    @Test
    public void testWrite() throws IOException {
        DeltaLakeSink sink = createMockSink(location);

        sink.write(
                Iterators.forArray(
                        new ArraySinkRow(Op.INSERT, 1, "Alice"),
                        new ArraySinkRow(Op.INSERT, 2, "Bob")));
        sink.sync();

        List<Row> rows = List.of(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
        StructType schema =
                DataTypes.createStructType(
                        new StructField[] {
                            createStructField("id", IntegerType, false),
                            createStructField("name", StringType, false),
                        });
        validateTableWithSpark(location, rows, schema);

        sink.drop();
        DeltaLakeSinkFactoryTest.dropMockTable(location);
    }

    @Test
    public void testSync() throws IOException {
        DeltaLakeSink sink = createMockSink(location);
        StructType schema =
                DataTypes.createStructType(
                        new StructField[] {
                            createStructField("id", IntegerType, false),
                            createStructField("name", StringType, false),
                        });

        sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 1, "Alice")));
        validateTableWithSpark(location, List.of(), schema);

        sink.sync();
        List<Row> rows = List.of(RowFactory.create(1, "Alice"));
        validateTableWithSpark(location, rows, schema);

        sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 2, "Bob")));
        sink.sync();
        rows = List.of(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
        validateTableWithSpark(location, rows, schema);

        sink.drop();
        DeltaLakeSinkFactoryTest.dropMockTable(location);
    }

    @Test
    public void testDrop() throws IOException {
        DeltaLakeSink sink = createMockSink(location);
        DataTypes.createStructType(
                new StructField[] {
                    createStructField("id", IntegerType, false),
                    createStructField("name", StringType, false),
                });

        sink.drop();
        assert (Files.exists(Paths.get(location)));

        DeltaLakeSinkFactoryTest.dropMockTable(location);
    }
}
