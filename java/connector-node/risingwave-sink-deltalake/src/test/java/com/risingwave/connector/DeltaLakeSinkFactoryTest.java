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

package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;

import com.risingwave.connector.api.TableSchema;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class DeltaLakeSinkFactoryTest {
    public static String location = "/tmp/rw-sinknode/delta-lake/delta";

    public static void createMockTable(String location) {
        if (Files.exists(Paths.get(location))) {
            throw INVALID_ARGUMENT
                    .withDescription("Test path should not exist")
                    .asRuntimeException();
        }

        Configuration conf = new Configuration();
        DeltaLog log = DeltaLog.forTable(conf, location);

        // should be synchronized with `TableSchema.getMockTableSchema()`;
        StructType schema =
                new StructType(
                        new StructField[] {
                            new StructField("id", new IntegerType()),
                            new StructField("name", new StringType())
                        });

        Operation operation = new Operation(Operation.Name.CREATE_TABLE);
        OptimisticTransaction txn = log.startTransaction();
        txn.updateMetadata(
                Metadata.builder().schema(schema).createdTime(System.currentTimeMillis()).build());
        txn.commit(List.of(), operation, "RisingWave Test");
    }

    public static void dropMockTable(String location) throws IOException {
        FileUtils.deleteDirectory(Paths.get(location).toFile());
    }

    @Test
    public void testCreate() throws IOException {
        createMockTable(location);
        DeltaLakeSinkFactory sinkFactory = new DeltaLakeSinkFactory();
        sinkFactory.create(
                TableSchema.getMockTableSchema(),
                new HashMap<>() {
                    {
                        put("location", String.format("file://%s", location));
                    }
                });
        dropMockTable(location);
    }
}
