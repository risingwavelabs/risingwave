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

import static io.grpc.Status.INTERNAL;
import static io.grpc.Status.UNIMPLEMENTED;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class IcebergSink extends SinkBase {
    private final HadoopCatalog hadoopCatalog;
    private final FileFormat fileFormat;
    private final Schema rowSchema;
    private final Table icebergTable;
    private Map<PartitionKey, DataWriter<Record>> dataWriterMap = new HashMap<>();
    private boolean closed = false;

    public HadoopCatalog getHadoopCatalog() {
        return this.hadoopCatalog;
    }

    public Table getIcebergTable() {
        return this.icebergTable;
    }

    public IcebergSink(
            TableSchema tableSchema,
            HadoopCatalog hadoopCatalog,
            Table icebergTable,
            FileFormat fileFormat) {
        super(tableSchema);
        this.hadoopCatalog = hadoopCatalog;
        this.icebergTable = icebergTable;
        this.rowSchema =
                icebergTable.schema().select(Arrays.asList(getTableSchema().getColumnNames()));
        this.fileFormat = fileFormat;
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                switch (row.getOp()) {
                    case INSERT:
                        Record record = GenericRecord.create(rowSchema);
                        if (row.size() != getTableSchema().getColumnNames().length) {
                            throw INTERNAL.withDescription("row values do not match table schema")
                                    .asRuntimeException();
                        }
                        for (int i = 0; i < rowSchema.columns().size(); i++) {
                            record.set(i, row.get(i));
                        }
                        PartitionKey partitionKey =
                                new PartitionKey(icebergTable.spec(), icebergTable.schema());
                        partitionKey.partition(record);
                        DataWriter<Record> dataWriter;
                        if (dataWriterMap.containsKey(partitionKey)) {
                            dataWriter = dataWriterMap.get(partitionKey);
                        } else {
                            try {
                                String filename =
                                        fileFormat.addExtension(UUID.randomUUID().toString());
                                OutputFile outputFile =
                                        icebergTable
                                                .io()
                                                .newOutputFile(
                                                        icebergTable.location()
                                                                + "/data/"
                                                                + icebergTable
                                                                        .spec()
                                                                        .partitionToPath(
                                                                                partitionKey)
                                                                + "/"
                                                                + filename);
                                dataWriter =
                                        Parquet.writeData(outputFile)
                                                .schema(rowSchema)
                                                .withSpec(icebergTable.spec())
                                                .withPartition(partitionKey)
                                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                                .overwrite()
                                                .build();
                            } catch (Exception e) {
                                throw INTERNAL.withDescription("failed to create dataWriter")
                                        .asRuntimeException();
                            }
                            dataWriterMap.put(partitionKey, dataWriter);
                        }
                        dataWriter.write(record);
                        break;
                    default:
                        throw UNIMPLEMENTED
                                .withDescription("unsupported operation: " + row.getOp())
                                .asRuntimeException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {
        try {
            Transaction transaction = icebergTable.newTransaction();
            AppendFiles append = transaction.newAppend();
            for (DataWriter<Record> dataWriter : dataWriterMap.values()) {
                dataWriter.close();
                DataFile dataFile = dataWriter.toDataFile();
                append = append.appendFile(dataFile);
            }
            append.commit();
            transaction.commitTransaction();
            dataWriterMap.clear();
        } catch (Exception e) {
            throw INTERNAL.withDescription(String.format("failed to commit transaction: %s", e))
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            for (DataWriter<Record> dataWriter : dataWriterMap.values()) {
                dataWriter.close();
            }
            hadoopCatalog.close();
            closed = true;
        } catch (Exception e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }
    }

    public boolean isClosed() {
        return closed;
    }
}
