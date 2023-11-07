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
import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

public class UpsertIcebergSinkWriter extends IcebergSinkWriterBase {
    private final Schema deleteRowSchema;
    private final List<Integer> pkIndices;
    private boolean closed = false;
    private boolean updateBufferExists = false;
    private Map<PartitionKey, SinkRowMap> sinkRowMapByPartition = new HashMap<>();

    public UpsertIcebergSinkWriter(
            TableSchema tableSchema,
            HadoopCatalog hadoopCatalog,
            Table icebergTable,
            FileFormat fileFormat) {
        super(
                tableSchema,
                icebergTable,
                hadoopCatalog,
                icebergTable.schema().select(Arrays.asList(tableSchema.getColumnNames())),
                fileFormat);
        this.deleteRowSchema = icebergTable.schema().select(tableSchema.getPrimaryKeys());
        this.pkIndices =
                tableSchema.getPrimaryKeys().stream()
                        .map(columnName -> tableSchema.getColumnIndex(columnName))
                        .collect(Collectors.toList());
    }

    private static Record newRecord(Schema schema, SinkRow row) {
        Record record = GenericRecord.create(schema);
        for (int i = 0; i < schema.columns().size(); i++) {
            record.set(i, row.get(i));
        }
        return record;
    }

    private EqualityDeleteWriter<Record> newEqualityDeleteWriter(PartitionKey partitionKey) {
        try {
            String filename = fileFormat.addExtension(UUID.randomUUID().toString());
            OutputFile outputFile =
                    icebergTable
                            .io()
                            .newOutputFile(
                                    icebergTable.location()
                                            + "/data/"
                                            + icebergTable.spec().partitionToPath(partitionKey)
                                            + "/"
                                            + filename);
            return Parquet.writeDeletes(outputFile)
                    .forTable(icebergTable)
                    .rowSchema(deleteRowSchema)
                    .withSpec(icebergTable.spec())
                    .withPartition(partitionKey)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .equalityFieldIds(
                            deleteRowSchema.columns().stream()
                                    .mapToInt(Types.NestedField::fieldId)
                                    .toArray())
                    .buildEqualityWriter();
        } catch (Exception e) {
            throw INTERNAL.withDescription("failed to create outputFile and equalityDeleteWriter")
                    .asRuntimeException();
        }
    }

    private DataWriter<Record> newDataWriter(PartitionKey partitionKey) {
        try {
            String filename = fileFormat.addExtension(UUID.randomUUID().toString());
            OutputFile outputFile =
                    icebergTable
                            .io()
                            .newOutputFile(
                                    icebergTable.location()
                                            + "/data/"
                                            + icebergTable.spec().partitionToPath(partitionKey)
                                            + "/"
                                            + filename);
            return Parquet.writeData(outputFile)
                    .schema(rowSchema)
                    .withSpec(icebergTable.spec())
                    .withPartition(partitionKey)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .build();
        } catch (Exception e) {
            throw INTERNAL.withDescription("failed to create outputFile and dataWriter")
                    .asRuntimeException();
        }
    }

    private List<Comparable<Object>> getKeyFromRow(SinkRow row) {
        return this.pkIndices.stream()
                .map(idx -> (Comparable<Object>) row.get(idx))
                .collect(Collectors.toList());
    }

    @Override
    public void write(Iterable<SinkRow> rows) {
        for (SinkRow row : rows) {
            if (row.size() != tableSchema.getColumnNames().length) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("row values do not match table schema")
                        .asRuntimeException();
            }
            Record record = newRecord(rowSchema, row);
            PartitionKey partitionKey =
                    new PartitionKey(icebergTable.spec(), icebergTable.schema());
            partitionKey.partition(record);
            SinkRowMap sinkRowMap;
            if (sinkRowMapByPartition.containsKey(partitionKey)) {
                sinkRowMap = sinkRowMapByPartition.get(partitionKey);
            } else {
                sinkRowMap = new SinkRowMap();
                sinkRowMapByPartition.put(partitionKey, sinkRowMap);
            }
            switch (row.getOp()) {
                case INSERT:
                    sinkRowMap.insert(getKeyFromRow(row), newRecord(rowSchema, row));
                    break;
                case DELETE:
                    sinkRowMap.delete(getKeyFromRow(row), newRecord(deleteRowSchema, row));
                    break;
                case UPDATE_DELETE:
                    if (updateBufferExists) {
                        throw Status.FAILED_PRECONDITION
                                .withDescription("an UPDATE_INSERT should precede an UPDATE_DELETE")
                                .asRuntimeException();
                    }
                    sinkRowMap.delete(getKeyFromRow(row), newRecord(deleteRowSchema, row));
                    updateBufferExists = true;
                    break;
                case UPDATE_INSERT:
                    if (!updateBufferExists) {
                        throw Status.FAILED_PRECONDITION
                                .withDescription("an UPDATE_INSERT should precede an UPDATE_DELETE")
                                .asRuntimeException();
                    }
                    sinkRowMap.insert(getKeyFromRow(row), newRecord(rowSchema, row));
                    updateBufferExists = false;
                    break;
                default:
                    throw UNIMPLEMENTED
                            .withDescription("unsupported operation: " + row.getOp())
                            .asRuntimeException();
            }
        }
    }

    @Override
    protected IcebergMetadata collectSinkMetadata() {
        List<DataFile> dataFileList = new ArrayList<>();
        List<DeleteFile> deleteFileList = new ArrayList<>();
        for (Map.Entry<PartitionKey, SinkRowMap> entry : sinkRowMapByPartition.entrySet()) {
            EqualityDeleteWriter<Record> equalityDeleteWriter =
                    newEqualityDeleteWriter(entry.getKey());
            DataWriter<Record> dataWriter = newDataWriter(entry.getKey());
            for (SinkRowOp sinkRowOp : entry.getValue().map.values()) {
                Record insert = sinkRowOp.getInsert();
                Record delete = sinkRowOp.getDelete();
                if (insert != null) {
                    dataWriter.write(insert);
                }
                if (delete != null) {
                    equalityDeleteWriter.write(delete);
                }
            }
            try {
                equalityDeleteWriter.close();
                dataWriter.close();
            } catch (IOException e) {
                throw INTERNAL.withDescription(
                                "failed to close dataWriter and equalityDeleteWriter")
                        .asRuntimeException();
            }

            if (equalityDeleteWriter.length() > 0) {
                DeleteFile eqDeletes = equalityDeleteWriter.toDeleteFile();
                deleteFileList.add(eqDeletes);
            }
            if (dataWriter.length() > 0) {
                DataFile dataFile = dataWriter.toDataFile();
                dataFileList.add(dataFile);
            }
        }
        sinkRowMapByPartition.clear();
        return new IcebergMetadata(
                dataFileList.toArray(new DataFile[0]), deleteFileList.toArray(new DeleteFile[0]));
    }

    @Override
    public void drop() {
        try {
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
