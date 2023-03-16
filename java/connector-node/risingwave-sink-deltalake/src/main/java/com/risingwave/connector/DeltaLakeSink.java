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

import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import java.io.IOException;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class DeltaLakeSink extends SinkBase {
    private static final CompressionCodecName codecName = CompressionCodecName.SNAPPY;
    private final String uuid = UUID.randomUUID().toString();
    private final Configuration conf;
    private final DeltaLog log;
    private final Schema sinkSchema;
    private ParquetWriter<GenericRecord> parquetWriter = null;
    private Path parquetPath = null;
    private int dataFileNum = 0;
    private int numOutputRows = 0;

    public DeltaLakeSink(TableSchema tableSchema, Configuration conf, DeltaLog log) {
        super(tableSchema);
        this.conf = conf;
        this.log = log;
        this.sinkSchema = DeltaLakeSinkUtil.convertSchema(log, tableSchema);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        if (this.parquetWriter == null) {
            this.dataFileNum += 1;
            this.parquetPath =
                    new Path(
                            this.log.getPath(),
                            String.format("%s-%d.parquet", this.uuid, this.dataFileNum));
            try {
                HadoopOutputFile outputFile = HadoopOutputFile.fromPath(this.parquetPath, conf);
                this.parquetWriter =
                        AvroParquetWriter.<GenericRecord>builder(outputFile)
                                .withSchema(this.sinkSchema)
                                .withConf(this.conf)
                                .withCompressionCodec(this.codecName)
                                .build();
            } catch (IOException ioException) {
                throw INTERNAL.withCause(ioException).asRuntimeException();
            }
        }
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            switch (row.getOp()) {
                case INSERT:
                    GenericRecord record = new GenericData.Record(this.sinkSchema);
                    for (int i = 0; i < this.sinkSchema.getFields().size(); i++) {
                        record.put(i, row.get(i));
                    }
                    try {
                        this.parquetWriter.write(record);
                        this.numOutputRows += 1;
                    } catch (IOException ioException) {
                        throw INTERNAL.withCause(ioException).asRuntimeException();
                    }
                    break;
                default:
                    throw UNIMPLEMENTED
                            .withDescription("unsupported operation: " + row.getOp())
                            .asRuntimeException();
            }
        }
    }

    @Override
    public void sync() {
        AddFile addFile;
        try {
            this.parquetWriter.close();
            addFile =
                    new AddFile(
                            this.parquetPath.toString(),
                            new HashMap<>(),
                            this.parquetPath
                                    .getFileSystem(conf)
                                    .getContentSummary(this.parquetPath)
                                    .getLength(),
                            System.currentTimeMillis(),
                            true,
                            null,
                            null);
        } catch (IOException ioException) {
            throw INTERNAL.withCause(ioException).asRuntimeException();
        }
        Map<String, String> operationParas = new HashMap<>();
        operationParas.put("mode", "\"Append\"");
        operationParas.put("partitionBy", "\"[]\"");
        Map<String, String> metrics = new HashMap<>();
        metrics.put("numFiles", "1");
        metrics.put("numOutputRows", String.valueOf(this.numOutputRows));
        metrics.put("numOutputBytes", String.valueOf(addFile.getSize()));
        Operation operation = new Operation(Operation.Name.WRITE, operationParas, metrics);
        OptimisticTransaction txn = log.startTransaction();
        try {
            txn.commit(List.of(addFile), operation, "RisingWave");
        } catch (DeltaConcurrentModificationException e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }
        this.parquetWriter = null;
        this.numOutputRows = 0;
    }

    @Override
    public void drop() {
        if (this.parquetWriter != null) {
            try {
                this.parquetWriter.close();
            } catch (IOException ioException) {
                throw INTERNAL.withCause(ioException).asRuntimeException();
            }
        }
    }
}
