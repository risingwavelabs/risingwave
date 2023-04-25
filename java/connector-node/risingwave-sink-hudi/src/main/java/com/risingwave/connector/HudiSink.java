/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import static io.grpc.Status.UNIMPLEMENTED;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_MAX_FILE_SIZE;
import static org.apache.hudi.common.table.HoodieTableConfig.CREATE_SCHEMA;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenUtils;

public class HudiSink extends SinkBase {
    private final HoodieRisingWaveWriter writer;
    private final Schema schema;
    private final List<String> recordKeyField;

    private HudiSinkRowMap sinkRowMap;
    private boolean updateBufferExists = false;

    public HudiSink(
            String basePath, String tableName, Configuration hadoopConf, TableSchema tableSchema) {
        super(tableSchema);
        HoodieTableMetaClient client =
                HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
        this.recordKeyField = List.of(client.getTableConfig().getRecordKeyFields().get());
        this.schema = client.getTableConfig().getTableCreateSchema().get();

        String schemaStr = client.getTableConfig().getProps().getString(CREATE_SCHEMA.key());
        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withPath(basePath)
                        .withSchema(schemaStr)
                        .forTable(tableName)
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder()
                                        .withIndexType(HoodieIndex.IndexType.BLOOM)
                                        .build())
                        .build();
        cfg.setValue(
                HoodieCompactionConfig.COPY_ON_WRITE_INSERT_SPLIT_SIZE,
                String.valueOf(Integer.MAX_VALUE));
        cfg.setValue(HoodieCompactionConfig.COPY_ON_WRITE_AUTO_SPLIT_INSERTS, "false");
        this.writer = new HoodieRisingWaveWriter(new HoodieJavaEngineContext(hadoopConf), cfg);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        if (sinkRowMap == null) {
            sinkRowMap = new HudiSinkRowMap();
        }
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                GenericRecord rec = getGenericRecord(row);
                HoodieKey key =
                        new HoodieKey(
                                KeyGenUtils.getRecordKey(rec, this.recordKeyField, false), "");
                switch (row.getOp()) {
                    case INSERT:
                        sinkRowMap.insert(
                                new HoodieAvroRecord<>(key, new HoodieAvroPayload(Option.of(rec))));
                        break;
                    case DELETE:
                        sinkRowMap.delete(key);
                        break;
                    case UPDATE_DELETE:
                        if (updateBufferExists) {
                            throw Status.FAILED_PRECONDITION
                                    .withDescription(
                                            "an UPDATE_INSERT should precede an UPDATE_DELETE")
                                    .asRuntimeException();
                        }
                        sinkRowMap.delete(key);
                        updateBufferExists = true;
                        break;
                    case UPDATE_INSERT:
                        if (!updateBufferExists) {
                            throw Status.FAILED_PRECONDITION
                                    .withDescription(
                                            "an UPDATE_INSERT should precede an UPDATE_DELETE")
                                    .asRuntimeException();
                        }
                        sinkRowMap.insert(
                                new HoodieAvroRecord<>(key, new HoodieAvroPayload(Option.of(rec))));
                        updateBufferExists = false;
                        break;
                    default:
                        throw UNIMPLEMENTED
                                .withDescription("unsupported operation: " + row.getOp())
                                .asRuntimeException();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void sync() {
        String instant = writer.startCommit();
        List<HoodieRecord<HoodieAvroPayload>> rows = new ArrayList<>();
        for (HudiSinkRowOp rowOp : sinkRowMap.map.values()) {
            if (rowOp.isDelete()) {
                rows.add(rowOp.getDelete());
            } else {
                rows.add(rowOp.getInsert());
            }
        }
        writer.ingest(rows, instant);
        sinkRowMap.clear();
    }

    @Override
    public void drop() {}

    private GenericRecord getGenericRecord(SinkRow row) {
        GenericRecord rec = new GenericData.Record(schema);
        for (String colName : getTableSchema().getColumnNames()) {
            int index = getTableSchema().getColumnIndex(colName);
            rec.put(colName, row.get(index));
        }
        return rec;
    }
}
