/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.cdc.debezium.internal;

import static com.risingwave.java.binding.Binding.getObject;
import static com.risingwave.java.binding.Binding.listObject;
import static com.risingwave.java.binding.Binding.putObject;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpendalSchemaHistory extends AbstractFileBasedSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpendalSchemaHistory.class);
    private String objectName = "schema_history.dat";
    public static final String SOURCE_ID = "schema.history.internal.source.id";
    private static final int MAX_RECORDS_PER_FILE = 1;
    private List<HistoryRecord> buffer = new ArrayList<>();
    private String objectDir = "";
    private static final Pattern HISTORY_FILE_PATTERN =
            Pattern.compile("schema_history_(\\d+)\\.dat");

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException(
                    "Error configuring an instance of "
                            + getClass().getSimpleName()
                            + "; check the logs for details");
        }
        String sourceId = config.getString("schema.history.internal.source.id");
        if (sourceId == null || sourceId.isEmpty()) {
            sourceId = "default_source";
            config = config.edit().with("source_id", sourceId).build();
        }
        objectDir = String.format("mysql-cdc-schema-history-source-%s", sourceId);
        objectName = String.format("%s/schema_history.dat", objectDir); // 兼容老逻辑
        LOGGER.info("Database history will be stored in bucket dir {}", objectDir);
    }

    @Override
    protected void doPreStart() {
        // 无需预启动操作
    }

    @Override
    protected void doStart() {
        try {
            // 验证 listObject 是否可用，列出 hummock_001 下的对象并打印
            String[] testList = listObject("hummock_001");
            if (testList != null) {
                LOGGER.info(
                        "listObject(\"hummock_001\") 返回: {}", java.util.Arrays.toString(testList));
            } else {
                LOGGER.info("listObject(\"hummock_001\") 返回 null");
            }

            List<String> files = new ArrayList<>();
            String[] fileArray = listObject(objectDir);
            if (fileArray != null) {
                Collections.addAll(files, fileArray);
            }
            List<String> historyFiles = new ArrayList<>();
            for (String file : files) {
                if (file.contains("schema_history_") && file.endsWith(".dat")) {
                    historyFiles.add(file);
                }
            }
            // 按时间戳排序
            Collections.sort(
                    historyFiles, Comparator.comparingLong(this::extractTimestampFromFileName));
            for (String file : historyFiles) {
                byte[] data = getObject(file);
                List<HistoryRecord> records = toHistoryRecords(data);
                this.records.addAll(records);
            }
        } catch (Exception e) {
            throw new SchemaHistoryException("Can't retrieve files with schema history", e);
        }
    }

    @Override
    public void doStop() {
        LOGGER.info("doStop");
        if (!buffer.isEmpty()) {
            flushBufferToNewFile();
        }
    }

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {
        LOGGER.info("doPreStoreRecord");
        // Todo: can do some check, can be removed or modified as needed
        if (false) {
            throw new SchemaHistoryException(
                    "No S3 client is available. Ensure that 'start()' is called before storing database history records.");
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        LOGGER.info("doStoreRecord");
        buffer.add(record);
        if (buffer.size() >= MAX_RECORDS_PER_FILE) {
            flushBufferToNewFile();
        }
    }

    @Override
    public boolean storageExists() {
        // Hummock bucket always exists.
        return true;
    }

    @Override
    public void initializeStorage() {
        LOGGER.info("Using hummock bucket to store database history");
    }

    @Override
    public String toString() {
        return "Opendal-S3";
    }

    private void flushBufferToNewFile() {
        if (buffer.isEmpty()) {
            return;
        }
        String fileName =
                String.format("%s/schema_history_%d.dat", objectDir, System.currentTimeMillis());
        byte[] data = fromHistoryRecords(buffer);
        putObject(fileName, data);
        buffer.clear();
        LOGGER.info("Flushed schema history to {}", fileName);
    }

    // 序列化多条记录
    private byte[] fromHistoryRecords(List<HistoryRecord> records) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            for (HistoryRecord r : records) {
                String line = documentWriter.write(r.document());
                if (line != null) {
                    writer.write(line);
                    writer.newLine();
                }
            }
            writer.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaHistoryException("Failed to serialize history records", e);
        }
    }

    // 反序列化多条记录
    private List<HistoryRecord> toHistoryRecords(byte[] data) {
        List<HistoryRecord> result = new ArrayList<>();
        try {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    new ByteArrayInputStream(data), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    result.add(new HistoryRecord(documentReader.read(line)));
                }
            }
        } catch (Exception e) {
            throw new SchemaHistoryException("Failed to deserialize history records", e);
        }
        return result;
    }

    // 提取文件名中的时间戳
    private long extractTimestampFromFileName(String fileName) {
        Matcher m = HISTORY_FILE_PATTERN.matcher(fileName);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return 0L;
    }
}
