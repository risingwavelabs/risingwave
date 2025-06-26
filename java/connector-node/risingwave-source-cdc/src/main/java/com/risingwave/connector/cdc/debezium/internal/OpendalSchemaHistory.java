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

import static com.risingwave.java.binding.Binding.deleteObjects;
import static com.risingwave.java.binding.Binding.getObject;
import static com.risingwave.java.binding.Binding.getObjectStoreType;
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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpendalSchemaHistory extends AbstractFileBasedSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpendalSchemaHistory.class);
    private String objectName = "";
    private String sourceId = "";
    public static final String SOURCE_ID = "schema.history.internal.source.id";
    private static final int MAX_RECORDS_PER_FILE = 10;
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
        sourceId = config.getString("schema.history.internal.source.id");
        if (sourceId == null || sourceId.isEmpty()) {
            sourceId = "default_source";
            config = config.edit().with("source_id", sourceId).build();
        }
        objectDir = String.format("mysql-cdc-schema-history-source-%s", sourceId);
        objectName = String.format("%s/schema_history.dat", objectDir);
        LOGGER.info(
                "Schema history for source id {} will be stored under directory {}",
                sourceId,
                objectDir);
    }

    @Override
    protected void doPreStart() {
        // No pre-start operation required
    }

    @Override
    protected void doStart() {
        try {
            // 1. List and sort history files by timestamp
            List<String> historyFiles = new ArrayList<>();
            String[] fileArray = listObject(objectDir);
            if (fileArray != null) {
                Collections.addAll(historyFiles, fileArray);
            }
            historyFiles.removeIf(file -> !HISTORY_FILE_PATTERN.matcher(file).find());
            Collections.sort(
                    historyFiles, Comparator.comparingLong(this::extractTimestampFromFileName));

            // 2. Assign the lazy list to this.records.
            // This is the key change to avoid OOM. We are overriding the parent's list
            // with our own implementation that loads data on demand.
            this.records = new LazyHistoryRecordList(historyFiles);
            LOGGER.info(
                    "Initialized lazy schema history with {} total records across {} files.",
                    this.records.size(),
                    historyFiles.size());

        } catch (Exception e) {
            throw new SchemaHistoryException("Failed to initialize lazy schema history", e);
        }
    }

    @Override
    public void doStop() {
        LOGGER.info(
                "Source {} is dropped, deleting all schema history records under directory {}",
                sourceId,
                objectDir);
        deleteObjects(objectDir);
    }

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {}

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        LOGGER.info("Storing new schema history record.");
        try {
            // 1. Find the latest schema_history_*.dat file
            List<String> files = new ArrayList<>();
            String[] fileArray = listObject(objectDir);
            if (fileArray != null) {
                Collections.addAll(files, fileArray);
            }
            files.removeIf(file -> !HISTORY_FILE_PATTERN.matcher(file).find());
            if (!files.isEmpty()) {
                files.sort(Comparator.comparingLong(this::extractTimestampFromFileName));
            }
            String latestFile = files.isEmpty() ? null : files.get(files.size() - 1);

            List<HistoryRecord> records;
            if (latestFile != null) {
                // 2. Read the latest file content
                byte[] data = getObject(latestFile);
                records = toHistoryRecords(data);
                if (records.size() < MAX_RECORDS_PER_FILE) {
                    // 3. Append and overwrite the file
                    records.add(record);
                    putObject(latestFile, fromHistoryRecords(records));
                    LOGGER.info(
                            "Appended record to existing file: {} (now {} records)",
                            latestFile,
                            records.size());
                    return;
                }
            }
            // 4. Create a new file
            String newFile =
                    String.format(
                            "%s/schema_history_%d.dat", objectDir, System.currentTimeMillis());
            putObject(newFile, fromHistoryRecords(Collections.singletonList(record)));
            LOGGER.info("Created new schema history file: {}", newFile);
        } catch (Exception e) {
            throw new SchemaHistoryException("Failed to store schema history record", e);
        }
    }

    @Override
    public boolean storageExists() {
        // Hummock bucket always exists.
        return true;
    }

    @Override
    public void initializeStorage() {
        String type = getObjectStoreType();
        LOGGER.info("Using hummock object store {} to store database history.", type);
    }

    @Override
    public String toString() {
        return getObjectStoreType();
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

    // Serialize multiple records
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

    // Deserialize multiple records
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

    // Extract timestamp from file name
    private long extractTimestampFromFileName(String fileName) {
        Matcher m = HISTORY_FILE_PATTERN.matcher(fileName);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return 0L;
    }

    /**
     * A custom List implementation that loads schema history records from Opendal on demand. This
     * avoids loading all history records into memory at once.
     */
    private class LazyHistoryRecordList extends AbstractList<HistoryRecord> {
        private final List<String> filePaths;
        // Stores the cumulative number of records up to file i
        private final int[] recordsPerFileIndex;
        private final int totalRecords;

        // An LRU cache to hold the contents of the most recently accessed files.
        // Key: file index, Value: list of records in that file.
        private final Map<Integer, List<HistoryRecord>> cache =
                new LinkedHashMap<Integer, List<HistoryRecord>>(6, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(
                            Map.Entry<Integer, List<HistoryRecord>> eldest) {
                        // Cache up to 100 files' content
                        return size() > 100;
                    }
                };

        public LazyHistoryRecordList(List<String> filePaths) {
            this.filePaths = filePaths;
            this.recordsPerFileIndex = new int[filePaths.size()];

            int cumulativeRecords = 0;
            if (!filePaths.isEmpty()) {
                LOGGER.info("Calculating total records for lazy list...");
                for (int i = 0; i < filePaths.size(); i++) {
                    // This reads the file content, but only holds it temporarily to count records.
                    byte[] data = getObject(filePaths.get(i));
                    // We must deserialize to count accurately.
                    List<HistoryRecord> recs = toHistoryRecords(data);
                    cumulativeRecords += recs.size();
                    this.recordsPerFileIndex[i] = cumulativeRecords;
                }
            }
            this.totalRecords = cumulativeRecords;
            LOGGER.info("Lazy list calculation complete. Total records: {}", this.totalRecords);
        }

        @Override
        public HistoryRecord get(int index) {
            if (index < 0 || index >= totalRecords) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + totalRecords);
            }

            // Find which file contains the record for the given index
            int fileIndex = 0;
            for (int i = 0; i < recordsPerFileIndex.length; i++) {
                if (index < recordsPerFileIndex[i]) {
                    fileIndex = i;
                    break;
                }
            }

            // Check cache first
            List<HistoryRecord> recordsInFile = cache.get(fileIndex);
            if (recordsInFile == null) {
                // Not in cache, load from remote object store and add to cache
                LOGGER.info(
                        "Cache miss for file index {}. Loading from remote object store.",
                        fileIndex);
                byte[] data = getObject(filePaths.get(fileIndex));
                recordsInFile = toHistoryRecords(data);
                cache.put(fileIndex, recordsInFile);
            } else {
                LOGGER.info("Cache hit for file index {}.", fileIndex);
            }

            // Calculate the index within the file
            int baseIndex = (fileIndex == 0) ? 0 : recordsPerFileIndex[fileIndex - 1];
            int indexInFile = index - baseIndex;

            return recordsInFile.get(indexInFile);
        }

        @Override
        public int size() {
            return this.totalRecords;
        }
    }
}
