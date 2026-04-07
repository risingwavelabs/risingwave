// Copyright 2025 RisingWave Labs
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

package com.risingwave.connector.cdc.debezium.internal;

import static com.risingwave.java.binding.Binding.getObject;
import static com.risingwave.java.binding.Binding.getObjectStoreType;
import static com.risingwave.java.binding.Binding.listObject;
import static com.risingwave.java.binding.Binding.putObject;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpendalSchemaHistory extends AbstractFileBasedSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpendalSchemaHistory.class);

    /**
     * Per-sourceId shared mutable state. All OpendalSchemaHistory instances for the same sourceId
     * share one {@link SharedState} object so that concurrent instances never derive stale cache
     * (e.g. duplicate sequence numbers) even if their lifecycles overlap.
     */
    private static final ConcurrentHashMap<String, SharedState> SHARED_STATES =
            new ConcurrentHashMap<>();

    /** Mutable state that must be shared across instances for the same sourceId. */
    static final class SharedState {
        String cachedLatestFile = null;
        List<HistoryRecord> cachedLatestFileRecords = null;
        long sequenceNumber = 0;
        boolean initialized = false;
    }

    private String sourceId = "";
    public static final String SOURCE_ID = "schema.history.internal.source.id";
    public static final String MAX_RECORDS_PER_FILE_CONFIG =
            "schema.history.internal.max.records.per.file";
    private int maxRecordsPerFile = 2048; // default records nums per file
    private String objectDir = "";

    private static final Pattern HISTORY_FILE_PATTERN =
            Pattern.compile("schema_history_(\\d+)\\.dat");

    private SharedState sharedState;

    // Override ALL_FIELDS to include our custom configuration fields
    // This ensures that our custom fields are properly validated by Debezium
    public static final Field.Set ALL_FIELDS =
            Field.setOf(
                    Field.create(SOURCE_ID, "Unique source ID for schema history storage")
                            .withDescription(
                                    "A unique identifier for this source to avoid path conflicts between multiple sources")
                            .required(),
                    Field.create(
                                    MAX_RECORDS_PER_FILE_CONFIG,
                                    "Maximum number of records per schema history file")
                            .withDescription(
                                    "Maximum number of schema history records to store in a single file before creating a new file")
                            .withDefault(2048));

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
        sourceId = config.getString(SOURCE_ID);
        if (sourceId == null || sourceId.isEmpty()) {
            throw new DebeziumException(
                    "Source ID is required for schema history. Please provide a unique source ID to avoid path conflicts between multiple sources.");
        }
        objectDir = String.format("rw-cdc-schema-history/source-%s", sourceId);
        String maxRecordsStr = config.getString(MAX_RECORDS_PER_FILE_CONFIG);
        if (maxRecordsStr != null) {
            try {
                maxRecordsPerFile = Integer.parseInt(maxRecordsStr);
            } catch (NumberFormatException e) {
                LOGGER.warn(
                        "Invalid value for {}: {}. Using default: {}",
                        MAX_RECORDS_PER_FILE_CONFIG,
                        maxRecordsStr,
                        maxRecordsPerFile);
            }
        }
        sharedState = SHARED_STATES.computeIfAbsent(sourceId, ignored -> new SharedState());
        LOGGER.info(
                "Schema history for source id {} will be stored under directory {} (maxRecordsPerFile={})",
                sourceId,
                objectDir,
                maxRecordsPerFile);
    }

    @Override
    protected void doPreStart() {
        // No pre-start operation required
    }

    @Override
    protected void doStart() {
        synchronized (sharedState) {
            try {
                // 1. List and sort history files by sequence number
                List<String> historyFiles = listAndSortHistoryFiles();

                // 2. Initialize sequence number from existing files
                initializeSequenceNumber(historyFiles);

                // 3. Load all records and initialize cache
                // loadAllHistoryRecords will also initialize the cache for the latest file
                this.records = loadAllHistoryRecords(historyFiles);

                sharedState.initialized = true;

                LOGGER.info(
                        "Loaded schema history with {} total records from {} files. Current sequence number: {}",
                        this.records.size(),
                        historyFiles.size(),
                        sharedState.sequenceNumber);

            } catch (Exception e) {
                throw new SchemaHistoryException("Failed to initialize schema history", e);
            }
        }
    }

    @Override
    public void doStop() {}

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {}

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        LOGGER.info("Storing new schema history record.");
        synchronized (sharedState) {
            try {
                // Use cached information to avoid expensive list operations and getObject() calls
                if (sharedState.cachedLatestFile != null
                        && sharedState.cachedLatestFileRecords.size() < maxRecordsPerFile) {
                    // 1. Append to existing file using cached records (no getObject() needed!)
                    sharedState.cachedLatestFileRecords.add(record);
                    putObject(
                            sharedState.cachedLatestFile,
                            fromHistoryRecords(sharedState.cachedLatestFileRecords));

                    LOGGER.info(
                            "Appended record to existing file: {} (now {} records)",
                            sharedState.cachedLatestFile,
                            sharedState.cachedLatestFileRecords.size());
                } else {
                    // 2. Create new file with next sequence number when current file is full or
                    // doesn't exist
                    long nextSequence = ++sharedState.sequenceNumber;
                    String newFile =
                            String.format("%s/schema_history_%d.dat", objectDir, nextSequence);
                    List<HistoryRecord> newRecords = new ArrayList<>();
                    newRecords.add(record);
                    putObject(newFile, fromHistoryRecords(newRecords));

                    // Update shared cache to point to new file
                    sharedState.cachedLatestFile = newFile;
                    sharedState.cachedLatestFileRecords = newRecords;

                    LOGGER.info(
                            "Created new schema history file: {} (sequence: {})",
                            newFile,
                            nextSequence);
                }
            } catch (Exception e) {
                throw new SchemaHistoryException("Failed to store schema history record", e);
            }
        }
    }

    @Override
    public boolean storageExists() {
        // Check if there are actual schema history files in the storage
        // Returning true only when history files exist ensures that schema snapshot
        // will be triggered on first startup when no history exists yet
        try {
            List<String> historyFiles = listAndSortHistoryFiles();
            boolean exists = !historyFiles.isEmpty();
            LOGGER.debug(
                    "Storage exists check: {} history files found, returning {}",
                    historyFiles.size(),
                    exists);
            return exists;
        } catch (Exception e) {
            LOGGER.warn("Failed to check storage existence, assuming storage does not exist", e);
            return false;
        }
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

    // Extract sequence number from file name
    private long extractSequenceFromFileName(String fileName) {
        Matcher m = HISTORY_FILE_PATTERN.matcher(fileName);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        // This should never happen as files are pre-filtered, but throw exception for safety
        throw new SchemaHistoryException(
                String.format(
                        "Invalid schema history file name format: %s. Expected format: schema_history_<number>.dat",
                        fileName));
    }

    /**
     * Initialize sequence number from existing files. This ensures we don't reuse sequence numbers
     * that have already been used. Caller must hold {@code synchronized(sharedState)}.
     */
    private void initializeSequenceNumber(List<String> historyFiles) {
        if (historyFiles.isEmpty()) {
            sharedState.sequenceNumber = 0;
            LOGGER.info("No existing files, initialized sequence number to 0");
        } else {
            // historyFiles is already sorted by sequence number in listAndSortHistoryFiles(),
            // so the last file has the maximum sequence number
            long maxSequence =
                    extractSequenceFromFileName(historyFiles.get(historyFiles.size() - 1));
            sharedState.sequenceNumber = maxSequence;
            LOGGER.info("Initialized sequence number to {} from existing files", maxSequence);
        }
    }

    /**
     * Load all history records from files sequentially. This is much simpler and more efficient
     * than the lazy loading approach. Schema history recovery typically needs all records anyway,
     * so there's no benefit to complex on-demand loading.
     *
     * <p>This method also initializes the shared cache for the latest file to avoid re-reading it
     * later. Caller must hold {@code synchronized(sharedState)}.
     */
    private List<HistoryRecord> loadAllHistoryRecords(List<String> historyFiles) {
        List<HistoryRecord> allRecords = new ArrayList<>();

        LOGGER.info("Loading schema history from {} files...", historyFiles.size());

        for (int i = 0; i < historyFiles.size(); i++) {
            String filePath = historyFiles.get(i);
            try {
                byte[] data = getObject(filePath);
                List<HistoryRecord> records = toHistoryRecords(data);
                allRecords.addAll(records);

                LOGGER.debug("Loaded {} records from file: {}", records.size(), filePath);

                // Initialize shared cache when processing the last file
                if (i == historyFiles.size() - 1) {
                    sharedState.cachedLatestFile = filePath;
                    sharedState.cachedLatestFileRecords = new ArrayList<>(records);
                    LOGGER.debug(
                            "Initialized cache: latest file {} with {} records",
                            sharedState.cachedLatestFile,
                            sharedState.cachedLatestFileRecords.size());
                }
            } catch (Exception e) {
                LOGGER.error("Failed to load history records from file: {}", filePath, e);
                throw new SchemaHistoryException(
                        "Failed to load history records from file: " + filePath, e);
            }
        }

        // If no files were loaded, reset shared cache
        if (historyFiles.isEmpty()) {
            sharedState.cachedLatestFile = null;
            sharedState.cachedLatestFileRecords = null;
            LOGGER.debug("No existing history files found, cache initialized as empty");
        }

        LOGGER.info("Successfully loaded {} total history records", allRecords.size());
        return allRecords;
    }

    /** List and sort history files by sequence number. Extracted from doStart() to be reusable. */
    private List<String> listAndSortHistoryFiles() {
        List<String> historyFiles = new ArrayList<>();
        String[] fileArray = listObject(objectDir);
        if (fileArray != null) {
            Collections.addAll(historyFiles, fileArray);
        }
        historyFiles.removeIf(file -> !HISTORY_FILE_PATTERN.matcher(file).find());
        Collections.sort(historyFiles, Comparator.comparingLong(this::extractSequenceFromFileName));
        return historyFiles;
    }
}
