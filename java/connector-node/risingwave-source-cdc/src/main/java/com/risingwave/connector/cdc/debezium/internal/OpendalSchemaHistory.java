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
import static com.risingwave.java.binding.Binding.putObject;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpendalSchemaHistory extends AbstractFileBasedSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpendalSchemaHistory.class);
    private String bucket = "hummock001";
    private String objectName = "SchemaHistory.dat";
    private String endpoint = "http://127.0.0.1:9301";

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        LOGGER.info("Database history will be stored in bucket");
    }

    @Override
    protected void doPreStart() {
        LOGGER.info("这里doPreStart");
        // No need for pre-start actions
    }

    @Override
    protected void doStart() {
        LOGGER.info("这里doStart");
        InputStream objectInputStream = retrieveObjectFromStorage();

        if (false) {
            toHistoryRecord(objectInputStream);
        }
    }

    private InputStream retrieveObjectFromStorage() {
        LOGGER.info("这里retrieveObjectFromStorage");
        byte[] byteArray = getObject(objectName);
        return new ByteArrayInputStream(byteArray);
    }

    @Override
    public void doStop() {
        LOGGER.info("这里doStop");
        // Do nothing on stop
    }

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {
        LOGGER.info("这里doPreStoreRecord");
        // Example check, can be removed or modified as needed
        if (false) {
            throw new SchemaHistoryException(
                    "No S3 client is available. Ensure that 'start()' is called before storing database history records.");
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        LOGGER.info("这里doStoreRecord");
        byte[] data = fromHistoryRecord(record);
        String dataString = new String(data, StandardCharsets.UTF_8);
        putObject(objectName, dataString);
    }

    @Override
    public boolean storageExists() {
        // Hummock bucket always exists
        return true;
    }

    @Override
    public void initializeStorage() {
        LOGGER.info("Using hummock bucket to store database history");
    }

    @Override
    public String toString() {
        return "s3"; // More descriptive return value
    }
}
