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

import java.io.Serializable;
import java.util.Map;

/**
 * The state that the Flink Debezium Consumer holds for each instance.
 *
 * <p>This class describes the most basic state that Debezium used for recovering based on Kafka
 * Connect mechanism. It includes a sourcePartition and sourceOffset.
 *
 * <p>The sourcePartition represents a single input sourcePartition that the record came from (e.g.
 * a filename, table name, or topic-partition). The sourceOffset represents a position in that
 * sourcePartition which can be used to resume consumption of data.
 *
 * <p>These values can have arbitrary structure and should be represented using
 * org.apache.kafka.connect.data objects (or primitive values). For example, a database connector
 * might specify the sourcePartition as a record containing { "db": "database_name", "table":
 * "table_name"} and the sourceOffset as a Long containing the timestamp of the row.
 *
 * <p>The original version is from https://github.com/ververica/flink-cdc-connectors
 */
public class DebeziumOffset implements Serializable {
    private static final long serialVersionUID = 1L;

    public Map<String, ?> sourcePartition;
    public Map<String, ?> sourceOffset;

    public DebeziumOffset() {}

    public DebeziumOffset(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

    public void setSourcePartition(Map<String, ?> sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public void setSourceOffset(Map<String, ?> sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    @Override
    public String toString() {
        return "DebeziumOffset{"
                + "sourcePartition="
                + sourcePartition
                + ", sourceOffset="
                + sourceOffset
                + '}';
    }
}
