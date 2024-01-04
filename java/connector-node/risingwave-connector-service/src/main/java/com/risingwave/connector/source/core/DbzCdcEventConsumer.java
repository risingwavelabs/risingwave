// Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.source.core;

import com.risingwave.connector.cdc.debezium.internal.DebeziumOffset;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffsetSerializer;
import com.risingwave.proto.ConnectorServiceProto.CdcMessage;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum EventType {
    HEARTBEAT,
    TRANSACTION,
    DATA,
}

public class DbzCdcEventConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEventConsumer.class);

    private final BlockingQueue<GetEventStreamResponse> outputChannel;
    private final long sourceId;
    private final JsonConverter converter;
    private final String heartbeatTopicPrefix;
    private final String transactionTopic;

    DbzCdcEventConsumer(
            long sourceId,
            String heartbeatTopicPrefix,
            String transactionTopic,
            BlockingQueue<GetEventStreamResponse> queue) {
        this.sourceId = sourceId;
        this.outputChannel = queue;
        this.heartbeatTopicPrefix = heartbeatTopicPrefix;
        this.transactionTopic = transactionTopic;
        LOG.info("heartbeat topic: {}, trnx topic: {}", heartbeatTopicPrefix, transactionTopic);

        // The default JSON converter will output the schema field in the JSON which is unnecessary
        // to source parser, we use a customized JSON converter to avoid outputting the `schema`
        // field.
        var jsonConverter = new DbzJsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        // only serialize the value part
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        // include record schema to output JSON in { "schema": { ... }, "payload": { ... } } format
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        jsonConverter.configure(configs);
        this.converter = jsonConverter;
    }

    private EventType getEventType(SourceRecord record) {
        LOG.info("event topic: {}", record.topic());
        if (isHeartbeatEvent(record)) {
            return EventType.HEARTBEAT;
        } else if (isTransactionMetaEvent(record)) {
            return EventType.TRANSACTION;
        } else {
            return EventType.DATA;
        }
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null
                && heartbeatTopicPrefix != null
                && topic.startsWith(heartbeatTopicPrefix);
    }

    private boolean isTransactionMetaEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.equals(transactionTopic);
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> events,
            DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        var respBuilder = GetEventStreamResponse.newBuilder();
        LOG.info("event batch size => {}", events.size());
        for (ChangeEvent<SourceRecord, SourceRecord> event : events) {
            var record = event.value();
            EventType eventType = getEventType(record);
            DebeziumOffset offset =
                    new DebeziumOffset(
                            record.sourcePartition(),
                            record.sourceOffset(),
                            (eventType == EventType.HEARTBEAT));
            // serialize the offset to a JSON, so that kernel doesn't need to
            // aware its layout
            String offsetStr = "";
            try {
                byte[] serialized = DebeziumOffsetSerializer.INSTANCE.serialize(offset);
                offsetStr = new String(serialized, StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.warn("failed to serialize debezium offset", e);
            }

            var msgBuilder =
                    CdcMessage.newBuilder()
                            .setOffset(offsetStr)
                            .setPartition(String.valueOf(sourceId));

            switch (eventType) {
                case HEARTBEAT:
                    {
                        var message = msgBuilder.build();
                        LOG.debug("heartbeat => {}", message.getOffset());
                        respBuilder.addEvents(message);

                        break;
                    }
                case TRANSACTION:
                    {
                        long trxTs = ((Struct) record.value()).getInt64("ts_ms");
                        byte[] payload =
                                converter.fromConnectData(
                                        record.topic(), record.valueSchema(), record.value());
                        var message =
                                msgBuilder
                                        .setIsTransactionMeta(true)
                                        .setPayload(new String(payload, StandardCharsets.UTF_8))
                                        .setSourceTsMs(trxTs)
                                        .build();
                        LOG.debug("transaction => {}", message);
                        respBuilder.addEvents(message);

                        break;
                    }
                case DATA:
                    {
                        // Topic naming conventions
                        // - PG: serverName.schemaName.tableName
                        // - MySQL: serverName.databaseName.tableName
                        // We can extract the full table name from the topic
                        var fullTableName =
                                record.topic().substring(record.topic().indexOf('.') + 1);

                        // ignore null record
                        if (record.value() == null) {
                            break;
                        }
                        // get upstream event time from the "source" field
                        var sourceStruct = ((Struct) record.value()).getStruct("source");
                        long sourceTsMs =
                                sourceStruct == null
                                        ? System.currentTimeMillis()
                                        : sourceStruct.getInt64("ts_ms");
                        byte[] payload =
                                converter.fromConnectData(
                                        record.topic(), record.valueSchema(), record.value());
                        var message =
                                msgBuilder
                                        .setFullTableName(fullTableName)
                                        .setPayload(new String(payload, StandardCharsets.UTF_8))
                                        .setSourceTsMs(sourceTsMs)
                                        .build();
                        LOG.debug("record => {}", message.getPayload());
                        respBuilder.addEvents(message);
                        break;
                    }
                default:
                    break;
            }

            // mark the event as processed
            committer.markProcessed(event);
        }

        // skip empty batch
        if (respBuilder.getEventsCount() > 0) {
            respBuilder.setSourceId(sourceId);
            var response = respBuilder.build();
            outputChannel.put(response);
        }

        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
    }

    public BlockingQueue<GetEventStreamResponse> getOutputChannel() {
        return this.outputChannel;
    }
}
