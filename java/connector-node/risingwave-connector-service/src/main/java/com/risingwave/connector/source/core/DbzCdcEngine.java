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

package com.risingwave.connector.source.core;

import static io.debezium.config.CommonConnectorConfig.TOPIC_PREFIX;
import static io.debezium.schema.AbstractTopicNamingStrategy.*;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DbzCdcEngine implements Runnable {
    static final int DEFAULT_QUEUE_CAPACITY = 16;

    private final DebeziumEngine<?> engine;
    private final DbzChangeEventConsumer changeEventConsumer;
    private final long id;

    /** If config is not valid will throw exceptions */
    public DbzCdcEngine(
            SourceTypeE connector,
            long sourceId,
            Properties config,
            DebeziumEngine.CompletionCallback completionCallback) {
        var heartbeatTopicPrefix = config.getProperty(TOPIC_HEARTBEAT_PREFIX.name());
        var topicPrefix = config.getProperty(TOPIC_PREFIX.name());
        var transactionTopic = String.format("%s.%s", topicPrefix, DEFAULT_TRANSACTION_TOPIC);
        var consumer =
                new DbzChangeEventConsumer(
                        connector,
                        sourceId,
                        heartbeatTopicPrefix,
                        transactionTopic,
                        topicPrefix,
                        new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY));

        // Builds a debezium engine but not start it
        this.id = sourceId;
        this.changeEventConsumer = consumer;
        this.engine =
                DebeziumEngine.create(Connect.class)
                        .using(config)
                        .using(completionCallback)
                        .notifying(consumer)
                        .build();
    }

    /** Start to run the cdc engine */
    @Override
    public void run() {
        engine.run();
    }

    public long getId() {
        return id;
    }

    public void stop() throws Exception {
        try {
            engine.close();
        } catch (IllegalStateException e) {
            // Debezium's AsyncEmbeddedEngine.close() throws IllegalStateException when called
            // multiple times. Treat stop as idempotent.
            return;
        }
    }

    public BlockingQueue<ConnectorServiceProto.GetEventStreamResponse> getOutputChannel() {
        return changeEventConsumer.getOutputChannel();
    }

    public DbzChangeEventConsumer getChangeEventConsumer() {
        return changeEventConsumer;
    }
}
