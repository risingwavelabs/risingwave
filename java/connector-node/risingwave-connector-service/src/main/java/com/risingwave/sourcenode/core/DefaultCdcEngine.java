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

package com.risingwave.sourcenode.core;

import com.risingwave.connector.api.source.CdcEngine;
import com.risingwave.connector.api.source.SourceConfig;
import com.risingwave.proto.ConnectorServiceProto;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.heartbeat.Heartbeat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DefaultCdcEngine implements CdcEngine {
    static final int DEFAULT_QUEUE_CAPACITY = 16;

    private final DebeziumEngine<?> engine;
    private final CdcEventConsumer consumer;
    private final SourceConfig config;

    /** If config is not valid will throw exceptions */
    public DefaultCdcEngine(SourceConfig config, DebeziumEngine.CompletionCallback callback) {
        var dbzHeartbeatPrefix =
                config.getProperties().getProperty(Heartbeat.HEARTBEAT_TOPICS_PREFIX.name());
        var consumer =
                new CdcEventConsumer(
                        config.getId(),
                        dbzHeartbeatPrefix,
                        new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY));

        // Builds a debezium engine but not start it
        this.config = config;
        this.consumer = consumer;
        this.engine =
                DebeziumEngine.create(Connect.class)
                        .using(config.getProperties())
                        .using(callback)
                        .notifying(consumer)
                        .build();
    }

    /** Start to run the cdc engine */
    @Override
    public void run() {
        engine.run();
    }

    @Override
    public long getId() {
        return config.getId();
    }

    public void stop() throws Exception {
        engine.close();
    }

    @Override
    public BlockingQueue<ConnectorServiceProto.GetEventStreamResponse> getOutputChannel() {
        return consumer.getOutputChannel();
    }
}
