/*
 * Copyright 2026 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A CDC source of one connector. {@link CdcSourceFactory} assembles it from the request of the Rust
 * reader. The generic engine and consumer code ask it for the behavior that differs between
 * connectors, and read everything derived from the request through its {@link DbzConnectorConfig}.
 */
public abstract class CdcSource {
    private final GetEventStreamRequest request;
    private DbzConnectorConfig config;

    protected CdcSource(GetEventStreamRequest request) {
        this.request = request;
    }

    public abstract SourceTypeE getSourceType();

    /** Builds the connector's {@link DbzConnectorConfig} from {@code request}. */
    protected abstract DbzConnectorConfig createConfig(GetEventStreamRequest request);

    /**
     * Returns the configuration of this source, built from the request on the first call. Building
     * it resolves the Debezium properties and does not reach the upstream database; call {@link
     * DbzConnectorConfig#validateSource} for that.
     */
    public synchronized DbzConnectorConfig getConfig() {
        if (config == null) {
            config = createConfig(request);
        }
        return config;
    }

    /** Returns the offset that RisingWave persists for a change event. */
    public Map<String, ?> getEventSourceOffset(SourceRecord record, CdcEventType eventType) {
        return record.sourceOffset();
    }
}
