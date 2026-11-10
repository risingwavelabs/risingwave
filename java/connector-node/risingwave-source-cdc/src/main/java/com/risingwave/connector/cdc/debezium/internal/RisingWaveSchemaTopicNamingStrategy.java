// Copyright 2026 RisingWave Labs
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

import io.debezium.schema.SchemaTopicNamingStrategy;
import java.util.Properties;

/**
 * {@link SchemaTopicNamingStrategy} that keeps the table identifiers in the topic name as they are.
 * The default strategy replaces characters that Kafka does not allow in topic names with an
 * underscore, for example {@code $}, {@code #} and non-ASCII characters. The topic name is the key
 * that routes a change event to its table, so it must match the table name.
 */
public class RisingWaveSchemaTopicNamingStrategy extends SchemaTopicNamingStrategy {
    public RisingWaveSchemaTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public String sanitizedTopicName(String topicName) {
        return topicName;
    }
}
