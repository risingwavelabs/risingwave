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

import com.risingwave.connector.api.source.ConnectorConfig;
import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.sourcenode.mysql.MySqlSourceConfig;
import com.risingwave.sourcenode.postgres.PostgresSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceHandlerFactory {
    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerFactory.class);

    public static SourceHandler createSourceHandler(
            SourceTypeE type, long sourceId, String startOffset, ConnectorConfig userProps) {
        switch (type) {
            case MYSQL:
                return DefaultSourceHandler.newWithConfig(
                        new MySqlSourceConfig(sourceId, startOffset, userProps));
            case POSTGRES:
                return DefaultSourceHandler.newWithConfig(
                        new PostgresSourceConfig(sourceId, startOffset, userProps));
            default:
                LOG.warn("unknown source type: {}", type);
                return null;
        }
    }
}
