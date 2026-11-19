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

public class MysqlCdcSource extends CdcSource {
    MysqlCdcSource(GetEventStreamRequest request) {
        super(request);
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.MYSQL;
    }

    @Override
    protected DbzConnectorConfig createConfig(GetEventStreamRequest request) {
        return new MysqlDbzConnectorConfig(request);
    }
}
