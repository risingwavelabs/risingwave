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
import java.util.Map;

public final class CdcSourceFactory {
    private CdcSourceFactory() {}

    public static CdcSource createSource(
            SourceTypeE sourceType,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        switch (sourceType) {
            case MYSQL:
                return new MysqlCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            case MARIADB:
                return new MariadbCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            case POSTGRES:
                return new PostgresCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            case MONGODB:
                return new MongodbCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            case SQL_SERVER:
                return new SqlServerCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            case ORACLE:
                return new OracleCdcSource(
                        sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
            default:
                throw ValidatorUtils.invalidArgument("Unknown source type: " + sourceType);
        }
    }
}
