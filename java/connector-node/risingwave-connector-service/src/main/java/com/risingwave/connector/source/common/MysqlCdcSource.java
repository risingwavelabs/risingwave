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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import java.util.Map;

public class MysqlCdcSource extends CdcSource {
    MysqlCdcSource(
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        super(sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.MYSQL;
    }

    @Override
    public void validate(TableSchema tableSchema, boolean isBackfillTable) throws Exception {
        var userProps = getUserProps();
        CdcSourceValidator.validateHeartbeatInterval(userProps);
        CdcSourceValidator.validateHeartbeatTableAutoInitialize(userProps, getSourceType());
        CdcSourceValidator.validateQueueMemoryRatio(userProps);
        CdcSourceValidator.ensureRequiredProps(userProps, isCdcSourceJob());
        CdcSourceValidator.ensurePropNotBlank(userProps, MysqlDbzConnectorConfig.SERVER_ID);
        try (var validator =
                new MySqlValidator(
                        getSourceType(),
                        userProps,
                        tableSchema,
                        isCdcSourceJob(),
                        isBackfillTable)) {
            validator.validateAll();
        }
    }
}
