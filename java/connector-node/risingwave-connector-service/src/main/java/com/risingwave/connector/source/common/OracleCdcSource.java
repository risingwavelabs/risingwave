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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class OracleCdcSource extends CdcSource {
    OracleCdcSource(
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        super(sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
    }

    public static String getJdbcUrl(String host, String port, String database) {
        return String.format("jdbc:oracle:thin:@//%s:%s/%s", host, port, database);
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.ORACLE;
    }

    @Override
    public void validate(TableSchema tableSchema, boolean isBackfillTable) throws Exception {
        var userProps = getUserProps();
        CdcSourceValidator.validateHeartbeatInterval(userProps);
        CdcSourceValidator.validateHeartbeatTableAutoInitialize(userProps, getSourceType());
        CdcSourceValidator.validateQueueMemoryRatio(userProps);
        CdcSourceValidator.ensureRequiredProps(userProps, isCdcSourceJob());
        CdcSourceValidator.ensurePropNotBlank(userProps, OracleDbzConnectorConfig.PDB_NAME);
        CdcSourceValidator.ensurePropNotBlank(userProps, DbzConnectorConfig.SCHEMA_NAME);
        CdcSourceValidator.ensurePropNotBlank(
                userProps, OracleDbzConnectorConfig.HEARTBEAT_TABLE_NAME);
        if (userProps.containsKey(OracleDbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY)) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' is generated internally; configure '%s' instead",
                            OracleDbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY,
                            OracleDbzConnectorConfig.HEARTBEAT_TABLE_NAME));
        }
        var adapter = userProps.get(OracleDbzConnectorConfig.CONNECTION_ADAPTER_KEY);
        if (adapter != null
                && !OracleDbzConnectorConfig.SUPPORTED_CONNECTION_ADAPTERS.contains(
                        adapter.trim().toLowerCase(Locale.ROOT))) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' must be 'logminer' or 'logminer_unbuffered', got '%s'",
                            OracleDbzConnectorConfig.CONNECTION_ADAPTER_KEY, adapter));
        }
        try (var validator = new OracleValidator(userProps, tableSchema, isCdcSourceJob())) {
            validator.validateAll();
        }
    }

    /**
     * Adds the commit SCN of a data event to its offset as {@code decoded_commit_scn}, by which the
     * backfill orders change events against the snapshot SCN. Debezium's own {@code commit_scn} in
     * the offset is a per-redo-thread recovery map, not the SCN of this event.
     */
    @Override
    public Map<String, ?> getEventSourceOffset(SourceRecord record, CdcEventType eventType) {
        var sourceOffset = record.sourceOffset();
        if (eventType != CdcEventType.DATA || record.value() == null) {
            return sourceOffset;
        }
        var source = ((Struct) record.value()).getStruct("source");
        if (source == null || source.getString("commit_scn") == null) {
            throw new CdcConnectorException("Oracle data event is missing source.commit_scn");
        }
        Map<String, Object> comparisonOffset = new HashMap<>(sourceOffset);
        comparisonOffset.put("decoded_commit_scn", source.getString("commit_scn"));
        return comparisonOffset;
    }
}
