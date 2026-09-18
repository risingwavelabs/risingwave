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

package com.risingwave.connector.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import com.risingwave.proto.ConnectorServiceProto.ValidateSourceRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SourceValidateHandlerTest {
    private static final String HEARTBEAT_INTERVAL = "debezium.heartbeat.interval.ms";
    private final SourceType sourceType;

    public SourceValidateHandlerTest(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] sourceTypes() {
        return new Object[] {
            SourceType.MYSQL,
            SourceType.SQL_SERVER,
            SourceType.MONGODB,
            SourceType.POSTGRES,
            SourceType.CITUS,
            SourceType.ORACLE
        };
    }

    @Test
    public void acceptsMissingAndPositiveHeartbeatIntervals() {
        SourceValidateHandler.validateHeartbeatInterval(Map.of(), sourceType);
        for (String value : new String[] {"1", "300000", "2147483647", "+1"}) {
            SourceValidateHandler.validateHeartbeatInterval(
                    Map.of(HEARTBEAT_INTERVAL, value), sourceType);
        }
    }

    @Test
    public void rejectsInvalidHeartbeatIntervals() {
        // Keep these cases aligned with the Rust heartbeat validation tests.
        for (String value :
                new String[] {
                    "-1",
                    "",
                    "invalid",
                    "0.5",
                    "2147483648",
                    "9223372036854775807",
                    "9223372036854775808",
                    " 1",
                    "1 ",
                    "１"
                }) {
            var props = Map.of(HEARTBEAT_INTERVAL, value);
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateHeartbeatInterval(
                                            props, sourceType));
            assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
            assertTrue(error.getMessage().contains("between 0 and 2147483647"));
            assertRejectedBeforeDatabaseValidation(props, "between 0 and 2147483647");
        }
    }

    @Test
    public void validatesHeartbeatTableAutoInitializeOption() {
        var key = DbzConnectorConfig.HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY;
        SourceValidateHandler.validateHeartbeatTableAutoInitialize(Map.of(), sourceType);
        SourceValidateHandler.validateHeartbeatTableAutoInitialize(
                Map.of(key, "false"), sourceType);

        for (String value : new String[] {"TRUE", "False", "1", ""}) {
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateHeartbeatTableAutoInitialize(
                                            Map.of(key, value), sourceType));
            assertTrue(error.getMessage().contains("must be 'true' or 'false'"));
        }

        var enabledProps = Map.of(key, "true", HEARTBEAT_INTERVAL, "1");
        if (sourceType == SourceType.ORACLE) {
            SourceValidateHandler.validateHeartbeatTableAutoInitialize(enabledProps, sourceType);
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateHeartbeatTableAutoInitialize(
                                            Map.of(key, "true", HEARTBEAT_INTERVAL, "0"),
                                            sourceType));
            assertTrue(error.getMessage().contains("requires a positive"));
        } else {
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateHeartbeatTableAutoInitialize(
                                            enabledProps, sourceType));
            assertTrue(error.getMessage().contains("is not supported for connector"));
            assertRejectedBeforeDatabaseValidation(enabledProps, "is not supported for connector");
        }
    }

    @Test
    public void zeroDisablesHeartbeatForMongoDbCitusAndOracle() {
        for (String value : new String[] {"0", "+0", "-0"}) {
            var props = Map.of(HEARTBEAT_INTERVAL, value);
            if (sourceType == SourceType.POSTGRES
                    || sourceType == SourceType.MYSQL
                    || sourceType == SourceType.SQL_SERVER) {
                var error =
                        assertThrows(
                                StatusRuntimeException.class,
                                () ->
                                        SourceValidateHandler.validateHeartbeatInterval(
                                                props, sourceType));
                assertTrue(error.getMessage().contains("must be greater than 0"));
                assertRejectedBeforeDatabaseValidation(props, "must be greater than 0");
            } else {
                SourceValidateHandler.validateHeartbeatInterval(props, sourceType);
            }
        }
    }

    private void assertRejectedBeforeDatabaseValidation(
            Map<String, String> props, String expectedMessage) {
        // Exercise the request entry point without a database, for source and table jobs.
        for (boolean isSourceJob : new boolean[] {false, true}) {
            var request =
                    ValidateSourceRequest.newBuilder()
                            .setSourceType(sourceType)
                            .setIsSourceJob(isSourceJob)
                            .putAllProperties(props)
                            .build();
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () -> SourceValidateHandler.validateSource(request));
            assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
            assertTrue(error.getMessage(), error.getMessage().contains(expectedMessage));
        }
    }
}
