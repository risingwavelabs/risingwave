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

package com.risingwave.connector.source.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.source.SourceTestClient;
import java.sql.SQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleSourceValidationTest extends OracleSourceTestBase {
    private static OracleTestFixture oracle;

    @BeforeClass
    public static void startServices() throws Exception {
        oracle = new OracleTestFixture();
        oracle.start();
    }

    @AfterClass
    public static void stopServices() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
    }

    @Override
    protected OracleTestFixture oracle() {
        return oracle;
    }

    @Test
    public void validatesPdbWithoutRequiringContainerDataAll() {
        createSourceTableWithAllColumnLogging();
        assertValid(oracle.sourceProperties());
    }

    @Test
    public void rejectsMissingLogging() {
        createSourceTable();
        assertLoggingRejected();
    }

    @Test
    public void rejectsPrimaryKeyOnlyLogging() {
        createSourceTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
        assertLoggingRejected();
    }

    @Test
    public void rejectsPartialConditionalGroup() {
        createSourceTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG GROUP PARTIAL_LOG (NAME)");
        assertLoggingRejected();
    }

    @Test
    public void rejectsPartialUnconditionalGroup() {
        createSourceTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG GROUP PARTIAL_LOG (NAME) ALWAYS");
        assertLoggingRejected();
    }

    @Test
    public void acceptsTableAllColumnLogging() {
        createSourceTableWithAllColumnLogging();
        assertValid(oracle.sourceProperties());
    }

    @Test
    public void rejectsAllColumnLoggingOnAnotherTable() {
        createSourceTable();
        createTable("APP.OTHER_TABLE", "CREATE TABLE APP.OTHER_TABLE (ID NUMBER)");
        execute("ALTER TABLE APP.OTHER_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        assertLoggingRejected();
    }

    @Test
    public void acceptsPdbAllColumnLoggingWithoutTableGroup() throws SQLException {
        createSourceTable();
        execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        try {
            try (var cdb = oracle.connectAsSystem(OracleTestFixture.CDB);
                    var result =
                            SourceTestClient.performQuery(
                                    cdb, "SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")) {
                assertEquals("NO", result.getString(1));
            }
            assertValid(oracle.sourceProperties());
        } finally {
            execute("ALTER DATABASE DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
        assertLoggingRejected();
    }

    @Test
    public void acceptsCdbAllColumnLoggingWithoutTableGroup() throws SQLException {
        createSourceTable();
        try (var cdb = oracle.connectAsSystem(OracleTestFixture.CDB)) {
            SourceTestClient.performQuery(
                    cdb, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            try {
                assertValid(oracle.sourceProperties());
            } finally {
                SourceTestClient.performQuery(
                        cdb, "ALTER DATABASE DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            }
        }
        assertLoggingRejected();
    }

    private void assertLoggingRejected() {
        var error = validate(oracle.sourceProperties()).getError().getErrorMessage();
        assertTrue(error, error.contains("all-column supplemental logging"));
    }
}
