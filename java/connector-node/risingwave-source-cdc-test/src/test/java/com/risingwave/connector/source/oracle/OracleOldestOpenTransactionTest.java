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
import com.risingwave.connector.source.common.JniOracleExternalTable;
import com.risingwave.proto.ConnectorServiceProto;
import java.sql.Connection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleOldestOpenTransactionTest extends OracleSourceTestBase {
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
    public void reportsOldestStartScnUntilCommitOrRollback() throws Exception {
        createTable(
                "APP.OPEN_TRANSACTIONS",
                "CREATE TABLE APP.OPEN_TRANSACTIONS (ID NUMBER(9) PRIMARY KEY, VALUE NUMBER(9))");
        execute("INSERT INTO APP.OPEN_TRANSACTIONS VALUES (1, 0)");
        execute("INSERT INTO APP.OPEN_TRANSACTIONS VALUES (2, 0)");
        assertEquals(0, readOldestTransactions().getOldestOpenTransactionScnsCount());

        try (var first = oracle.connectAsSystem(OracleTestFixture.PDB);
                var second = oracle.connectAsSystem(OracleTestFixture.PDB)) {
            first.setAutoCommit(false);
            second.setAutoCommit(false);
            try {
                SourceTestClient.performQuery(
                        first, "UPDATE APP.OPEN_TRANSACTIONS SET VALUE = 1 WHERE ID = 1");
                var firstScn = transactionStartScn(first);
                assertOldestStartScn(firstScn);

                // A separate committed write advances Oracle's SCN before the second transaction.
                execute("INSERT INTO APP.OPEN_TRANSACTIONS VALUES (3, 0)");
                SourceTestClient.performQuery(
                        second, "UPDATE APP.OPEN_TRANSACTIONS SET VALUE = 2 WHERE ID = 2");
                var secondScn = transactionStartScn(second);
                assertTrue(secondScn > firstScn);
                assertOldestStartScn(firstScn);

                first.commit();
                assertOldestStartScn(secondScn);
                second.rollback();
                assertEquals(0, readOldestTransactions().getOldestOpenTransactionScnsCount());
            } finally {
                // Release row locks before the base fixture drops the table, including on failure.
                first.rollback();
                second.rollback();
            }
        }
    }

    private long transactionStartScn(Connection connection) throws Exception {
        try (var result =
                SourceTestClient.performQuery(
                        connection,
                        "SELECT t.START_SCN FROM V$TRANSACTION t "
                                + "JOIN V$SESSION s ON s.TADDR = t.ADDR "
                                + "WHERE s.SID = TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))")) {
            var scn = result.getLong(1);
            assertTrue(scn > 0);
            return scn;
        }
    }

    private void assertOldestStartScn(long expected) throws Exception {
        var response = readOldestTransactions();
        assertEquals(1, response.getOldestOpenTransactionScnsCount());
        var transaction = response.getOldestOpenTransactionScns(0);
        assertEquals(expected, transaction.getStartScn());
        // Oracle Free is single-instance; zero denotes the non-RAC query path.
        assertEquals(0, transaction.getInstanceId());
    }

    private ConnectorServiceProto.OracleExternalTableResponse readOldestTransactions()
            throws Exception {
        var request =
                ConnectorServiceProto.OracleExternalTableRequest.newBuilder()
                        .putAllProperties(oracle.sourceProperties())
                        .build();
        var response =
                ConnectorServiceProto.OracleExternalTableResponse.parseFrom(
                        JniOracleExternalTable.oldestOpenTransactionScns(request.toByteArray()));
        assertEquals("", response.getError().getErrorMessage());
        return response;
    }
}
