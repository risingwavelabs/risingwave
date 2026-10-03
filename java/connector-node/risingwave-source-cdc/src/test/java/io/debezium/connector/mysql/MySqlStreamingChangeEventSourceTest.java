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

package io.debezium.connector.mysql;

import static io.debezium.connector.mysql.MySqlStreamingChangeEventSource.unwrapSetStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.SchemaHistory;
import java.util.function.Predicate;
import org.junit.Test;

public class MySqlStreamingChangeEventSourceTest {
    @Test
    public void unwrapsMixedCaseAndMultilineStatements() {
        assertEquals(
                "flush table",
                unwrapSetStatement("SET STATEMENT max_statement_time=60 FOR flush table"));
        assertEquals(
                "FLUSH TABLES;",
                unwrapSetStatement(
                        " \nSeT\tStAtEmEnT max_statement_time=60,\nlock_wait_timeout=5\nfOr FLUSH TABLES;"));
    }

    @Test
    public void ignoresForInQuotedValuesAndComments() {
        String ddl = "ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'keep FOR intact'";
        for (String value :
                new String[] {
                    "' FOR '",
                    "\" FOR \"",
                    "'escaped \\' FOR value'",
                    "'doubled '' FOR value'",
                    "'supplementary 😀 FOR value'",
                    "SUBSTRING('value' FROM 1 FOR 2)"
                }) {
            assertEquals(
                    ddl,
                    unwrapSetStatement(
                            "/* FOR */ SET /* FOR */ STATEMENT `variable FOR name`="
                                    + value
                                    + " /* FOR */ FOR "
                                    + ddl));
        }
        assertEquals(
                ddl, unwrapSetStatement("SET STATEMENT lock_wait_timeout=5 -- FOR\nFOR " + ddl));
    }

    @Test
    public void leavesOrdinarySqlUnchanged() {
        for (String sql :
                new String[] {
                    "FLUSH TABLES",
                    "BEGIN",
                    "COMMIT",
                    "SET lock_wait_timeout=5",
                    "SET STATEMENT_TIMEOUT=5",
                    "ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'SET STATEMENT x=1 FOR text'",
                    ""
                }) {
            assertEquals(sql, unwrapSetStatement(sql));
        }
    }

    @Test
    public void leavesIncompleteWrappersUnchanged() {
        for (String sql :
                new String[] {
                    "SET STATEMENT lock_wait_timeout=5",
                    "SET STATEMENT lock_wait_timeout=5 FOR ",
                    "SET STATEMENT lock_wait_timeout=5; SELECT ' FOR '"
                }) {
            assertEquals(sql, unwrapSetStatement(sql));
        }
    }

    @Test
    public void parsesWrappedFlushStatements() {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        Tables tables = new Tables();
        for (String sql :
                new String[] {
                    "SET STATEMENT max_statement_time=60 FOR flush table",
                    "SET STATEMENT max_statement_time=60 FOR FLUSH TABLES"
                }) {
            parser.parse(unwrapSetStatement(sql), tables);
        }
        assertTrue(tables.tableIds().isEmpty());
    }

    @Test
    public void preservesWrappedSchemaChanges() {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        parser.setCurrentSchema("testdb");
        Tables tables = new Tables();
        parser.parse("CREATE TABLE t (id INT PRIMARY KEY)", tables);

        parser.parse(
                unwrapSetStatement(
                        "SET STATEMENT lock_wait_timeout=5 FOR ALTER TABLE t ADD COLUMN c INT"),
                tables);
        assertNotNull(tables.forTable(new TableId("testdb", null, "t")).columnWithName("c"));
    }

    @Test
    public void appliesDefaultFiltersToUnwrappedStatements() {
        Predicate<String> filter = connectorConfig(Configuration.create()).ddlFilter();
        assertTrue(
                filter.test(
                        unwrapSetStatement(
                                "SET STATEMENT max_statement_time=60 FOR FLUSH RELAY LOGS")));
        assertTrue(
                filter.test(
                        unwrapSetStatement(
                                "set statement max_statement_time=60 for flush relay logs")));
        assertTrue(
                filter.test(
                        unwrapSetStatement(
                                "SET STATEMENT max_statement_time=60 FOR DELETE FROM mysql.rds_sysinfo")));
        assertFalse(
                filter.test(
                        unwrapSetStatement(
                                "SET STATEMENT lock_wait_timeout=5 FOR ALTER TABLE t ADD COLUMN c INT")));
    }

    @Test
    public void appliesCustomFilterToUnwrappedStatement() {
        Predicate<String> filter =
                connectorConfig(
                                Configuration.create()
                                        .with(SchemaHistory.DDL_FILTER, "(?i)FLUSH TABLES?"))
                        .ddlFilter();
        assertTrue(
                filter.test(
                        unwrapSetStatement("SET STATEMENT max_statement_time=60 FOR flush table")));
    }

    private static MySqlConnectorConfig connectorConfig(Configuration.Builder builder) {
        return new MySqlConnectorConfig(
                builder.with("topic.prefix", "test")
                        .with("database.hostname", "localhost")
                        .with("database.user", "test")
                        .with("database.server.id", 1)
                        .build());
    }
}
