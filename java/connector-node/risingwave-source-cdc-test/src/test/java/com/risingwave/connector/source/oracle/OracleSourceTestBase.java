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

import com.risingwave.connector.source.SourceTestClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;

abstract class OracleSourceTestBase {
    private final Set<String> tables = new HashSet<>();
    protected Connection pdb;

    protected abstract OracleTestFixture oracle();

    @Before
    public void connectToPdb() {
        pdb = oracle().connectAsSystem(OracleTestFixture.PDB);
    }

    @After
    public void cleanUpTables() throws SQLException {
        if (pdb == null) {
            return;
        }
        try {
            for (var table : tables) {
                SourceTestClient.performQuery(pdb, "DROP TABLE " + table + " PURGE");
            }
        } finally {
            pdb.close();
        }
    }

    protected void createTable(String name, String sql) {
        execute(sql);
        tables.add(name);
    }

    protected void execute(String sql) {
        SourceTestClient.performQuery(pdb, sql);
    }
}
