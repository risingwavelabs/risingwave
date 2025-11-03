/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.cdc.debezium.internal;

import io.debezium.bean.StandardBeanNames;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.snapshot.mode.NoDataSnapshotter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoDataRecoverySnapshotter extends NoDataSnapshotter {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoDataRecoverySnapshotter.class);

    @Override
    public String name() {
        return "no_data_recovery";
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        final DatabaseSchema databaseSchema =
                beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, DatabaseSchema.class);

        if (!databaseSchema.isHistorized()) {
            return false;
        }

        boolean should = !((HistorizedDatabaseSchema) databaseSchema).historyExists();
        LOGGER.debug("shouldSnapshotSchema {}", should);
        return should;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return true;
    }
}
