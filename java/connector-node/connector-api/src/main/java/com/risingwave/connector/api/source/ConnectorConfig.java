// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.api.source;

import java.util.HashMap;
import java.util.Map;

public class ConnectorConfig {
    /* Common configs */
    public static final String HOST = "hostname";
    public static final String PORT = "port";
    public static final String USER = "username";
    public static final String PASSWORD = "password";

    public static final String DB_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";

    /* MySQL specified configs */
    public static final String MYSQL_SERVER_ID = "server.id";

    /* Postgres specified configs */
    public static final String PG_SLOT_NAME = "slot.name";
    public static final String PG_SCHEMA_NAME = "schema.name";

    public static Map<String, String> extractDebeziumProperties(Map<String, String> properties) {
        // retain only debezium properties if any
        var userProps = new HashMap<>(properties);
        userProps.remove(ConnectorConfig.HOST);
        userProps.remove(ConnectorConfig.PORT);
        userProps.remove(ConnectorConfig.USER);
        userProps.remove(ConnectorConfig.PASSWORD);
        userProps.remove(ConnectorConfig.DB_NAME);
        userProps.remove(ConnectorConfig.TABLE_NAME);
        userProps.remove(ConnectorConfig.MYSQL_SERVER_ID);
        userProps.remove(ConnectorConfig.PG_SLOT_NAME);
        userProps.remove(ConnectorConfig.PG_SCHEMA_NAME);
        return userProps;
    }
}
