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
