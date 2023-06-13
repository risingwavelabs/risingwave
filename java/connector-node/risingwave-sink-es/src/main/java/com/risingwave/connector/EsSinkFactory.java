package com.risingwave.connector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.Catalog;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EsSinkFactory implements SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EsSinkFactory.class);

    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        EsSinkConfig config = mapper.convertValue(tableProperties, EsSinkConfig.class);
        return new EsSink(config, tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, Catalog.SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        EsSinkConfig config = mapper.convertValue(tableProperties, EsSinkConfig.class);

        String esUrl = config.getEsUrl();
        String index = config.getIndex();
        Set<String> jdbcColumns = new HashSet<>();
        Set<String> jdbcPk = new HashSet<>();
        Set<String> jdbcTableNames = new HashSet<>();

        // 1. check url
        // 2. The user is not allowed to define the primary key for upsert JDBC sink.
    }
}
