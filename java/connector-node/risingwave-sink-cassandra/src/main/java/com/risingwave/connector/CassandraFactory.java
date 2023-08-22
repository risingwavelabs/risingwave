package com.risingwave.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.api.sink.SinkWriterV1;
import com.risingwave.proto.Catalog.SinkType;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraFactory implements SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraFactory.class);

    public SinkWriter createWriter(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        CassandraConfig config = mapper.convertValue(tableProperties, CassandraConfig.class);
        return new SinkWriterV1.Adapter(new CassandraSink(tableSchema, config));
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        CassandraConfig config = mapper.convertValue(tableProperties, CassandraConfig.class);

        // 1. check url
        String url = config.getUrl();
        String[] hostPort = url.split(":");
        if (hostPort.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid cassandraURL: expected `host:port`, got " + url);
        }
        for (String s : hostPort) {
            System.out.println(s);
        }
        // 2. check connection
        CqlSessionBuilder sessionBuilder =
                CqlSession.builder()
                        .addContactPoint(
                                new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])))
                        .withKeyspace(config.getKeyspace())
                        .withLocalDatacenter(config.getDatacenter());
        if (config.getUsername() != null && config.getPassword() != null) {
            sessionBuilder =
                    sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
        }
        CqlSession session = sessionBuilder.build();
        if (session.isClosed()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Cannot connect to " + config.getUrl())
                    .asRuntimeException();
        }
        // 3. close client
        session.close();
    }
}
