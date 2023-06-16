package com.risingwave.connector.source;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.ValidatorUtils;
import com.risingwave.proto.ConnectorServiceProto.*;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceDropHandler {
    static final Logger LOG = LoggerFactory.getLogger(SourceDropHandler.class);
    private final StreamObserver<DropEventStreamResponse> responseObserver;

    public SourceDropHandler(StreamObserver<DropEventStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    private String getPropNotNull(Map<String, String> props, String name) {
        String prop = props.get(name);
        if (prop == null) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' not found, please check the WITH properties", name));
        } else {
            return prop;
        }
    }

    public void handle(DropEventStreamRequest request) {
        try {
            dropReplicationSlot(request);
            // drop replication slot ok
            responseObserver.onNext(DropEventStreamResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.warn("drop replication slot failed", e);
            responseObserver.onNext(
                    DropEventStreamResponse.newBuilder()
                            .setError(DropError.newBuilder().setErrorMessage(e.toString()).build())
                            .build());
            responseObserver.onCompleted();
        }
    }

    private void dropReplicationSlot(DropEventStreamRequest request) throws Exception {
        SourceType sourceType = request.getSourceType();
        if (sourceType != SourceType.POSTGRES) {
            throw ValidatorUtils.invalidArgument("unexpected type " + sourceType.toString());
        }
        Map<String, String> userProps = request.getPropertiesMap();
        String dbHost = getPropNotNull(userProps, DbzConnectorConfig.HOST);
        String dbPort = getPropNotNull(userProps, DbzConnectorConfig.PORT);
        String dbName = getPropNotNull(userProps, DbzConnectorConfig.DB_NAME);
        String jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbPort, dbName);

        String user = getPropNotNull(userProps, DbzConnectorConfig.USER);
        String password = getPropNotNull(userProps, DbzConnectorConfig.PASSWORD);
        String slotName = getPropNotNull(userProps, DbzConnectorConfig.PG_SLOT_NAME);
        Connection jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);
        try (var stmt = jdbcConnection.prepareStatement("select pg_drop_replication_slot(?)")) {
            stmt.setString(1, slotName);
            stmt.executeQuery();
        }
    }
}
