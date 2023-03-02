package com.risingwave.connector.api.source;

import com.risingwave.proto.ConnectorServiceProto;

public enum SourceTypeE {
    MYSQL,
    POSTGRES,
    INVALID;

    public static SourceTypeE valueOf(ConnectorServiceProto.SourceType type) {
        switch (type) {
            case MYSQL:
                return SourceTypeE.MYSQL;
            case POSTGRES:
                return SourceTypeE.POSTGRES;
            default:
                return SourceTypeE.INVALID;
        }
    }
}
