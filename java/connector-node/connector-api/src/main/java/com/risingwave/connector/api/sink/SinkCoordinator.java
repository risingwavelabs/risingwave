package com.risingwave.connector.api.sink;

import com.risingwave.proto.ConnectorServiceProto;
import java.util.List;

public interface SinkCoordinator {
    void commit(long epoch, List<ConnectorServiceProto.SinkMetadata> metadataList);

    void drop();
}
