package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.FlinkSinkTableSchemaFinder;
import io.grpc.StatusRuntimeException;
import java.util.List;

/*
 * The HTTP sink implementation of `FlinkSinkTableSchemaFinder`, requiring no additional operations.
 */
public class HttpFlinkSinkTableSchemaFinder implements FlinkSinkTableSchemaFinder {

    public HttpFlinkSinkTableSchemaFinder() {
        return;
    }

    @Override
    public void validate(List<ColumnDesc> rwColumnDescs) throws StatusRuntimeException {
        // Don't need to check schema
        return;
    }
}
