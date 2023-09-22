package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import java.sql.SQLException;
import java.util.Map;

public class CitusValidator extends PostgresValidator {
    public CitusValidator(Map<String, String> userProps, TableSchema tableSchema)
            throws SQLException {
        super(userProps, tableSchema);
    }

    @Override
    protected void alterPublicationIfNeeded() throws SQLException {
        // do nothing for citus worker node,
        // since we created a FOR ALL TABLES publication when creating the connector
    }
}
