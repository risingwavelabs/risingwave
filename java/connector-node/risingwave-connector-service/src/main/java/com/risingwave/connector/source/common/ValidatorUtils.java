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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import io.grpc.Status;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidatorUtils {
    static final Logger LOG = LoggerFactory.getLogger(ValidatorUtils.class);

    public static final String CONFIGED_SQL_FILE = "validate_sql.properties";

    public static RuntimeException invalidArgument(String description) {
        return Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }

    public static RuntimeException internalError(String description) {
        return Status.INTERNAL.withDescription(description).asRuntimeException();
    }

    public static RuntimeException internalError(Throwable cause) {
        return Status.INTERNAL.withCause(cause).asRuntimeException();
    }

    private static final Properties storedSqls;

    static {
        var props = new Properties();
        try (var input =
                ValidatorUtils.class.getClassLoader().getResourceAsStream(CONFIGED_SQL_FILE)) {
            props.load(input);
        } catch (IOException e) {
            LOG.error("failed to load sql statements", e);
            throw ValidatorUtils.internalError(e.getMessage());
        }
        storedSqls = props;
    }

    public static String getSql(String name) {
        assert (storedSqls != null);
        return storedSqls.getProperty(name);
    }

    public static String getJdbcPrefix(SourceTypeE sourceType) {
        switch (sourceType) {
            case MYSQL:
                return "jdbc:mysql";
            case POSTGRES:
            case CITUS:
                return "jdbc:postgresql";
            default:
                throw ValidatorUtils.invalidArgument("Unknown source type: " + sourceType);
        }
    }

    public static String getJdbcUrl(SourceTypeE sourceType, String host, String database) {
        switch (sourceType) {
            case MYSQL:
                return String.format("jdbc:mysql://%s/%s", host, database);
            case POSTGRES:
            case CITUS:
                return String.format("jdbc:postgresql://%s/%s", host, database);
            default:
                throw ValidatorUtils.invalidArgument("Unknown source type: " + sourceType);
        }
    }

    public static boolean isPrimaryKeyMatch(TableSchema sourceSchema, Set<String> pkFields) {
        if (sourceSchema.getPrimaryKeys().size() != pkFields.size()) {
            return false;
        }
        for (var colName : sourceSchema.getPrimaryKeys()) {
            if (!pkFields.contains(colName)) {
                return false;
            }
        }
        return true;
    }
}
