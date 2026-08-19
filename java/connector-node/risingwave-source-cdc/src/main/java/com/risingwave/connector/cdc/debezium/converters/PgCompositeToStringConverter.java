/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.cdc.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.sql.Types;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Converts PostgreSQL composite values (and arrays of composites) into plain strings. */
public class PgCompositeToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // PG FirstNormalObjectId: OIDs >= this are user-defined types.
    // Built-in array types (e.g. _int4, _text) sit below this threshold and go
    // through Debezium's standard handlers untouched.
    private static final int PG_FIRST_USER_OID = 16384;

    private boolean debug;

    @Override
    public void configure(Properties props) {
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (debug) {
            System.err.printf(
                    "PgCompositeToStringConverter field=%s.%s jdbcType=%d nativeType=%d typeName=%s typeExpression=%s optional=%s%n",
                    column.dataCollection(),
                    column.name(),
                    column.jdbcType(),
                    column.nativeType(),
                    column.typeName(),
                    column.typeExpression(),
                    column.isOptional());
        }

        if (column.jdbcType() == Types.STRUCT) {
            // Always optional: PG DELETE before-image fills non-PK columns
            // with null under REPLICA IDENTITY DEFAULT, regardless of
            // upstream NOT NULL.
            registration.register(
                    SchemaBuilder.string().optional(), PgTextConversionUtils::convertScalar);

            return;
        }
        if (isUserDefinedArray(column)) {
            var elementSchema = SchemaBuilder.string().optional().build();
            registration.register(
                    SchemaBuilder.array(elementSchema).optional(),
                    PgTextConversionUtils::convertArray);
        }
    }

    private static boolean isUserDefinedArray(RelationalColumn column) {
        // ARRAY columns whose element OID is user-defined are treated as
        // arrays of composites. Built-in arrays (int[], text[], ...) have OIDs
        // below PG_FIRST_USER_OID and are skipped. User-defined enum/domain
        // arrays will also match and get rendered as text — acceptable
        // since the downstream column is varchar[] anyway.
        //
        // Do not intercept extension arrays that Debezium already knows how to
        // encode with a native schema. For example, PostGIS geometry[] arrives
        // as an array of geometry structs and RisingWave decodes each element
        // as EWKB bytea. Re-registering it as array(string) would make the
        // downstream bytea parser treat hex EWKB text as base64.

        return column.jdbcType() == Types.ARRAY
                && column.nativeType() >= PG_FIRST_USER_OID
                && !isDebeziumNativeExtensionArray(column);
    }

    /**
     * Extension arrays that Debezium already encodes with a native schema. PostGIS {@code
     * geometry[]} arrives as an array of geometry structs that RisingWave decodes per element as
     * EWKB bytea; re-registering it as array(string) would make the downstream bytea parser treat
     * the hex EWKB text as base64.
     */
    private static boolean isDebeziumNativeExtensionArray(RelationalColumn column) {
        return PgTextConversionUtils.isPostgresArrayOf(column, "geometry")
                || PgTextConversionUtils.isPostgresArrayOf(column, "geography");
    }
}
