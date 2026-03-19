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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Convert PostgreSQL composite textual value into a JSON string.
 *
 * <p>Example:
 *
 * <ul>
 *   <li>Input: <code>(USD,66.66)</code>
 *   <li>Output: <code>{"currency":"USD","amount":"66.66"}</code>
 * </ul>
 *
 * <p>Config:
 *
 * <ul>
 *   <li><code>types</code>: comma-separated PostgreSQL type names to handle, or <code>*</code> for
 *       all types
 *   <li><code>fields.&lt;type_name&gt;</code>: comma-separated field names for each type
 * </ul>
 */
public class PostgresCompositeToJsonConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final String TYPES_CONFIG = "types";
    private static final String FIELDS_PREFIX = "fields.";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Set<String> enabledTypes = Set.of();
    private Map<String, List<String>> typeToFields = Map.of();
    private boolean applyAllTypes = false;

    @Override
    public void configure(Properties props) {
        enabledTypes =
                Arrays.stream(props.getProperty(TYPES_CONFIG, "").split(","))
                        .map(String::trim)
                        .map(s -> s.toLowerCase(Locale.ROOT))
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toSet());
        applyAllTypes = enabledTypes.contains("*");

        Map<String, List<String>> fieldsMap = new LinkedHashMap<>();
        for (String typeName : enabledTypes) {
            String key = FIELDS_PREFIX + typeName;
            String fields = props.getProperty(key, "");
            List<String> fieldNames =
                    Arrays.stream(fields.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
            if (!fieldNames.isEmpty()) {
                fieldsMap.put(typeName, fieldNames);
            }
        }
        typeToFields = fieldsMap;
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String fullTypeName = normalizeTypeName(column.typeName());
        String shortTypeName = unqualifyTypeName(fullTypeName);
        if (!applyAllTypes
                && !enabledTypes.contains(fullTypeName)
                && !enabledTypes.contains(shortTypeName)) {
            return;
        }

        List<String> fieldNames = typeToFields.get(fullTypeName);
        if (fieldNames == null || fieldNames.isEmpty()) {
            fieldNames = typeToFields.get(shortTypeName);
        }
        final List<String> finalFieldNames = fieldNames == null ? List.of() : fieldNames;

        // Emit as nullable JSON string.
        SchemaBuilder schemaBuilder =
                SchemaBuilder.string().name("rw.cdc.postgres.composite.json.string").optional();

        registration.register(
                schemaBuilder, input -> convertCompositeToJson(input, finalFieldNames));
    }

    private static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        return typeName.toLowerCase(Locale.ROOT).trim().replace("\"", "");
    }

    private static String unqualifyTypeName(String typeName) {
        int idx = typeName.lastIndexOf('.');
        if (idx >= 0 && idx + 1 < typeName.length()) {
            return typeName.substring(idx + 1);
        }
        return typeName;
    }

    private static String convertCompositeToJson(Object input, List<String> fieldNames) {
        if (input == null) {
            return null;
        }

        String text = toText(input);
        if (text == null || text.isEmpty()) {
            return null;
        }

        List<String> values = parseCompositeText(text);
        if (values == null) {
            // Keep original value on parse failure to avoid hard-failing CDC stream.
            return text;
        }

        if (fieldNames.isEmpty()) {
            try {
                return OBJECT_MAPPER.writeValueAsString(values);
            } catch (JsonProcessingException e) {
                return text;
            }
        }

        if (values.size() != fieldNames.size()) {
            // Fallback to JSON array when field-name mapping does not match value arity.
            try {
                return OBJECT_MAPPER.writeValueAsString(values);
            } catch (JsonProcessingException e) {
                return text;
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            out.put(fieldNames.get(i), values.get(i));
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            return text;
        }
    }

    private static String toText(Object input) {
        if (input instanceof String) {
            return (String) input;
        }
        if (input instanceof byte[]) {
            return new String((byte[]) input, StandardCharsets.UTF_8);
        }
        return String.valueOf(input);
    }

    // Parse PostgreSQL composite text like "(a,b)" with basic quote/escape handling.
    private static List<String> parseCompositeText(String text) {
        if (text.length() < 2 || text.charAt(0) != '(' || text.charAt(text.length() - 1) != ')') {
            return null;
        }
        String body = text.substring(1, text.length() - 1);
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;

        for (int i = 0; i < body.length(); i++) {
            char ch = body.charAt(i);
            if (escaped) {
                current.append(ch);
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inQuotes = !inQuotes;
                continue;
            }
            if (ch == ',' && !inQuotes) {
                parts.add(normalizeField(current.toString()));
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        parts.add(normalizeField(current.toString()));
        return parts;
    }

    private static String normalizeField(String s) {
        String field = s.trim();
        if ("NULL".equalsIgnoreCase(field)) {
            return null;
        }
        return field;
    }
}
