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

package com.risingwave.connector.source.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PostgresValidatorTest {
    @Test
    public void testExactTextTypesRequireBinaryOrdering() {
        assertTrue(PostgresValidator.primaryKeyTypeRequiresUtf8BinaryOrdering("id", "text"));
        assertTrue(PostgresValidator.primaryKeyTypeRequiresUtf8BinaryOrdering("id", "varchar"));
        assertFalse(PostgresValidator.primaryKeyTypeRequiresUtf8BinaryOrdering("id", "uuid"));
    }

    @Test
    public void testUnsupportedPrimaryKeyTypesAreRejected() {
        assertThrows(
                RuntimeException.class,
                () -> PostgresValidator.primaryKeyTypeRequiresUtf8BinaryOrdering("id", "bpchar"));
        assertThrows(
                RuntimeException.class,
                () -> PostgresValidator.primaryKeyTypeRequiresUtf8BinaryOrdering("id", "interval"));
    }

    @Test
    public void testBinaryOrderingRequiresUtf8ServerEncoding() {
        PostgresValidator.validateServerEncodingForBinaryOrdering("UTF8");
        PostgresValidator.validateServerEncodingForBinaryOrdering("utf8");
        assertThrows(
                RuntimeException.class,
                () -> PostgresValidator.validateServerEncodingForBinaryOrdering("LATIN1"));
    }
}
