/*
 * Copyright 2025 RisingWave Labs
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

package com.risingwave.runner;

import static org.junit.Assert.assertNotNull;

import com.risingwave.connector.SnowflakeJDBCSinkConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import org.junit.Test;

public class JDBCSqlRunnerTest {

    private String loadTestPem() throws IOException {
        try (InputStream is =
                getClass().getClassLoader().getResourceAsStream("test-private-key.pem")) {
            if (is == null) {
                throw new IOException("Test PEM file not found in resources");
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void loadPrivateKeyFromPem_unencrypted() throws Exception {
        String testPem = loadTestPem();
        PrivateKey key = SnowflakeJDBCSinkConfig.loadPrivateKeyFromPem(testPem, null);
        assertNotNull(key);
    }
}
