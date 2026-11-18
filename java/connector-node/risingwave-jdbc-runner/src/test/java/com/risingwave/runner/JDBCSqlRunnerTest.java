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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.amazon.redshift.Driver;
import com.amazon.redshift.util.RedshiftProperties;
import com.risingwave.connector.SnowflakeJDBCSinkConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import org.junit.Test;

public class JDBCSqlRunnerTest {

    @Test
    public void redshiftDriverPropertiesPreserveCredentials() throws Exception {
        String username = "user+&%2B=?# 用户";
        for (String password :
                new String[] {
                    "Example+Pass9!", "Example&Pass9!", "Example%2BPass9!", "Example=?# 密码9!"
                }) {
            RedshiftProperties props = new RedshiftProperties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            RedshiftProperties parsed =
                    Driver.parseURL(
                            "jdbc:redshift://example.invalid:5439/dev?ssl=true&ApplicationName=risingwave",
                            props);
            assertNotNull(parsed);
            assertEquals(username, parsed.getProperty("user"));
            assertEquals(password, parsed.getProperty("password"));
            assertEquals("true", parsed.getProperty("ssl"));
            assertEquals("risingwave", parsed.getProperty("ApplicationName"));
        }
    }

    @Test
    public void redshiftPasswordOverrideRequiresRemovingUrlPassword() throws Exception {
        String password = "New+&%2B=?# 密码9!";
        String baseUrl = "jdbc:redshift://example.invalid:5439/dev";
        RedshiftProperties props = new RedshiftProperties();
        props.setProperty("password", password);

        // URL parameters override driver properties, including with mixed-case names.
        RedshiftProperties conflicting =
                Driver.parseURL(baseUrl + "?user=url%2Buser&PASSWORD=old", props);
        assertNotNull(conflicting);
        assertEquals("old", conflicting.getProperty("password"));

        // The Rust configuration builder removes the overridden password from the URL.
        props = new RedshiftProperties();
        props.setProperty("password", password);
        RedshiftProperties parsed = Driver.parseURL(baseUrl + "?user=url%2Buser&ssl=true", props);
        assertNotNull(parsed);
        assertEquals("url+user", parsed.getProperty("user"));
        assertEquals(password, parsed.getProperty("password"));
        assertEquals("true", parsed.getProperty("ssl"));
    }

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
