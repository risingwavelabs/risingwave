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
    public void redshiftUrlCredentialsTakePrecedence() throws Exception {
        String baseUrl = "jdbc:redshift://example.invalid:5439/dev";
        for (String parameters :
                new String[] {
                    "?user=url%2Buser&PASSWORD=Url%2BPass9%21",
                    ";USER=url%2Buser;password=Url%2BPass9%21",
                    "?user=older;USER=url%2Buser&password=older;PASSWORD=Url%2BPass9%21"
                }) {
            RedshiftProperties props = new RedshiftProperties();
            props.setProperty("user", "separate_user");
            props.setProperty("password", "Separate+&%2BPass9!");
            RedshiftProperties parsed = Driver.parseURL(baseUrl + parameters, props);
            assertNotNull(parsed);
            assertEquals("url+user", parsed.getProperty("user"));
            assertEquals("Url+Pass9!", parsed.getProperty("password"));
        }
    }

    @Test
    public void redshiftMixedDelimitersPreserveDriverSemantics() throws Exception {
        RedshiftProperties props = new RedshiftProperties();
        props.setProperty("password", "Separate+&%2BPass9!");
        RedshiftProperties parsed =
                Driver.parseURL(
                        "jdbc:redshift://example.invalid:5439/dev;ApplicationName=app?password=old",
                        props);
        assertNotNull(parsed);
        // Removing the query would turn part of the database name into a URL parameter.
        assertEquals("dev;ApplicationName=app", parsed.getProperty("dbname"));
        assertEquals("old", parsed.getProperty("password"));

        props = new RedshiftProperties();
        props.setProperty("password", "Separate+&%2BPass9!");
        parsed =
                Driver.parseURL(
                        "jdbc:redshift:iam://cluster:us-east-1/dev;password=old?ssl=true", props);
        assertNotNull(parsed);
        // IAM parsing can recognize credentials before the question mark.
        assertEquals("dev", parsed.getProperty("dbname"));
        assertEquals("true", parsed.getProperty("iamauth"));
        assertEquals("old", parsed.getProperty("password"));
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
