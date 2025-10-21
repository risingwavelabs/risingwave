package com.risingwave.runner;

import static org.junit.Assert.assertNotNull;

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
        PrivateKey key = JDBCSqlRunner.loadPrivateKeyFromPem(testPem, null);
        assertNotNull(key);
    }
}
