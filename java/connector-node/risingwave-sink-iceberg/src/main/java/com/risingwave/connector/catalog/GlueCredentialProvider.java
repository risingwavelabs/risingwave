package com.risingwave.connector.catalog;

import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.utils.Validate;

public class GlueCredentialProvider implements AwsCredentialsProvider {
    private final AwsCredentials credentials;

    private GlueCredentialProvider(AwsCredentials credentials) {
        this.credentials =
                (AwsCredentials)
                        Validate.notNull(
                                credentials, "Credentials must not be null.", new Object[0]);
    }

    public static GlueCredentialProvider create(Map<String, String> config) {
        return new GlueCredentialProvider(
                AwsBasicCredentials.create(
                        config.get("glue.access-key-id"), config.get("glue.secret-access-key")));
    }

    public AwsCredentials resolveCredentials() {
        return this.credentials;
    }
}
