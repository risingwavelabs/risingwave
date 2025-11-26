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

package com.risingwave.connector.catalog;

import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.Validate;

/** This class is used to provide a credential to glue catalog */
public class GlueCredentialProvider implements AwsCredentialsProvider {
    private static final String DEFAULT_SESSION_NAME = "risingwave-glue";
    private final AwsCredentials credentials;

    private GlueCredentialProvider(AwsCredentials credentials) {
        this.credentials =
                (AwsCredentials)
                        Validate.notNull(
                                credentials, "Credentials must not be null.", new Object[0]);
    }

    public static GlueCredentialProvider create(Map<String, String> config) {
        Validate.notNull(config, "Config must not be null");
        String accessKey =
                Validate.notNull(
                        config.get("glue.access-key-id"),
                        "Glue access key must not be null.",
                        new Object[0]);
        String secretKey =
                Validate.notNull(
                        config.get("glue.secret-access-key"),
                        "Glue secret key must not be null.",
                        new Object[0]);

        AwsCredentials baseCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        String assumeRoleArn = config.get("glue.iam-role-arn");
        if (StringUtils.isBlank(assumeRoleArn)) {
            return new GlueCredentialProvider(baseCredentials);
        }

        AwsCredentials assumed =
                assumeRole(
                        baseCredentials,
                        assumeRoleArn,
                        config.get("glue.iam-role-session-name"),
                        config.get("glue.region"));
        return new GlueCredentialProvider(assumed);
    }

    private static AwsCredentials assumeRole(
            AwsCredentials baseCredentials, String roleArn, String sessionName, String region) {
        StaticCredentialsProvider provider = StaticCredentialsProvider.create(baseCredentials);
        StsClientBuilder builder = StsClient.builder().credentialsProvider(provider);
        if (!StringUtils.isBlank(region)) {
            builder = builder.region(Region.of(region));
        }
        try (StsClient stsClient = builder.build()) {
            AssumeRoleRequest request =
                    AssumeRoleRequest.builder()
                            .roleArn(roleArn)
                            .roleSessionName(
                                    StringUtils.isBlank(sessionName)
                                            ? DEFAULT_SESSION_NAME
                                            : sessionName)
                            .build();
            Credentials credentials = stsClient.assumeRole(request).credentials();
            return AwsSessionCredentials.create(
                    credentials.accessKeyId(),
                    credentials.secretAccessKey(),
                    credentials.sessionToken());
        }
    }

    public AwsCredentials resolveCredentials() {
        return this.credentials;
    }
}
