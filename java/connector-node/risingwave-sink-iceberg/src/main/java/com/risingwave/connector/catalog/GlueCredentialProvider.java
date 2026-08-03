/*
 * Copyright 2024 RisingWave Labs
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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.Validate;

/** This class is used to provide a credential to glue catalog */
public class GlueCredentialProvider implements AwsCredentialsProvider, SdkAutoCloseable {
    private static final String DEFAULT_SESSION_NAME = "risingwave-glue";
    private final AwsCredentialsProvider credentialsProvider;
    private final SdkAutoCloseable closeable;

    private GlueCredentialProvider(AwsCredentialsProvider credentialsProvider) {
        this(credentialsProvider, () -> {});
    }

    private GlueCredentialProvider(
            AwsCredentialsProvider credentialsProvider, SdkAutoCloseable closeable) {
        this.credentialsProvider =
                (AwsCredentialsProvider)
                        Validate.notNull(
                                credentialsProvider,
                                "Credentials provider must not be null.",
                                new Object[0]);
        this.closeable =
                (SdkAutoCloseable)
                        Validate.notNull(closeable, "Closeable must not be null.", new Object[0]);
    }

    public static GlueCredentialProvider create(Map<String, String> config) {
        Validate.notNull(config, "Config must not be null");
        String accessKey = config.get("glue.access-key-id");
        String secretKey = config.get("glue.secret-access-key");
        boolean useDefaultChain =
                Boolean.parseBoolean(
                        config.getOrDefault(
                                "glue.use-default-credential-chain", Boolean.FALSE.toString()));

        AwsCredentialsProvider baseCredentialsProvider =
                resolveBaseCredentialsProvider(accessKey, secretKey, useDefaultChain);
        String assumeRoleArn = config.get("glue.iam-role-arn");
        if (StringUtils.isBlank(assumeRoleArn)) {
            return new GlueCredentialProvider(baseCredentialsProvider);
        }

        return assumeRole(
                baseCredentialsProvider,
                assumeRoleArn,
                config.get("glue.iam-role-session-name"),
                config.get("glue.region"));
    }

    private static AwsCredentialsProvider resolveBaseCredentialsProvider(
            String accessKey, String secretKey, boolean useDefaultChain) {
        if (!StringUtils.isBlank(accessKey) && !StringUtils.isBlank(secretKey)) {
            AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            return StaticCredentialsProvider.create(credentials);
        }

        if (useDefaultChain) {
            // `create()` returns a JVM-wide singleton. Glue catalogs are closed independently, so
            // use an owned provider to avoid one catalog closing credentials still used by another.
            return DefaultCredentialsProvider.builder().build();
        }

        Validate.notNull(accessKey, "Glue access key must not be null.", new Object[0]);
        Validate.notNull(secretKey, "Glue secret key must not be null.", new Object[0]);
        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        return StaticCredentialsProvider.create(credentials);
    }

    private static GlueCredentialProvider assumeRole(
            AwsCredentialsProvider baseCredentialsProvider,
            String roleArn,
            String sessionName,
            String region) {
        StsClientBuilder builder = StsClient.builder().credentialsProvider(baseCredentialsProvider);
        if (!StringUtils.isBlank(region)) {
            builder = builder.region(Region.of(region));
        }
        StsClient stsClient = builder.build();
        AssumeRoleRequest request =
                AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName(
                                StringUtils.isBlank(sessionName)
                                        ? DEFAULT_SESSION_NAME
                                        : sessionName)
                        .build();
        StsAssumeRoleCredentialsProvider assumedProvider =
                StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(request)
                        .build();
        return new GlueCredentialProvider(
                assumedProvider,
                () -> closeAssumeRoleResources(stsClient, baseCredentialsProvider));
    }

    public AwsCredentials resolveCredentials() {
        return this.credentialsProvider.resolveCredentials();
    }

    AwsCredentialsProvider credentialsProviderForTest() {
        return credentialsProvider;
    }

    private static void closeAssumeRoleResources(
            StsClient stsClient, AwsCredentialsProvider baseCredentialsProvider) {
        stsClient.close();
        if (baseCredentialsProvider instanceof SdkAutoCloseable) {
            ((SdkAutoCloseable) baseCredentialsProvider).close();
        }
    }

    @Override
    public void close() {
        if (credentialsProvider instanceof SdkAutoCloseable) {
            ((SdkAutoCloseable) credentialsProvider).close();
        }
        closeable.close();
    }
}
