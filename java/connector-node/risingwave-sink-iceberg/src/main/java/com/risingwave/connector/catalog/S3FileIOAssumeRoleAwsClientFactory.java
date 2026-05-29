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

package com.risingwave.connector.catalog;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.StringUtils;

public class S3FileIOAssumeRoleAwsClientFactory implements S3FileIOAwsClientFactory {
    private static final String S3_IAM_ROLE_ARN = "s3.iam-role-arn";
    private static final String S3_IAM_ROLE_SESSION_NAME = "s3.iam-role-session-name";
    private static final String DEFAULT_SESSION_NAME = "risingwave-s3";

    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;
    private AwsClientProperties awsClientProperties;
    private AwsCredentialsProvider credentialsProvider;

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
        this.credentialsProvider = createCredentialsProvider(properties);
    }

    @Override
    public S3Client s3() {
        return S3Client.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(awsClientProperties::applyLegacyMd5Plugin)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .applyMutation(s3FileIOProperties::applyServiceConfigurations)
                .credentialsProvider(credentialsProvider)
                .applyMutation(s3FileIOProperties::applySignerConfiguration)
                .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
                .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
                .applyMutation(s3FileIOProperties::applyRetryConfigurations)
                .build();
    }

    @Override
    public S3AsyncClient s3Async() {
        if (s3FileIOProperties.isS3CRTEnabled()) {
            return S3AsyncClient.crtBuilder()
                    .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                    .credentialsProvider(credentialsProvider)
                    .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                    .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
                    .build();
        }
        return S3AsyncClient.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .credentialsProvider(credentialsProvider)
                .applyMutation(awsClientProperties::applyLegacyMd5Plugin)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .build();
    }

    AwsCredentialsProvider credentialsProviderForTest() {
        return credentialsProvider;
    }

    private AwsCredentialsProvider createCredentialsProvider(Map<String, String> properties) {
        AwsCredentialsProvider baseCredentialsProvider =
                createBaseCredentialsProvider(
                        s3FileIOProperties.accessKeyId(),
                        s3FileIOProperties.secretAccessKey(),
                        s3FileIOProperties.sessionToken());

        String roleArn = properties.get(S3_IAM_ROLE_ARN);
        if (StringUtils.isBlank(roleArn)) {
            return baseCredentialsProvider;
        }

        StsClientBuilder stsClientBuilder =
                StsClient.builder().credentialsProvider(baseCredentialsProvider);
        String region = awsClientProperties.clientRegion();
        if (!StringUtils.isBlank(region)) {
            stsClientBuilder = stsClientBuilder.region(Region.of(region));
        }
        StsClient stsClient = stsClientBuilder.build();
        String sessionName = properties.get(S3_IAM_ROLE_SESSION_NAME);
        AssumeRoleRequest request =
                AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName(
                                StringUtils.isBlank(sessionName)
                                        ? DEFAULT_SESSION_NAME
                                        : sessionName)
                        .build();
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(request)
                .build();
    }

    private static AwsCredentialsProvider createBaseCredentialsProvider(
            String accessKeyId, String secretAccessKey, String sessionToken) {
        if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(secretAccessKey)) {
            // `create()` returns a JVM-wide singleton. FileIO factories are scoped to catalogs, so
            // use an owned provider that can follow the factory/client lifecycle independently.
            return DefaultCredentialsProvider.builder().build();
        }

        AwsCredentials credentials =
                StringUtils.isBlank(sessionToken)
                        ? AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                        : AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        return StaticCredentialsProvider.create(credentials);
    }
}
