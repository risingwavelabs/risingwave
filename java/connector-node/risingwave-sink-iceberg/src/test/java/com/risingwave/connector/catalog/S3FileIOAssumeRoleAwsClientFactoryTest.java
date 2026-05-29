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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

public class S3FileIOAssumeRoleAwsClientFactoryTest {
    @Test
    public void testS3AssumeRoleUsesRefreshableProvider() {
        Map<String, String> config = new HashMap<>();
        config.put("client.region", "ap-southeast-2");
        config.put("s3.access-key-id", "access-key");
        config.put("s3.secret-access-key", "secret-key");
        config.put("s3.iam-role-arn", "arn:aws:iam::123456789012:role/risingwave-s3");

        S3FileIOAssumeRoleAwsClientFactory factory = new S3FileIOAssumeRoleAwsClientFactory();
        factory.initialize(config);

        assertThat(factory.credentialsProviderForTest())
                .isInstanceOf(StsAssumeRoleCredentialsProvider.class);
    }

    @Test
    public void testDefaultCredentialChainDoesNotUseSharedSingleton() {
        Map<String, String> config = new HashMap<>();

        S3FileIOAssumeRoleAwsClientFactory first = new S3FileIOAssumeRoleAwsClientFactory();
        first.initialize(config);
        S3FileIOAssumeRoleAwsClientFactory second = new S3FileIOAssumeRoleAwsClientFactory();
        second.initialize(config);

        assertThat(first.credentialsProviderForTest())
                .isInstanceOf(DefaultCredentialsProvider.class)
                .isNotSameAs(second.credentialsProviderForTest());
    }
}
