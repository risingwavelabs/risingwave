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
import software.amazon.awssdk.utils.Validate;

/** This class is used to provide a credential to glue catalog */
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
