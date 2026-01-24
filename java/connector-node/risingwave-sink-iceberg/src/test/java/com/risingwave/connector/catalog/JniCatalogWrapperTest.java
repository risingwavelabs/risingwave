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

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class JniCatalogWrapperTest {
    @Test
    public void testJdbc() throws Exception {
        System.setProperty("aws.region", "us-east-1");
        JniCatalogWrapper catalog =
                JniCatalogWrapper.create(
                        "demo",
                        "org.apache.iceberg.jdbc.JdbcCatalog",
                        new String[] {
                            "uri", "jdbc:postgresql://172.17.0.3:5432/iceberg",
                            "jdbc.user", "admin",
                            "jdbc.password", "123456",
                            "warehouse", "s3://icebergdata/demo",
                            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                            "s3.endpoint", "http://172.17.0.2:9301",
                            "s3.region", "us-east-1",
                            "s3.path-style-access", "true",
                            "s3.access-key-id", "hummockadmin",
                            "s3.secret-access-key", "hummockadmin",
                        });

        System.out.println(catalog.loadTable("s1.t1"));
    }
}
