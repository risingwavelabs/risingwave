// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opendal::services::Gcs;
use opendal::Operator;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use md5::Md5;
use md5::Digest;
use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;
impl OpendalObjectStore {
    /// create opendal hdfs engine.
    pub fn new_gcs_engine(bucket: String, root: String) -> ObjectResult<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&bucket);

        builder.root(&root);
        // builder.endpoint("https://storage.googleapis.com/upload/storage/v1/b/");
        // builder.service_account("wcy-test@rwcdev.iam.gserviceaccount.com");
        // let orig = "MIIDJzCCAg+gAwIBAgIJAP/rpZ9V6WubMA0GCSqGSIb3DQEBBQUAMDYxNDAyBgNV\nBAMMK2ZlZGVyYXRlZC1zaWdub24uc3lzdGVtLmdzZXJ2aWNlYWNjb3VudC5jb20w\nHhcNMjMwMjAzMTUyMjMyWhcNMjMwMjIwMDMzNzMyWjA2MTQwMgYDVQQDDCtmZWRl\ncmF0ZWQtc2lnbm9uLnN5c3RlbS5nc2VydmljZWFjY291bnQuY29tMIIBIjANBgkq\nhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzSkYGlwDMKd7TWEuog27TdT04nLqocBh\nSKc6XpEfojywqKTACMtwzA3jtSC0pCTtf2a6VVOPZdMEmWYA32aqymUWmxCwLK12\n/R/s4WE8aRjzPzm9dx1P+3JA2286EF39jSq1btIhZbx/Q791heUFbsCMf1B9l3GO\nDjMXFx4Hopuu7SnUffDGehdMQrphd2kNmzOfJ7DxTTwmtYwqnBjFwCI8vYRf72aN\nwAZ4xwwb7j4dUUCz19/EAa4TyqbGvSy4L1+kix6wTtXIwnUGH/dxFFCqa7WATsQ+\nKXBaFkXh7Px69M1KabItapQibNWQhMyeKxfRVNEih0C3NYLN6ZGkWQIDAQABozgw\nNjAMBgNVHRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIHgDAWBgNVHSUBAf8EDDAKBggr\nBgEFBQcDAjANBgkqhkiG9w0BAQUFAAOCAQEABfavGv3e8+KEy9LVCZbL0zrmGYbw\nr/jETgMTd5jOsv4mk3jaif0KTFXa6ygEHsEWf3b17mD/rSThmdO+PlnAaQPRKBet\nek9czrDMCFAOn4GHHzITxlaPBWySfRwsI5VlMUVYJAzKQCdczZ+WMavbn05gEyIy\nf4TqWuC9Fu3uOpDLJhagbP1urWtCNCWdvaAP7barjpMzM6b8FnuC2aFr2xnmf7iA\n5NMXNfIYFVVLNkytgOI+JOZ70rYyFM8Z56YOtmoeSb0znh+uzoy54yhAJr+N5z9Q\naKju+J9ZEVBGv7XDszAoAP12TyUXAm5UQXgPhJFLPVPG+qptamw14gIXRQ==";
        // let orig = "114236264557425085938";
        // let encoded: String = BASE64_STANDARD.encode(orig);
        // builder.credential(&encoded);
        let op: Operator = Operator::create(builder)?.finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
        })
    }
}
