// Copyright 2023 Greptime Team
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

use common_base::secrets::{ExposeSecret, SecretString};
use common_telemetry::info;
use opendal::services::{Azblob, Gcs, Oss, S3};
use serde::{Deserialize, Serialize};

use crate::util;

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct GcsConnection {
    pub root: String,
    pub bucket: String,
    pub scope: String,
    #[serde(skip_serializing)]
    pub credential_path: SecretString,
    #[serde(skip_serializing)]
    pub credential: SecretString,
    pub endpoint: String,
}

impl From<&GcsConnection> for Gcs {
    fn from(connection: &GcsConnection) -> Self {
        let root = util::normalize_dir(&connection.root);
        info!(
            "The gcs storage bucket is: {}, root is: {}",
            connection.bucket, &root
        );

        Gcs::default()
            .root(&root)
            .bucket(&connection.bucket)
            .scope(&connection.scope)
            .credential_path(connection.credential_path.expose_secret())
            .credential(connection.credential.expose_secret())
            .endpoint(&connection.endpoint)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct S3Connection {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub secret_access_key: SecretString,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Enable virtual host style so that opendal will send API requests in virtual host style instead of path style.
    /// By default, opendal will send API to https://s3.us-east-1.amazonaws.com/bucket_name
    /// Enabled, opendal will send API to https://bucket_name.s3.us-east-1.amazonaws.com
    pub enable_virtual_host_style: bool,
}

impl From<&S3Connection> for S3 {
    fn from(connection: &S3Connection) -> Self {
        let root = util::normalize_dir(&connection.root);

        info!(
            "The s3 storage bucket is: {}, root is: {}",
            connection.bucket, &root
        );

        let mut builder = S3::default()
            .root(&root)
            .bucket(&connection.bucket)
            .access_key_id(connection.access_key_id.expose_secret())
            .secret_access_key(connection.secret_access_key.expose_secret());

        if connection.endpoint.is_some() {
            builder = builder.endpoint(connection.endpoint.as_ref().unwrap());
        }
        if connection.region.is_some() {
            builder = builder.region(connection.region.as_ref().unwrap());
        }
        if connection.enable_virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }

        builder
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct OssConnection {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
}

impl From<&OssConnection> for Oss {
    fn from(connection: &OssConnection) -> Self {
        let root = util::normalize_dir(&connection.root);
        info!(
            "The oss storage bucket is: {}, root is: {}",
            connection.bucket, &root
        );

        Oss::default()
            .root(&root)
            .bucket(&connection.bucket)
            .endpoint(&connection.endpoint)
            .access_key_id(connection.access_key_id.expose_secret())
            .access_key_secret(connection.access_key_secret.expose_secret())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct AzblobConnection {
    pub container: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub account_name: SecretString,
    #[serde(skip_serializing)]
    pub account_key: SecretString,
    pub endpoint: String,
    pub sas_token: Option<String>,
}

impl From<&AzblobConnection> for Azblob {
    fn from(connection: &AzblobConnection) -> Self {
        let root = util::normalize_dir(&connection.root);

        info!(
            "The azure storage container is: {}, root is: {}",
            connection.container, &root
        );

        let mut builder = Azblob::default()
            .root(&root)
            .container(&connection.container)
            .endpoint(&connection.endpoint)
            .account_name(connection.account_name.expose_secret())
            .account_key(connection.account_key.expose_secret());

        if let Some(token) = &connection.sas_token {
            builder = builder.sas_token(token);
        };

        builder
    }
}
