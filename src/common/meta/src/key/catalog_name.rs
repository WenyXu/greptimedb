// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Error, InvalidTableMetadataSnafu, Result};
use crate::key::{TableMetaKey, CATALOG_NAME_KEY_PATTERN, CATALOG_NAME_KEY_PREFIX};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::{CompareAndPutRequest, RangeRequest};
use crate::rpc::KeyValue;
use crate::table_name::TableName;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CatalogNameKey<'a> {
    pub catalog: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogNameValue;

impl<'a> CatalogNameKey<'a> {
    pub fn new(catalog: &'a str) -> Self {
        Self { catalog }
    }

    pub fn range_start_key() -> String {
        format!("{}/", CATALOG_NAME_KEY_PREFIX)
    }
}

impl<'a> From<&'a TableName> for CatalogNameKey<'a> {
    fn from(value: &'a TableName) -> Self {
        Self {
            catalog: &value.catalog_name,
        }
    }
}

impl Display for CatalogNameKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", CATALOG_NAME_KEY_PREFIX, self.catalog)
    }
}

impl TableMetaKey for CatalogNameKey<'_> {
    fn as_raw_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl<'a> TryFrom<&'a str> for CatalogNameKey<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let captures = CATALOG_NAME_KEY_PATTERN
            .captures(s)
            .context(InvalidTableMetadataSnafu {
                err_msg: format!("Illegal CatalogNameKey format: '{s}'"),
            })?;

        // Safety: pass the regex check above
        Ok(Self {
            catalog: captures.get(1).unwrap().as_str(),
        })
    }
}

/// Decoder `KeyValue` to ({catalog},())
pub fn catalog_decoder(kv: KeyValue) -> Result<(String, ())> {
    let str = std::str::from_utf8(&kv.key).context(error::ConvertRawKeySnafu)?;
    let catalog_name = CatalogNameKey::try_from(str)?;

    Ok((catalog_name.catalog.to_string(), ()))
}

pub struct CatalogManager {
    kv_backend: KvBackendRef,
}

impl CatalogManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Creates `CatalogNameKey`, throws an error if key exists.
    pub async fn create(&self, catalog: CatalogNameKey<'_>) -> Result<()> {
        let resp = self
            .kv_backend
            .compare_and_put(Self::build_create_req(catalog)?)
            .await?;

        resp.handle(|resp| {
            if resp.is_success() {
                Ok(())
            } else {
                error::CatalogAlreadyExistsSnafu {
                    catalog: catalog.to_string(),
                }
                .fail()
            }
        })
    }

    /// Creates `CatalogNameKey`, if key not exists.
    pub async fn create_if_not_exists(&self, catalog: CatalogNameKey<'_>) -> Result<()> {
        let resp = self
            .kv_backend
            .compare_and_put(Self::build_create_req(catalog)?)
            .await?;
        resp.handle(|_| Ok(()))
    }

    fn build_create_req(schema: CatalogNameKey<'_>) -> Result<CompareAndPutRequest> {
        let raw_key = schema.as_raw_key();

        Ok(CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_expect(CatalogNameValue.try_as_raw_value()?))
    }

    pub async fn catalog_names(&self) -> BoxStream<'static, Result<String>> {
        let start_key = CatalogNameKey::range_start_key();
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(catalog_decoder),
        );

        Box::pin(stream.map(|kv| kv.map(|kv| kv.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let key = CatalogNameKey::new("my-catalog");

        assert_eq!(key.to_string(), "__catalog_name/my-catalog");

        let parsed: CatalogNameKey = "__catalog_name/my-catalog".try_into().unwrap();

        assert_eq!(key, parsed);
    }
}
