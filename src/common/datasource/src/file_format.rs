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

pub mod csv;
pub mod json;
pub mod parquet;
#[cfg(test)]
pub mod tests;

pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use object_store::ObjectStore;

use crate::error::Result;

#[async_trait]
pub trait FileFormat: Send + Sync + std::fmt::Debug {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<SchemaRef>;
}
