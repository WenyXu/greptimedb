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

use std::io::BufReader;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::json::reader::{infer_json_schema_from_iterator, ValueIter};
use async_trait::async_trait;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::file_format::{self, FileFormat};
use crate::util::io::SyncIoBridge;

#[derive(Debug)]
pub struct JsonFormat {
    pub schema_infer_max_record: Option<usize>,
    // TODO(weny): add compression_type
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
        }
    }
}

#[async_trait]
impl FileFormat for JsonFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<SchemaRef> {
        let reader = store
            .reader(&path)
            .await
            .context(error::ReadObjectSnafu { path: &path })?;

        let mut reader = BufReader::new(SyncIoBridge::new(reader));

        let iter = ValueIter::new(&mut reader, self.schema_infer_max_record);

        let schema = infer_json_schema_from_iterator(iter)
            .context(error::InferSchemaSnafu { path: &path })?;

        Ok(Arc::new(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{self, format_schema, test_store};

    fn test_data_root() -> String {
        test_util::get_data_dir("tests/json").display().to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = JsonFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "simple.json".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec!["a: Int64", "b: Float64", "c: Boolean", "d: Utf8",],
            formatted
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = JsonFormat {
            schema_infer_max_record: Some(3),
        };
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.json".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean"], formatted);
    }
}
