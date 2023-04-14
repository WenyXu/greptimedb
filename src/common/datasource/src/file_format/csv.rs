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

use std::sync::Arc;
use std::task::Poll;

use arrow::csv;
use arrow::csv::reader::infer_reader_schema as infer_csv_schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common_runtime;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::file_format::{FileMeta, FileOpenFuture, FileOpener};
use derive_builder::Builder;
use futures::{ready, StreamExt};
use object_store::ObjectStore;
use snafu::ResultExt;
use tokio_util::io::SyncIoBridge;

use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, FileFormat};

#[derive(Debug)]
pub struct CsvFormat {
    pub has_header: bool,
    pub delimiter: u8,
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::UNCOMPRESSED,
        }
    }
}

#[derive(Debug, Clone, Builder)]
pub struct CsvConfig {
    batch_size: usize,
    file_schema: SchemaRef,
    #[builder(default = "None")]
    file_projection: Option<Vec<usize>>,
    #[builder(default = "true")]
    has_header: bool,
    #[builder(default = "b','")]
    delimiter: u8,
}

impl CsvConfig {
    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new()
            .with_schema(self.file_schema.clone())
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_size)
            .has_header(self.has_header);

        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }

        builder
    }
}

#[derive(Debug, Clone)]
pub struct CsvOpener {
    config: Arc<CsvConfig>,
    object_store: Arc<ObjectStore>,
    compression_type: CompressionType,
}

impl CsvOpener {
    /// Return a new [`CsvOpener`]. The call must ensure [`CsvConfig`].file_schema must correspond to the opening file.
    pub fn new(
        config: CsvConfig,
        object_store: ObjectStore,
        compression_type: CompressionType,
    ) -> Self {
        CsvOpener {
            config: Arc::new(config),
            object_store: Arc::new(object_store),
            compression_type,
        }
    }
}

impl FileOpener for CsvOpener {
    fn open(&self, meta: FileMeta) -> DataFusionResult<FileOpenFuture> {
        let path = meta.location().to_string();
        let compression_type = self.compression_type;
        let object_store = self.object_store.clone();
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let reader = object_store
                .reader(&path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut upstream = compression_type.convert_stream(reader).fuse();

            let mut buffered = Bytes::new();

            let mut decoder = config.builder().build_decoder();

            let stream = futures::stream::poll_fn(move |cx| {
                loop {
                    if buffered.is_empty() {
                        if let Some(result) = ready!(upstream.poll_next_unpin(cx)) {
                            buffered = result?;
                        };
                    }

                    let decoded = decoder.decode(buffered.as_ref())?;

                    if decoded == 0 {
                        break;
                    } else {
                        buffered.advance(decoded);
                    }
                }

                Poll::Ready(decoder.flush().transpose())
            });

            Ok(stream.boxed())
        }))
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<SchemaRef> {
        let reader = store
            .reader(&path)
            .await
            .context(error::ReadObjectSnafu { path: &path })?;

        let decoded = self.compression_type.convert_async_read(reader);

        let delimiter = self.delimiter;
        let schema_infer_max_record = self.schema_infer_max_record;
        let has_header = self.has_header;

        common_runtime::spawn_blocking_read(move || {
            let reader = SyncIoBridge::new(decoded);

            let (schema, _records_read) =
                infer_csv_schema(reader, delimiter, schema_infer_max_record, has_header)
                    .context(error::InferSchemaSnafu { path: &path })?;

            Ok(Arc::new(schema))
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{self, format_schema, test_store};

    fn test_data_root() -> String {
        test_util::get_data_dir("tests/csv").display().to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let csv = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = csv
            .infer_schema(&store, "simple.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "c1: Utf8: NULL",
                "c2: Int64: NULL",
                "c3: Int64: NULL",
                "c4: Int64: NULL",
                "c5: Int64: NULL",
                "c6: Int64: NULL",
                "c7: Int64: NULL",
                "c8: Int64: NULL",
                "c9: Int64: NULL",
                "c10: Int64: NULL",
                "c11: Float64: NULL",
                "c12: Float64: NULL",
                "c13: Utf8: NULL"
            ],
            formatted,
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = CsvFormat {
            schema_infer_max_record: Some(3),
            ..CsvFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Int64: NULL",
                "d: Int64: NULL"
            ],
            formatted
        );

        let json = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Int64: NULL",
                "d: Utf8: NULL"
            ],
            formatted
        );
    }
}
