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

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_wal::options::{KafkaWalOptions, WalOptions};
use futures::stream::BoxStream;
use snafu::ResultExt;
use store_api::logstore::entry::{Entry, RawEntry};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use crate::error::{self, Result};
use crate::wal::EntryId;

/// The context of [WalReadCtx].
pub(crate) enum LogStoreReadCtx {
    RaftEngine(RegionId),
    Kafka(String),
}

impl LogStoreReadCtx {
    pub fn from_wal_options(region_id: RegionId, options: WalOptions) -> Self {
        match options {
            WalOptions::RaftEngine => LogStoreReadCtx::RaftEngine(region_id),
            WalOptions::Kafka(options) => LogStoreReadCtx::Kafka(options.topic),
        }
    }
}

/// A stream that yields [RawEntry].
pub type RawEntryStream<'a> = BoxStream<'a, Result<RawEntry>>;

/// [RawEntryReader] provides the ability to read [RawEntry] from the underlying [LogStore].
pub(crate) trait RawEntryReader: Send + Sync {
    fn read<'a>(
        &'a self,
        ctx: &'a LogStoreReadCtx,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>>;
}

pub struct LogStoreRawEntryReader<S> {
    store: Arc<S>,
}

impl<S: LogStore> LogStoreRawEntryReader<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    fn read_region<'a>(
        &'a self,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>> {
        let store = self.store.clone();
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = store.namespace(region_id.into(), &Default::default());
            let mut stream = store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadWalSnafu { region_id })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu { region_id })?;

                for entry in entries {
                    yield entry.into_raw_entry()
                }
            }
        });

        Ok(Box::pin(stream))
    }

    fn read_topic<'a>(
        &'a self,
        topic: &'a str,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>> {
        let store = self.store.clone();
        let topic = topic.to_string();
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = store.namespace(
                RegionId::from_u64(0).into(),
                &WalOptions::Kafka(KafkaWalOptions {
                    topic: topic.clone(),
                }),
            );

            let mut stream = store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadKafkaWalSnafu { topic: &topic })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadKafkaWalSnafu { topic: &topic })?;

                for entry in entries {
                    yield entry.into_raw_entry()
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

impl<S: LogStore> RawEntryReader for LogStoreRawEntryReader<S> {
    fn read<'a>(
        &'a self,
        ctx: &'a LogStoreReadCtx,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>> {
        let stream = match ctx {
            LogStoreReadCtx::RaftEngine(region_id) => self.read_region(*region_id, start_id)?,
            LogStoreReadCtx::Kafka(topic) => self.read_topic(topic, start_id)?,
        };

        Ok(Box::pin(stream))
    }
}
