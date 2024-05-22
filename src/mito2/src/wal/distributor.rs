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

use std::collections::HashMap;
use std::sync::Mutex;

use api::v1::WalEntry;
use async_stream::{stream, try_stream};
use common_error::ext::BoxedError;
use common_wal::options::WalOptions;
use store_api::logstore::entry_distributor::{EntryDistributor, EntryStream};
use store_api::logstore::reader::{ReaderCtx, ReaderRef};
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::StreamExt;

use crate::wal::EntryId;

/// Naive Entry distributor.
///
/// ** For Kafka LogStore **
/// Read entries from a Kafka topic and filter out the entries for the Region(`region_id`).
///
/// **For RaftEngine LogStore **
/// Read the entries for the Region(`region_id`) from RaftEngine.
#[derive(Debug, Clone)]
pub struct NaiveEntryDistributor;

impl EntryDistributor for NaiveEntryDistributor {
    type DecodedEntry = WalEntry;

    fn subscribe(
        &self,
        reader: ReaderRef,
        region_id: RegionId,
        options: &WalOptions,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, Self::DecodedEntry>, BoxedError> {
        let ctx = match options {
            WalOptions::RaftEngine => ReaderCtx::RaftEngine(region_id),
            WalOptions::Kafka(options) => ReaderCtx::Kafka(options.topic.clone()),
        };
        let reader = reader.clone();
        let mut stream = reader.read(ctx, start_id)?;
        let stream = try_stream!({
            while let Some(entry) = stream.next().await {
                let (entry_id, entry_region_id, entry) = entry?;
                if region_id == entry_region_id {
                    yield (entry_id, entry)
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
struct KafkaWalEntryDistributorSource {
    options: WalOptions,
    start_id: EntryId,
    reader: ReaderRef,
}

#[derive(Debug, Default)]
struct KafkaWalEntryDistributorInner {
    source: Option<KafkaWalEntryDistributorSource>,
    sinks: HashMap<RegionId, Sink>,
}

#[derive(Debug)]
struct Sink {
    sender: UnboundedSender<(EntryId, WalEntry)>,
    start_id: EntryId,
}

/// Distribute WAL entries from single Kafka topic.
#[derive(Default, Debug)]
pub struct KafkaWalEntryDistributor {
    inner: Mutex<KafkaWalEntryDistributorInner>,
}

impl EntryDistributor for KafkaWalEntryDistributor {
    type DecodedEntry = WalEntry;

    fn subscribe(
        &self,
        reader: ReaderRef,
        region_id: RegionId,
        options: &WalOptions,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, Self::DecodedEntry>, BoxedError> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut inner = self.inner.lock().unwrap();
        if inner.source.is_none() {
            inner.source = Some(KafkaWalEntryDistributorSource {
                options: options.clone(),
                start_id,
                reader,
            });
        }

        // TODO(weny): add validation.

        self.inner.lock().unwrap().sinks.insert(
            region_id,
            Sink {
                sender: tx,
                start_id,
            },
        );
        let stream = stream!({
            while let Some(entry) = rx.recv().await {
                yield Ok(entry)
            }
        });
        Ok(Box::pin(stream))
    }
}

impl KafkaWalEntryDistributor {
    pub async fn distribute(self) -> crate::error::Result<()> {
        let KafkaWalEntryDistributorInner { source, sinks } = self.inner.into_inner().unwrap();
        let source = source.unwrap();
        let ctx = match source.options {
            WalOptions::RaftEngine => unreachable!(),
            WalOptions::Kafka(options) => ReaderCtx::Kafka(options.topic),
        };

        let mut stream = source.reader.read(ctx, source.start_id).unwrap();

        while let Some(entry) = stream.next().await {
            let (id, region_id, entry) = entry.map_err(BoxedError::new).unwrap();
            if let Some(Sink { sender, start_id }) = sinks.get(&region_id) {
                if id > *start_id {
                    sender.send((id, entry)).unwrap();
                }
            } else {
                // TODO(weny): add logs
            }
        }

        Ok(())
    }
}
