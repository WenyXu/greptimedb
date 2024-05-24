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

use api::v1::WalEntry;
use async_stream::try_stream;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::entry::RawEntry;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use super::raw_entry_reader::{LogStoreReadCtx, RawEntryReader};
use super::WalEntryStream;
use crate::error::{DecodeWalSnafu, Result};
use crate::wal::EntryId;

pub(crate) fn decode_raw_entry(raw_entry: RawEntry) -> Result<(EntryId, WalEntry)> {
    let entry_id = raw_entry.entry_id;
    let wal_entry = WalEntry::decode(raw_entry.data.as_slice()).context(DecodeWalSnafu {
        region_id: raw_entry.region_id,
    })?;

    Ok((entry_id, wal_entry))
}

/// [OneShotWalEntryReader] provides the ability to read and decode entries from the underlying store.
pub(crate) trait OneShotWalEntryReader: Send + Sync {
    fn read(
        self,
        ctx: &LogStoreReadCtx,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<WalEntryStream>;
}

/// A Reader reads the [RawEntry] from [RawEntryReader] and decodes [RawEntry] into [WalEntry].
pub(crate) struct LogStoreEntryReader<R> {
    wal_raw_reader: R,
}

impl<R> LogStoreEntryReader<R> {
    pub fn new(wal_raw_reader: R) -> Self {
        Self { wal_raw_reader }
    }
}

impl<R: RawEntryReader> OneShotWalEntryReader for LogStoreEntryReader<R> {
    fn read(
        self,
        ctx: &LogStoreReadCtx,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<WalEntryStream> {
        let mut stream = self.wal_raw_reader.read(ctx, start_id)?;
        match ctx {
            LogStoreReadCtx::RaftEngine(_) => {
                let stream = try_stream!({
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        yield decode_raw_entry(entry)?
                    }
                });

                Ok(Box::pin(stream))
            }
            LogStoreReadCtx::Kafka(_) => {
                let stream = try_stream!({
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        if region_id == entry.region_id {
                            yield decode_raw_entry(entry)?
                        }
                    }
                });

                Ok(Box::pin(stream))
            }
        }
    }
}
