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

use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::WalEntry;
use async_stream::stream;
use common_telemetry::{debug, error};
use futures::future::join_all;
use snafu::ensure;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use super::entry_reader::{decode_raw_entry, OneShotWalEntryReader};
use super::raw_entry_reader::{LogStoreReadCtx, RawEntryReader};
use super::WalEntryStream;
use crate::error::{self, Result};
use crate::wal::EntryId;

/// [WalEntryDistributor] distributes Wal entries to specific [WalEntryReceiver]s based on [RegionId].
pub(crate) struct WalEntryDistributor {
    raw_wal_reader: Arc<dyn RawEntryReader>,
    ctx: LogStoreReadCtx,
    senders: HashMap<RegionId, UnboundedSender<Result<(EntryId, WalEntry)>>>,
    arg_receivers: Vec<(RegionId, oneshot::Receiver<EntryId>)>,
}

impl WalEntryDistributor {
    /// Distributes Wal entries to specific [WalEntryReceiver]s based on [RegionId].
    pub async fn distribute(mut self) -> Result<()> {
        let mut arg_receivers = std::mem::take(&mut self.arg_receivers);
        let args_futures = arg_receivers.iter_mut().map(|(region_id, receiver)| async {
            {
                if let Ok(start_id) = receiver.await {
                    (*region_id, Some(start_id))
                } else {
                    (*region_id, None)
                }
            }
        });
        let args = join_all(args_futures)
            .await
            .into_iter()
            .flat_map(|(region_id, start_id)| {
                if let Some(start_id) = start_id {
                    Some((region_id, start_id))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // No subscribers
        if args.len() == 0 {
            return Ok(());
        }
        // Safety: must exist
        let mut min_start_id = args.first().unwrap().1;

        let mut subscribers = HashMap::with_capacity(args.len());
        for (region_id, start_id) in args {
            subscribers.insert(
                region_id,
                Receiver {
                    start_id,
                    sender: self.senders[&region_id].clone(),
                },
            );
            min_start_id = min(min_start_id, start_id);
        }

        let mut stream = self.raw_wal_reader.read(&self.ctx, min_start_id)?;
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            let entry_id = entry.entry_id;
            let region_id = entry.region_id;

            if let Some(Receiver { sender, start_id }) = subscribers.get(&region_id) {
                if entry_id >= *start_id {
                    if let Err(err) = sender.send(decode_raw_entry(entry)) {
                        error!(err; "Failed to distribute raw entry, entry_id:{}, region_id: {}", entry_id, region_id);
                    }
                }
            } else {
                debug!("Subscriber not found, region_id: {}", region_id);
            }
        }

        Ok(())
    }
}

/// Receives the Wal entries from [WalEntryDistributor].
#[derive(Debug)]
pub(crate) struct WalEntryReceiver {
    region_id: RegionId,
    /// `None` means [WalEntryDistributor] was consumed.
    entry_receiver: UnboundedReceiver<Result<(EntryId, WalEntry)>>,
    /// `None` means [WalEntryDistributor] was consumed.
    arg_sender: oneshot::Sender<EntryId>,
}

impl WalEntryReceiver {
    pub fn new(
        region_id: RegionId,
        entry_receiver: UnboundedReceiver<Result<(EntryId, WalEntry)>>,
        arg_sender: oneshot::Sender<EntryId>,
    ) -> Self {
        Self {
            region_id,
            entry_receiver,
            arg_sender,
        }
    }
}

impl OneShotWalEntryReader for WalEntryReceiver {
    fn read<'a>(
        self,
        _ctx: &'a LogStoreReadCtx,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<WalEntryStream<'a>> {
        let WalEntryReceiver {
            region_id: expected_region_id,
            mut entry_receiver,
            arg_sender,
        } = self;

        ensure!(
            expected_region_id == region_id,
            error::InvalidWalReadRequestSnafu {
                reason: format!(
                    "unmatched region_id, expected: {expected_region_id}, actual: {region_id}"
                ),
            }
        );

        if let Err(_) = arg_sender.send(start_id) {
            return error::InvalidWalReadRequestSnafu {
                reason: format!("WalEntryDistributor is dropped, failed to send arg, region_id: {region_id}, start_id: {start_id}"),
            }.fail();
        }

        let stream = stream!({
            while let Some(entry) = entry_receiver.recv().await {
                yield entry
            }
        });
        Ok(Box::pin(stream))
    }
}

struct Receiver {
    start_id: EntryId,
    sender: UnboundedSender<Result<(EntryId, WalEntry)>>,
}

/// Returns [WalEntryDistributor] and batch [WalEntryReceiver]s.
pub fn build_wal_entry_distributor(
    ctx: LogStoreReadCtx,
    raw_wal_reader: Arc<dyn RawEntryReader>,
    region_ids: Vec<RegionId>,
) -> (WalEntryDistributor, Vec<WalEntryReceiver>) {
    let mut senders = HashMap::with_capacity(region_ids.len());
    let mut readers = Vec::with_capacity(region_ids.len());
    let mut arg_receivers = Vec::with_capacity(region_ids.len());

    for region_id in region_ids {
        let (entry_sender, entry_receiver) = mpsc::unbounded_channel();
        let (arg_sender, arg_receiver) = oneshot::channel();

        senders.insert(region_id, entry_sender);
        arg_receivers.push((region_id, arg_receiver));
        readers.push(WalEntryReceiver::new(region_id, entry_receiver, arg_sender));
    }

    (
        WalEntryDistributor {
            ctx,
            raw_wal_reader,
            senders,
            arg_receivers,
        },
        readers,
    )
}
