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
use std::sync::Arc;

use api::v1::WalEntry;
use common_runtime::JoinHandle;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use store_api::logstore::provider::{KafkaProvider, Provider};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex};

use crate::error::Result;
use crate::request::WorkerRequest;
use crate::wal::entry_reader::decode_stream;
use crate::wal::raw_entry_reader::{stream_filter, stream_flatten};
use crate::wal::EntryId;

#[derive(Debug)]
pub(crate) struct ReplicatorGroup<S> {
    replicators: Arc<Mutex<HashMap<Arc<KafkaProvider>, Sender<ReplicatorEvent>>>>,
    log_store: Arc<S>,
}

impl<S> Clone for ReplicatorGroup<S> {
    fn clone(&self) -> Self {
        Self {
            replicators: self.replicators.clone(),
            log_store: self.log_store.clone(),
        }
    }
}

impl<S: LogStore> ReplicatorGroup<S> {
    pub fn new(log_store: Arc<S>) -> Self {
        Self {
            replicators: Default::default(),
            log_store,
        }
    }
}

struct SubscribeRegion {
    /// The [`RegionId`]
    region_id: RegionId,
    /// The last [`EntryId`] of the Region.
    last_entry_id: EntryId,
    /// Sends replication instructions to the Region.
    sender: Sender<WorkerRequest>,
    /// Sends the response of [`SubscribeRegion`].
    resp_sender: Sender<Option<EntryId>>,
}

enum ReplicatorEvent {
    ReceivedEntry(Result<(RegionId, EntryId, WalEntry)>),
    SubscribeRegion(SubscribeRegion),
}

struct ReplicatorLoop<S> {
    provider: Arc<KafkaProvider>,
    /// The sender notifies the upstream to exit.
    exit_sender: oneshot::Sender<()>,
    /// The high watermark.
    watermark: EntryId,
    /// All subscribers.
    subscribers: HashMap<RegionId, Subscriber>,
    /// Receives [`ReplicatorEvent`]s.
    receiver: Receiver<ReplicatorEvent>,
    log_store: Arc<S>,
}

const EVENT_BATCH_SIZE: usize = 128;

enum Subscriber {
    Catching {
        region_id: RegionId,
        task: JoinHandle<()>,
        sender: Option<Sender<WorkerRequest>>,
    },
    Replicating {
        region_id: RegionId,
        watermark: EntryId,
        sender: Sender<WorkerRequest>,
    },
}

impl Subscriber {
    async fn handle_response(region_id: RegionId, receiver: oneshot::Receiver<Result<usize>>) {
        match receiver.await {
            Ok(rows) => {
                let rows = rows.expect("Failed to replicate entry");
                info!("Replicated {} rows, region: {}", rows, region_id);
            }
            Err(err) => {
                error!(err;"Worker is stopped");
            }
        }
    }

    async fn push(&mut self, entry_id: EntryId, wal_entry: WalEntry) {
        match self {
            Subscriber::Catching {
                region_id,
                task,
                sender,
            } => {
                task.await.expect("Failed to catchup");
                let sender = sender.take().unwrap();

                let (request, receiver) =
                    WorkerRequest::new_region_replication_request(*region_id, entry_id, wal_entry);
                if sender.send(request).await.is_err() {
                    Self::handle_response(*region_id, receiver).await;
                    *self = Subscriber::Replicating {
                        region_id: *region_id,
                        watermark: entry_id,
                        sender,
                    };
                } else {
                    warn!("Worker is stopped, region: {}", region_id);
                }
            }
            Subscriber::Replicating {
                region_id,
                watermark,
                sender,
            } => {
                if entry_id <= *watermark {
                    return;
                }
                let (request, receiver) =
                    WorkerRequest::new_region_replication_request(*region_id, entry_id, wal_entry);
                if sender.send(request).await.is_err() {
                    *watermark = entry_id;
                    Self::handle_response(*region_id, receiver).await;
                } else {
                    warn!("Worker is stopped, region: {}", region_id);
                }
            }
        }
    }
}

impl<S: LogStore> ReplicatorLoop<S> {
    async fn handle_received_entries(
        &mut self,
        received_entries: Vec<Result<(RegionId, EntryId, WalEntry)>>,
    ) {
        match received_entries.into_iter().collect::<Result<Vec<_>>>() {
            Ok(entries) => {
                for (region_id, entry_id, wal_entry) in entries {
                    match self.subscribers.get_mut(&region_id) {
                        Some(subscriber) => {
                            subscriber.push(entry_id, wal_entry).await;
                        }
                        None => debug!("Ignoring region {} replication", region_id),
                    }
                }
            }
            Err(err) => {
                error!(err; "Received corrupted entries");
            }
        }
    }

    async fn handle_subscribe_regions(&mut self, subscribe_regions: Vec<SubscribeRegion>) {
        for SubscribeRegion {
            region_id,
            last_entry_id,
            sender,
            resp_sender,
        } in subscribe_regions
        {
            if self.subscribers.contains_key(&region_id) {
                if resp_sender.send(None).await.is_err() {
                    error!("SubscribeRegion receiver is dropped");
                }
                continue;
            }

            let watermark = self.watermark;
            let provider = Provider::Kafka(self.provider.clone());
            let stream = self
                .log_store
                .read_until(&provider, last_entry_id + 1, move |offset| {
                    offset >= watermark
                })
                .await
                .unwrap();
            let mut stream = decode_stream(stream_filter(
                stream_flatten::<S>(stream, provider),
                region_id,
            ));

            let moved_sender = sender.clone();
            let handle = common_runtime::spawn_global(async move {
                while let Some(entry) = stream.next().await {
                    let (entry_id, wal_entry) = entry.unwrap();
                    let (request, receiver) = WorkerRequest::new_region_replication_request(
                        region_id, entry_id, wal_entry,
                    );
                    if moved_sender.send(request).await.is_err() {
                        let r = receiver.await.unwrap();
                        debug!("Catching up region: {region_id}, result: {:?}", r);
                    } else {
                        warn!("Worker is stopped, region: {}", region_id);
                    }
                }
            });

            self.subscribers.insert(
                region_id,
                Subscriber::Catching {
                    region_id,
                    task: handle,
                    sender: Some(sender),
                },
            );
            if resp_sender.send(Some(watermark)).await.is_err() {
                error!("SubscribeRegion receiver is dropped");
            }
        }
    }

    async fn handel_events(&mut self, events: &mut Vec<ReplicatorEvent>) {
        let mut received_entries = Vec::with_capacity(events.len());
        let mut subscribe_regions = Vec::with_capacity(events.len());

        for event in events.drain(..) {
            match event {
                ReplicatorEvent::ReceivedEntry(receive_entry) => {
                    received_entries.push(receive_entry)
                }
                ReplicatorEvent::SubscribeRegion(subscribe_region) => {
                    subscribe_regions.push(subscribe_region)
                }
            }
        }

        // Always handles region subscription first.
        self.handle_subscribe_regions(subscribe_regions).await;

        self.handle_received_entries(received_entries).await;
    }

    async fn run(&mut self) {
        let mut buffer = Vec::with_capacity(EVENT_BATCH_SIZE);
        loop {
            buffer.clear();

            tokio::select! {
                event = self.receiver.recv() =>{
                    match event {
                        Some(event) => {
                            buffer.push(event);
                        },
                        None => {
                            info!("ReplicatorLoop is quit, topic: {}", self.provider.topic);
                            break
                        }
                    }

                    for _ in 1..buffer.capacity() {
                        match self.receiver.try_recv(){
                            Ok(event) => buffer.push(event),
                            Err(_) => break,
                        }
                    }
                }
            }

            self.handel_events(&mut buffer).await;
        }
    }
}
