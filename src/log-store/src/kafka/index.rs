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

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use common_runtime::JoinHandle;
use common_telemetry::error;
use rskafka::client::partition::OffsetAt;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;
use tokio::time::Interval;
use tokio::{select, time};

use super::client_manager::ClientManagerRef;
use crate::error::Result;

#[async_trait::async_trait]
pub trait IndexCreator: Send + Sync {
    async fn create(&self) -> Result<()>;
}

#[derive(Debug)]
enum Instruction {
    Create,
}

#[derive(Debug)]
pub(crate) struct IndexSupervisor {
    sender: Sender<Instruction>,
    handle: JoinHandle<()>,
}

impl IndexSupervisor {
    pub fn new(creator: Box<dyn IndexCreator>, interval: Duration) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let mut interval: Interval = time::interval(interval);
        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = interval.tick() => {
                        if let Err(err) = creator.create().await{
                            error!(err; "Failed to create WAL index");
                        }
                    }
                    instruction = rx.recv() => {
                        match instruction{
                            Some(Instruction::Create) => {
                                if let Err(err) = creator.create().await{
                                    error!(err; "Failed to create WAL index");
                                }
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self { sender: tx, handle }
    }
}

pub(crate) struct NaiveIndexCreator {
    pub(crate) client_manager: ClientManagerRef,
    pub(crate) operator: object_store::ObjectStore,
    pub(crate) node: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TopicIndex {
    index: HashMap<RegionId, BTreeSet<u64>>,
    last_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WalIndex {
    map: HashMap<String, TopicIndex>,
}

fn index_path(node: u64) -> String {
    format!("/datanode/{node}/wal.index")
}

#[async_trait::async_trait]
impl IndexCreator for NaiveIndexCreator {
    async fn create(&self) -> Result<()> {
        let instances = self.client_manager.instances.read().await;
        let mut wal_index = HashMap::with_capacity(instances.len());
        for (provider, client) in instances.iter() {
            let partition_client = client.client();
            let last_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
            let index = client.index.lock().await.clone();
            wal_index.insert(
                provider.topic.to_string(),
                TopicIndex {
                    index,
                    last_offset: last_offset as u64,
                },
            );
        }

        self.operator
            .write(
                &index_path(self.node),
                serde_json::to_string(&wal_index).unwrap(),
            )
            .await
            .unwrap();

        Ok(())
    }
}
