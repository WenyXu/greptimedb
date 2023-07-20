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
use std::time::Instant;

use common_meta::key::table_name::{TableNameKey, TableNameManager};
use common_telemetry::info;
use futures::future::try_join_all;
use futures::Future;

use super::bench;

pub struct TableNameBencher {
    table_name_manager: Arc<TableNameManager>,
    count: u32,
}

impl TableNameBencher {
    pub fn new(table_name_manager: TableNameManager, count: u32) -> Self {
        Self {
            table_name_manager: Arc::new(table_name_manager),
            count,
        }
    }

    pub async fn start(&self) {
        self.bench_create().await;
        self.bench_rename().await;
        self.bench_get().await;
        // self.bench_tables().await;
        self.bench_remove().await;
    }

    async fn bench_parallel<F, Fut>(&self, desc: &str, f: F, count: u32)
    where
        F: Fn(u32, Arc<TableNameManager>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut handlers = Vec::with_capacity(count as usize);

        for i in 1..=count {
            let fut = f(i, self.table_name_manager.clone());
            handlers.push(tokio::spawn(async move {
                let start = Instant::now();
                fut.await;
                start.elapsed().as_millis() as u64
            }));
        }
        let mut all = try_join_all(handlers).await.unwrap();
        let avg = all.iter().sum::<u64>() / count as u64;
        all.sort();
        let p50 = all[(count as f64 * 0.5) as usize];
        let p95 = all[(count as f64 * 0.95) as usize];
        let p99 = all[(count as f64 * 0.99) as usize];

        info!("{desc}, average operation cost: {avg:.2} ms, p50: {p50:.2} ms, p95: {p95:.2} ms, p99: {p99:.2} ms");
    }

    async fn bench_create(&self) {
        let desc = format!("TableNameBencher: create {} table names", self.count);
        let f = |i, mgr: Arc<TableNameManager>| async move {
            let table_name = format!("bench_table_name_{}", i);
            let table_name_key = create_table_name_key(&table_name);
            mgr.create(&table_name_key, i).await.unwrap();
        };
        self.bench_parallel(&desc, f, self.count).await;
    }

    async fn bench_rename(&self) {
        let desc = format!("TableNameBencher: rename {} table names", self.count);
        let f = |i, mgr: Arc<TableNameManager>| async move {
            let table_name = format!("bench_table_name_{}", i);
            let new_table_name = format!("bench_table_name_new_{}", i);
            let table_name_key = create_table_name_key(&table_name);
            mgr.rename(table_name_key, i, &new_table_name)
                .await
                .unwrap();
        };
        self.bench_parallel(&desc, f, self.count).await;
    }

    async fn bench_get(&self) {
        let desc = format!("TableNameBencher: get {} table names", self.count);
        let f = |i, mgr: Arc<TableNameManager>| async move {
            let table_name = format!("bench_table_name_new_{}", i);
            let table_name_key = create_table_name_key(&table_name);
            assert!(mgr.get(table_name_key).await.unwrap().is_some());
        };
        self.bench_parallel(&desc, f, self.count).await;
    }

    async fn bench_tables(&self) {
        let desc = format!("TableNameBencher: list all {} table names", self.count);
        let f = |_, mgr: Arc<TableNameManager>| async move {
            assert!(!mgr
                .tables("bench_catalog", "bench_schema")
                .await
                .unwrap()
                .is_empty());
        };
        self.bench_parallel(&desc, f, self.count).await;
    }

    async fn bench_remove(&self) {
        let desc = format!("TableNameBencher: remove {} table names", self.count);
        let f = |i, mgr: Arc<TableNameManager>| async move {
            let table_name = format!("bench_table_name_new_{}", i);
            let table_name_key = create_table_name_key(&table_name);
            mgr.remove(table_name_key).await.unwrap();
        };
        self.bench_parallel(&desc, f, self.count).await;
    }
}

pub fn create_table_name_key(table_name: &str) -> TableNameKey {
    TableNameKey::new("bench_catalog", "bench_schema", table_name)
}
