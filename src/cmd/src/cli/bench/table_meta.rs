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

use common_meta::key::datanode_table::{
    DatanodeTableKey, DatanodeTableManager, DatanodeTableValue,
};
use common_meta::key::table_info::{TableInfoKey, TableInfoManager, TableInfoValue};
use common_meta::key::table_name::{TableNameManager, TableNameValue};
use common_meta::key::table_region::{TableRegionKey, TableRegionManager, TableRegionValue};
use common_meta::key::TableMetaKey;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::table_name::TableName;
use common_telemetry::info;
use futures::future::try_join_all;
use futures::Future;

use super::table_name::create_table_name_key;
use super::{create_region_distribution, create_table_info};

#[derive(Clone)]
pub struct TableMetadataBencher {
    datanode_table_manager: Arc<DatanodeTableManager>,
    table_name_manager: Arc<TableNameManager>,
    table_region_manager: Arc<TableRegionManager>,
    table_info_manager: Arc<TableInfoManager>,
    parallel: u32,
}

impl TableMetadataBencher {
    pub fn new(
        datanode_table_manager: DatanodeTableManager,
        table_name_manager: TableNameManager,
        table_region_manager: TableRegionManager,
        table_info_manager: TableInfoManager,
        parallel: u32,
    ) -> Self {
        Self {
            datanode_table_manager: Arc::new(datanode_table_manager),
            table_name_manager: Arc::new(table_name_manager),
            table_region_manager: Arc::new(table_region_manager),
            table_info_manager: Arc::new(table_info_manager),
            parallel,
        }
    }

    async fn bench_parallel<F, Fut>(self: &Arc<Self>, desc: &str, f: F, count: u32)
    where
        F: Fn(u32, Arc<Self>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut handlers = Vec::with_capacity(count as usize);

        for i in 1..=count {
            let fut = f(i, self.clone());
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

    pub async fn bench_create(self: &Arc<Self>) {
        let desc = format!("TableMetadata: create {} table metadata", self.parallel);

        let f = |i, self_arc: Arc<TableMetadataBencher>| async move {
            let table_name_str = format!("p_bench_table_name_{}", i);
            let table_name_key = create_table_name_key(&table_name_str);

            // table name
            self_arc
                .table_name_manager
                .create(&table_name_key, i)
                .await
                .unwrap();

            // table info
            let table_name = TableName::new("bench_catalog", "bench_schema", table_name_str);
            let table_info = create_table_info(i, table_name);
            self_arc
                .table_info_manager
                .create(i, &table_info)
                .await
                .unwrap();

            // datanode table
            self_arc
                .datanode_table_manager
                .create(1, i, vec![1, 2, 3, 4])
                .await
                .unwrap();

            // table region
            let region_distribution = create_region_distribution();
            self_arc
                .table_region_manager
                .create(i, &region_distribution)
                .await
                .unwrap();
        };
        self.bench_parallel(&desc, f, self.parallel).await
    }

    pub async fn bench_create_large_txn(self: &Arc<Self>) {
        let desc = format!(
            "TableMetadata with large txn: create {} table metadata",
            self.parallel
        );

        let f = |i, self_arc: Arc<TableMetadataBencher>| async move {
            let backend = &self_arc.datanode_table_manager.kv_backend;
            let table_name_str = format!("s_bench_table_name_{}", i);
            let table_name_key = create_table_name_key(&table_name_str);

            let mut ops = vec![];
            ops.push(TxnOp::Put(
                table_name_key.as_raw_key(),
                TableNameValue::new(i).try_as_raw_value().unwrap(),
            ));
            // table name

            let table_name =
                TableName::new("bench_catalog", "bench_schema", table_name_str.clone());
            let table_info = create_table_info(i, table_name);

            // table info
            let key = TableInfoKey::new(i);
            let value = TableInfoValue::new(table_info);

            ops.push(TxnOp::Put(
                key.as_raw_key(),
                value.try_as_raw_value().unwrap(),
            ));

            // datanode
            let key = DatanodeTableKey::new(1, i);
            let value = DatanodeTableValue::new(i, vec![1, 2, 3, 4]);

            ops.push(TxnOp::Put(
                key.as_raw_key(),
                value.try_as_raw_value().unwrap(),
            ));
            // table region
            let region_distribution = create_region_distribution();
            let key = TableRegionKey::new(i);
            let value = TableRegionValue::new(region_distribution);

            ops.push(TxnOp::Put(
                key.as_raw_key(),
                value.try_as_raw_value().unwrap(),
            ));

            let txn = Txn::new()
                .when(vec![Compare::with_not_exist_value(
                    table_name_key.clone().as_raw_key(),
                    CompareOp::Equal,
                )])
                .and_then(ops);

            let resp = backend.txn(txn).await.unwrap();
            assert!(resp.succeeded);
        };
        self.bench_parallel(&desc, f, self.parallel).await
    }
}
