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

use client::{OutputData, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use common_meta::rpc::store::{BatchPutRequest, DeleteRangeRequest, RangeRequest};
use common_meta::rpc::KeyValue;
use common_recordbatch::util::collect_batches;
use common_telemetry::debug;
use common_test_util::recordbatch::check_output_stream;
use frontend::instance::Instance;
use futures::TryStreamExt;
use itertools::Itertools;
use table::table_reference::TableReference;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::tests::test_util::{
    execute_sql, MockInstance, MockInstanceBuilder, RebuildableMockInstance, TestContext,
};

fn new_test_physical_table_sql() -> String {
    r#"
CREATE TABLE phy (
    ts timestamp time index,
    val double,
    host string primary key
)
PARTITION ON COLUMNS ("host") (
    host < '1024',
    host >= '1024'
)
ENGINE=metric
WITH ("physical_metric_table" = "");
"#
    .to_string()
}

fn create_logical_table_sql(tags: &[&str], table_name: &str) -> String {
    let tags_str = tags.iter().map(|t| format!("{} string", t)).join(",\n");
    let primary_key = tags.iter().join(", ");

    format!(
        r#"
        CREATE TABLE {} (
            ts timestamp time index,
            val double,
            {},
            PRIMARY KEY ({})
        )
        engine=metric
        with ("on_physical_table" = "phy");
    "#,
        table_name, tags_str, primary_key
    )
}

async fn setup_tests(frontend: &Arc<Instance>) {
    // Creates the physical table.
    let output = execute_sql(frontend, &new_test_physical_table_sql()).await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    // Creates the logical table1.
    let output = execute_sql(frontend, &create_logical_table_sql(&["host", "job"], "t1")).await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));
    let output = insert_data(frontend, "t1", &["host", "job", "val", "ts"]).await;
    assert!(matches!(output, OutputData::AffectedRows(3)));

    // Creates the logical table2.
    let output = execute_sql(frontend, &create_logical_table_sql(&["host", "env"], "t2")).await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));
    assert!(matches!(
        insert_data(frontend, "t2", &["host", "env", "val", "ts"]).await,
        OutputData::AffectedRows(3),
    ));
}

#[tokio::test]
async fn test_reconcile_logical_table_with_dropped_table() {
    common_telemetry::init_default_ut_logging();
    let builder =
        GreptimeDbClusterBuilder::new("test_reconcile_logical_table_with_dropped_table").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;
    setup_tests(&test_context.frontend()).await;

    // Mock metasrv backup.
    let dump_keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    debug!("dropping table t2");
    // Drop the logical table2.
    let output = execute_sql(&test_context.frontend(), "drop table t2").await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    // Restores the metadata to the kv backend.
    restore_kvbackend(test_context.metasrv().kv_backend(), dump_keyvalues).await;
    // Rebuilds all components.
    test_context.rebuild().await;
    let frontend = test_context.frontend();

    // Try query the logical table2.
    let output = execute_sql(&frontend, "select * from t2").await;
    assert_region_not_found(output.data).await;

    // Try query the logical table1.
    let output = execute_sql(&frontend, "select * from t1 order by host").await;
    let expected = r#"+-------+-----+-------------------------+------+
| host  | job | ts                      | val  |
+-------+-----+-------------------------+------+
| host1 | a   | 2022-11-03T03:39:57.450 | 10.3 |
| host2 | b   | 2022-11-03T03:39:57.455 | 10.5 |
| host3 | c   | 2022-11-03T03:39:57.465 | 20.5 |
+-------+-----+-------------------------+------+"#;
    check_output_stream(output.data, expected).await;

    let expected = r#"+-------------------------+------+-------+------------+----------------------+-----+-----+
| ts                      | val  | host  | __table_id | __tsid               | job | env |
+-------------------------+------+-------+------------+----------------------+-----+-----+
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1025       | 18158861556952186298 | a   |     |
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1026       | 14399158719604416468 |     | a   |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1025       | 5125207934447856269  | b   |     |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1026       | 1935387462790936013  |     | b   |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1025       | 12577166563753894792 | c   |     |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1026       | 16095285061536390213 |     | c   |
+-------------------------+------+-------+------------+----------------------+-----+-----+"#;
    // Try query the physical table.
    let output = execute_sql(&frontend, "select * from phy order by host, job").await;
    check_output_stream(output.data, expected).await;

    let metasrv = test_context.metasrv();
    let reconciliation_manager = metasrv.reconciliation_manager();

    // Try to reconcile the logical table2.
    let table_ref = TableReference {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        table: "t2",
    };
    reconciliation_manager
        .reconcile_table(table_ref)
        .await
        .unwrap();

    // Now, the t2 is available again.
    let sql = r#"
        INSERT INTO t2 (host, env, val, ts)
        VALUES 
            ("host4", "a", 10.3, 1667446797450),
            ("host5", "b", 10.5, 1667446797455);"#;
    let output = execute_sql(&frontend, sql).await;
    assert!(matches!(output.data, OutputData::AffectedRows(2)));
    let output = execute_sql(&frontend, "select * from t2 order by host").await;
    let expected = r#"+-----+-------+-------------------------+------+
| env | host  | ts                      | val  |
+-----+-------+-------------------------+------+
| a   | host1 | 2022-11-03T03:39:57.450 | 10.3 |
| b   | host2 | 2022-11-03T03:39:57.455 | 10.5 |
| c   | host3 | 2022-11-03T03:39:57.465 | 20.5 |
| a   | host4 | 2022-11-03T03:39:57.450 | 10.3 |
| b   | host5 | 2022-11-03T03:39:57.455 | 10.5 |
+-----+-------+-------------------------+------+"#;
    check_output_stream(output.data, expected).await;
}

#[tokio::test]
async fn test_reconcile_logical_table_with_columns() {
    common_telemetry::init_default_ut_logging();
    let builder = GreptimeDbClusterBuilder::new("test_reconcile_logical_table_with_columns").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;
    setup_tests(&test_context.frontend()).await;

    // Mock metasrv backup.
    let dump_keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    let output = execute_sql(
        &test_context.frontend(),
        "ALTER TABLE t2 ADD COLUMN foo STRING PRIMARY KEY;",
    )
    .await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    // Inserts data to the logical table2.
    let sql = r#"
        INSERT INTO t2 (host, env, foo, val, ts)
        VALUES 
            ("host4", "a", "g1", 10.3, 1667446797450),
            ("host5", "b", "g2", 10.5, 1667446797455);"#;
    let output = execute_sql(&test_context.frontend(), sql).await;
    assert!(matches!(output.data, OutputData::AffectedRows(2)));

    // Restores the metadata to the kv backend.
    restore_kvbackend(test_context.metasrv().kv_backend(), dump_keyvalues).await;
    // Rebuilds all components.
    test_context.rebuild().await;
    let frontend = test_context.frontend();

    // Try query the logical table2.
    // We can't see `foo` column before the reconciliation.
    let output = execute_sql(&frontend, "select * from t2 order by host").await;
    let expected = r#"+-----+-------+-------------------------+------+
| env | host  | ts                      | val  |
+-----+-------+-------------------------+------+
| a   | host1 | 2022-11-03T03:39:57.450 | 10.3 |
| b   | host2 | 2022-11-03T03:39:57.455 | 10.5 |
| c   | host3 | 2022-11-03T03:39:57.465 | 20.5 |
| a   | host4 | 2022-11-03T03:39:57.450 | 10.3 |
| b   | host5 | 2022-11-03T03:39:57.455 | 10.5 |
+-----+-------+-------------------------+------+"#;
    check_output_stream(output.data, expected).await;

    // Try query the physical table.
    // We can't see `foo` column before the reconciliation.
    let output = execute_sql(&frontend, "select * from phy order by host, job").await;
    let expected = r#"+-------------------------+------+-------+------------+----------------------+-----+-----+
| ts                      | val  | host  | __table_id | __tsid               | job | env |
+-------------------------+------+-------+------------+----------------------+-----+-----+
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1025       | 18158861556952186298 | a   |     |
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1026       | 14399158719604416468 |     | a   |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1025       | 5125207934447856269  | b   |     |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1026       | 1935387462790936013  |     | b   |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1025       | 12577166563753894792 | c   |     |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1026       | 16095285061536390213 |     | c   |
| 2022-11-03T03:39:57.450 | 10.3 | host4 | 1026       | 4642016305345462226  |     | a   |
| 2022-11-03T03:39:57.455 | 10.5 | host5 | 1026       | 5626344382382473396  |     | b   |
+-------------------------+------+-------+------------+----------------------+-----+-----+"#;
    check_output_stream(output.data, expected).await;

    // We need to reconcile the physical table first.
    let metasrv = test_context.metasrv();
    let reconciliation_manager = metasrv.reconciliation_manager();
    let table_ref = TableReference {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        table: "phy",
    };
    reconciliation_manager
        .reconcile_table(table_ref)
        .await
        .unwrap();

    // Try query the physical table.
    // Now we can see `foo` column.
    let output = execute_sql(&frontend, "select * from phy order by host, job").await;
    let expected = r#"+-------------------------+------+-------+------------+----------------------+-----+-----+-----+
| ts                      | val  | host  | __table_id | __tsid               | job | env | foo |
+-------------------------+------+-------+------------+----------------------+-----+-----+-----+
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1025       | 18158861556952186298 | a   |     |     |
| 2022-11-03T03:39:57.450 | 10.3 | host1 | 1026       | 14399158719604416468 |     | a   |     |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1025       | 5125207934447856269  | b   |     |     |
| 2022-11-03T03:39:57.455 | 10.5 | host2 | 1026       | 1935387462790936013  |     | b   |     |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1025       | 12577166563753894792 | c   |     |     |
| 2022-11-03T03:39:57.465 | 20.5 | host3 | 1026       | 16095285061536390213 |     | c   |     |
| 2022-11-03T03:39:57.450 | 10.3 | host4 | 1026       | 4642016305345462226  |     | a   | g1  |
| 2022-11-03T03:39:57.455 | 10.5 | host5 | 1026       | 5626344382382473396  |     | b   | g2  |
+-------------------------+------+-------+------------+----------------------+-----+-----+-----+"#;
    check_output_stream(output.data, expected).await;

    let table_ref = TableReference {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        table: "t2",
    };
    reconciliation_manager
        .reconcile_table(table_ref)
        .await
        .unwrap();
    // Try query the logical table2.
    // Now we can see `foo` column.
    let output = execute_sql(&frontend, "select * from t2 order by host").await;
    let expected = r#"+-----+-----+-------+-------------------------+------+
| env | foo | host  | ts                      | val  |
+-----+-----+-------+-------------------------+------+
| a   |     | host1 | 2022-11-03T03:39:57.450 | 10.3 |
| b   |     | host2 | 2022-11-03T03:39:57.455 | 10.5 |
| c   |     | host3 | 2022-11-03T03:39:57.465 | 20.5 |
| a   | g1  | host4 | 2022-11-03T03:39:57.450 | 10.3 |
| b   | g2  | host5 | 2022-11-03T03:39:57.455 | 10.5 |
+-----+-----+-------+-------------------------+------+"#;
    check_output_stream(output.data, expected).await;

    // Creates the logical table1.
    let output = execute_sql(&frontend, &create_logical_table_sql(&["host", "typ"], "t3")).await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));
    let output = insert_data(&frontend, "t3", &["host", "typ", "val", "ts"]).await;
    assert!(matches!(output, OutputData::AffectedRows(3)));

    let output = execute_sql(&frontend, "select * from t3 order by host").await;
    let expected = r#"+-------+-------------------------+-----+------+
| host  | ts                      | typ | val  |
+-------+-------------------------+-----+------+
| host1 | 2022-11-03T03:39:57.450 | a   | 10.3 |
| host2 | 2022-11-03T03:39:57.455 | b   | 10.5 |
| host3 | 2022-11-03T03:39:57.465 | c   | 20.5 |
+-------+-------------------------+-----+------+"#;
    check_output_stream(output.data, expected).await;
}

async fn assert_region_not_found(output: OutputData) {
    let error = match output {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap_err(),
        _ => unreachable!(),
    };
    assert!(format!("{error:?}").contains("not found"));
}

async fn insert_data(frontend: &Arc<Instance>, table_name: &str, columns: &[&str]) -> OutputData {
    let columns_str = columns.iter().join(", ");
    let sql = format!(
        r#"
        INSERT INTO {} ({})
        VALUES 
            ("host1", "a", 10.3, 1667446797450),
            ("host2", "b", 10.5, 1667446797455),
            ("host3", "c", 20.5, 1667446797465);
        "#,
        table_name, columns_str
    );
    let output = execute_sql(frontend, &sql).await;
    output.data
}

async fn dump_kvbackend(kv_backend: &KvBackendRef) -> Vec<(Vec<u8>, Vec<u8>)> {
    let req = RangeRequest::new().with_range(vec![0], vec![0]);
    let stream = PaginationStream::new(kv_backend.clone(), req, DEFAULT_PAGE_SIZE, |kv| {
        Ok((kv.key, kv.value))
    })
    .into_stream();
    stream.try_collect::<Vec<_>>().await.unwrap()
}

async fn restore_kvbackend(kv_backend: &KvBackendRef, keyvalues: Vec<(Vec<u8>, Vec<u8>)>) {
    // Clear the kv backend before restoring.
    let req = DeleteRangeRequest::new().with_range(vec![0], vec![0]);
    kv_backend.delete_range(req).await.unwrap();

    let mut req = BatchPutRequest::default();
    for (key, value) in keyvalues {
        debug!("restoring key: {}", String::from_utf8_lossy(&key));
        req.kvs.push(KeyValue { key, value });
    }
    kv_backend.batch_put(req).await.unwrap();
}
