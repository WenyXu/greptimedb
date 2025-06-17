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
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_time::{Timestamp, FOREVER};
use datafusion_expr::{col, lit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use object_store::util::join_dir;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    METADATA_REGION_SUBDIR, METADATA_SCHEMA_KEY_COLUMN_INDEX, METADATA_SCHEMA_KEY_COLUMN_NAME,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX, METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
    METADATA_SCHEMA_VALUE_COLUMN_INDEX, METADATA_SCHEMA_VALUE_COLUMN_NAME,
    METRIC_METADATA_REGION_GROUP,
};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING, SKIP_WAL_KEY, TTL_KEY,
};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    RegionCloseRequest, RegionCompactRequest, RegionCreateRequest, RegionFlushRequest,
    RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::oneshot;

use crate::compaction::compactor::{open_compaction_region, OpenCompactionRegionRequest};
use crate::config::MitoConfig;
use crate::engine::MITO_ENGINE_NAME;
use crate::error;
use crate::region::options::RegionOptions;
use crate::test_util::{
    build_rows, flush_region, put_rows, reopen_region, rows_schema, CreateRequestBuilder, TestEnv,
};

#[tokio::test]
async fn test_engine_open_empty() {
    let mut env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "empty".to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let role = engine.role(region_id);
    assert_eq!(role, None);
}

#[tokio::test]
async fn test_engine_open_existing() {
    let mut env = TestEnv::with_prefix("open-exiting");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_reopen_region() {
    let mut env = TestEnv::with_prefix("reopen-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_engine_open_readonly() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;

    // Region is readonly.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: rows.clone(),
                hint: None,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, err.status_code());

    assert_eq!(Some(RegionRole::Follower), engine.role(region_id));
    // Converts region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(Some(RegionRole::Leader), engine.role(region_id));

    put_rows(&engine, region_id, rows).await;
}

#[tokio::test]
async fn test_engine_region_open_with_options() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("ttl".to_string(), "4d".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        region.version().options.ttl,
        Some(Duration::from_secs(3600 * 24 * 4).into())
    );
}

#[tokio::test]
async fn test_engine_region_open_with_custom_store() {
    let mut env = TestEnv::new();
    let engine = env
        .create_engine_with_multiple_object_stores(MitoConfig::default(), None, None, &["Gcs"])
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("storage", "Gcs")
        .build();
    let region_dir = request.region_dir.clone();

    // Create a custom region.
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Close the custom region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the custom region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("storage".to_string(), "Gcs".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    // The region should not be opened with the default object store.
    let region = engine.get_region(region_id).unwrap();
    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(!object_store_manager
        .default_object_store()
        .exists(region.access_layer.region_dir())
        .await
        .unwrap());
    assert!(object_store_manager
        .find("Gcs")
        .unwrap()
        .exists(region.access_layer.region_dir())
        .await
        .unwrap());
}

#[tokio::test]
async fn test_open_region_skip_wal_replay() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 5),
    };
    put_rows(&engine, region_id, rows).await;

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Skip the WAL replay .
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: region_dir.to_string(),
                options: Default::default(),
                skip_wal_replay: true,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replay the WAL.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: Default::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_ok() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-ok");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Ok(0));
    }

    assert_eq!(handle_open.await.unwrap().unwrap().affected_rows, 0);
    assert_eq!(rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_err() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-err");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
    }

    assert_eq!(
        handle_open.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
    assert_eq!(
        rx.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
}

#[tokio::test]
async fn test_open_compaction_region() {
    let mut env = TestEnv::new();
    let mut mito_config = MitoConfig::default();
    mito_config
        .sanitize(&env.data_home().display().to_string())
        .unwrap();

    let engine = env.create_engine(mito_config.clone()).await;

    let region_id = RegionId::new(1, 1);
    let schema_metadata_manager = env.get_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let object_store_manager = env.get_object_store_manager().unwrap();

    let req = OpenCompactionRegionRequest {
        region_id,
        region_dir: region_dir.clone(),
        region_options: RegionOptions::default(),
        max_parallelism: 1,
    };

    let compaction_region = open_compaction_region(
        &req,
        &mito_config,
        object_store_manager.clone(),
        schema_metadata_manager,
    )
    .await
    .unwrap();

    assert_eq!(region_id, compaction_region.region_id);
}

pub(crate) fn region_options_for_metadata_region(
    mut original: HashMap<String, String>,
) -> HashMap<String, String> {
    // TODO(ruihang, weny): add whitelist for metric engine options.
    original.remove(APPEND_MODE_KEY);
    // Don't allow to set primary key encoding for metadata region.
    original.remove(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING);
    original.insert(TTL_KEY.to_string(), FOREVER.to_string());
    original.remove(SKIP_WAL_KEY);
    original
}

pub fn concat_column_key_prefix(region_id: RegionId) -> String {
    format!("__column_{}_", region_id.as_u64())
}

fn build_prefix_read_request(prefix: &str, key_only: bool) -> ScanRequest {
    let filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).like(lit(prefix));

    let projection = if key_only {
        vec![METADATA_SCHEMA_KEY_COLUMN_INDEX]
    } else {
        vec![
            METADATA_SCHEMA_KEY_COLUMN_INDEX,
            METADATA_SCHEMA_VALUE_COLUMN_INDEX,
        ]
    };
    ScanRequest {
        projection: Some(projection),
        filters: vec![filter_expr],
        ..Default::default()
    }
}

pub fn create_request_for_metadata_region(
    region_dir: &str,
    options: HashMap<String, String>,
) -> RegionCreateRequest {
    // ts TIME INDEX DEFAULT 0
    let timestamp_column_metadata = ColumnMetadata {
        column_id: METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX as _,
        semantic_type: SemanticType::Timestamp,
        column_schema: ColumnSchema::new(
            METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_default_constraint(Some(datatypes::schema::ColumnDefaultConstraint::Value(
            Value::Timestamp(Timestamp::new_millisecond(0)),
        )))
        .unwrap(),
    };
    // key STRING PRIMARY KEY
    let key_column_metadata = ColumnMetadata {
        column_id: METADATA_SCHEMA_KEY_COLUMN_INDEX as _,
        semantic_type: SemanticType::Tag,
        column_schema: ColumnSchema::new(
            METADATA_SCHEMA_KEY_COLUMN_NAME,
            ConcreteDataType::string_datatype(),
            false,
        ),
    };
    // val STRING
    let value_column_metadata = ColumnMetadata {
        column_id: METADATA_SCHEMA_VALUE_COLUMN_INDEX as _,
        semantic_type: SemanticType::Field,
        column_schema: ColumnSchema::new(
            METADATA_SCHEMA_VALUE_COLUMN_NAME,
            ConcreteDataType::string_datatype(),
            true,
        ),
    };

    // concat region dir
    let metadata_region_dir = join_dir(&region_dir, METADATA_REGION_SUBDIR);

    let options = region_options_for_metadata_region(options.clone());
    RegionCreateRequest {
        engine: MITO_ENGINE_NAME.to_string(),
        column_metadatas: vec![
            timestamp_column_metadata,
            key_column_metadata,
            value_column_metadata,
        ],
        primary_key: vec![METADATA_SCHEMA_KEY_COLUMN_INDEX as _],
        options,
        region_dir: metadata_region_dir,
    }
}

#[tokio::test]
async fn test_open_metadata_region() {
    common_telemetry::init_default_ut_logging();
    let mut env =
        TestEnv::with_existing_data_home("/home/weny/Projects/greptimedb/src/mito2/src/engine");
    let mut mito_config = MitoConfig::default();
    mito_config
        .sanitize(&env.data_home().display().to_string())
        .unwrap();

    let options = HashMap::from([
        ("physical_metric_table".to_string(), "".to_string()),
        ("memtable.type".to_string(), "partition_tree".to_string()),
        ("index.type".to_string(), "inverted".to_string()),
    ]);
    let dir = "/metadata";
    let engine = env.create_engine(mito_config.clone()).await;

    let options = region_options_for_metadata_region(options.clone());
    engine
        .handle_request(
            RegionId::new(1024, 0),
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: dir.to_string(),
                options,
                skip_wal_replay: true,
            }),
        )
        .await
        .unwrap();

    let reqgion_column_prefix = concat_column_key_prefix(RegionId::new(1026, 0));
    // let request = build_prefix_read_request(&reqgion_column_prefix, false);
    let stream = engine
        .scan_to_stream(RegionId::new(1024, 0), ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    println!("{}", batches.pretty_print().unwrap());
}

pub(crate) fn build_put_request_from_iter(
    kv: impl Iterator<Item = (String, String)>,
) -> RegionPutRequest {
    let cols = vec![
        api::v1::ColumnSchema {
            column_name: METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME.to_string(),
            datatype: api::v1::ColumnDataType::TimestampMillisecond as _,
            semantic_type: SemanticType::Timestamp as _,
            ..Default::default()
        },
        api::v1::ColumnSchema {
            column_name: METADATA_SCHEMA_KEY_COLUMN_NAME.to_string(),
            datatype: api::v1::ColumnDataType::String as _,
            semantic_type: SemanticType::Tag as _,
            ..Default::default()
        },
        api::v1::ColumnSchema {
            column_name: METADATA_SCHEMA_VALUE_COLUMN_NAME.to_string(),
            datatype: api::v1::ColumnDataType::String as _,
            semantic_type: SemanticType::Field as _,
            ..Default::default()
        },
    ];
    let rows = Rows {
        schema: cols,
        rows: kv
            .into_iter()
            .map(|(key, value)| api::v1::Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(0)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(key)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(value)),
                    },
                ],
            })
            .collect(),
    };

    RegionPutRequest { rows, hint: None }
}

fn write_region_4406636445696() -> Vec<(String, String)> {
    vec![
        (
            "__column_4406636445696_aW5zdGFuY2U".to_string(),
            r#"{"column_schema":{"name":"instance","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":7}"#.to_string(),
        ),
        (
            "__column_4406636445696_aG9zdA".to_string(),
            r#"{"column_schema":{"name":"host","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":6}"#.to_string(),
        ),
        (
            "__column_4406636445696_bmFtZXNwYWNl".to_string(),
            r#"{"column_schema":{"name":"namespace","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":2}"#.to_string(),
        ),
        (
            "__column_4406636445696_am9i".to_string(),
            r#"{"column_schema":{"name":"job","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":8}"#.to_string(),
        ),
        (
            "__column_4406636445696_ZW52".to_string(),
            r#"{"column_schema":{"name":"env","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":5}"#.to_string(),
        ),
        (
            "__column_4406636445696_Z3JlcHRpbWVfdmFsdWU".to_string(),
            r#"{"column_schema":{"name":"greptime_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":1}"#.to_string(),
        ),
        (
            "__column_4406636445696_Z3JlcHRpbWVfdGltZXN0YW1w".to_string(),
            r#"{"column_schema":{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},"semantic_type":"Timestamp","column_id":0}"#.to_string(),
        ),
        (
            "__column_4406636445696_YXBw".to_string(),
            r#"{"column_schema":{"name":"app","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":3}"#.to_string(),
        ),
        (
            "__region_4406636445696".to_string(),
            "".to_string(),
        ),
    ]
}

fn write_region_4402341478400() -> Vec<(String, String)> {
    vec![
        (
            "__column_4402341478400_bmFtZXNwYWNl".to_string(),
            r#"{"column_schema":{"name":"namespace","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":2}"#.to_string(),
        ),
        (
            "__column_4402341478400_Z3JlcHRpbWVfdmFsdWU".to_string(),
            r#"{"column_schema":{"name":"greptime_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":1}"#.to_string(),
        ),
        (
            "__column_4402341478400_Y2xvdWRfcHJvdmlkZXI".to_string(),
            r#"{"column_schema":{"name":"cloud_provider","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":4}"#.to_string(),
        ),
        (
            "__column_4402341478400_YXBw".to_string(),
            r#"{"column_schema":{"name":"app","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":3}"#.to_string(),
        ),
        (
            "__column_4402341478400_aG9zdA".to_string(),
            r#"{"column_schema":{"name":"host","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":6}"#.to_string(),
        ),
        (
            "__column_4402341478400_ZW52".to_string(),
            r#"{"column_schema":{"name":"env","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{"greptime:inverted_index":"true"}},"semantic_type":"Tag","column_id":5}"#.to_string(),
        ),
        (
            "__column_4402341478400_Z3JlcHRpbWVfdGltZXN0YW1w".to_string(),
            r#"{"column_schema":{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},"semantic_type":"Timestamp","column_id":0}"#.to_string(),
        ),
        (
            "__region_4402341478400".to_string(),
            "".to_string(),
        )
    ]
}

pub fn to_metadata_region_id(region_id: RegionId) -> RegionId {
    let table_id = region_id.table_id();
    let region_sequence = region_id.region_sequence();
    RegionId::with_group_and_seq(table_id, METRIC_METADATA_REGION_GROUP, region_sequence)
}

#[tokio::test]
async fn test_write_metadata_region() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("write-metadata-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let options = HashMap::from([
        ("physical_metric_table".to_string(), "".to_string()),
        ("memtable.type".to_string(), "partition_tree".to_string()),
        ("index.type".to_string(), "inverted".to_string()),
    ]);

    let reigon_id = RegionId::new(1024, 0);
    engine
        .handle_request(
            to_metadata_region_id(reigon_id),
            RegionRequest::Create(create_request_for_metadata_region("/metadata", options)),
        )
        .await
        .unwrap();

    let kv = write_region_4406636445696();
    let req = build_put_request_from_iter(kv.into_iter());
    engine
        .handle_request(RegionId::new(1024, 0), RegionRequest::Put(req))
        .await
        .unwrap();

    engine
        .handle_request(
            RegionId::new(1024, 0),
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    let kv = write_region_4402341478400();
    let req = build_put_request_from_iter(kv.into_iter());
    engine
        .handle_request(RegionId::new(1024, 0), RegionRequest::Put(req))
        .await
        .unwrap();
    engine
        .handle_request(
            RegionId::new(1024, 0),
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    let kv = write_region_4406636445696();
    let req = build_put_request_from_iter(kv.into_iter());
    engine
        .handle_request(RegionId::new(1024, 0), RegionRequest::Put(req))
        .await
        .unwrap();
    engine
        .handle_request(
            RegionId::new(1024, 0),
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    engine
        .handle_request(
            RegionId::new(1024, 0),
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();

    let stream = engine
        .scan_to_stream(RegionId::new(1024, 0), ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    println!("{}", batches.pretty_print().unwrap());
}
