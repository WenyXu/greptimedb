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

use std::assert_matches::assert_matches;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::Error;
use crate::request::WorkerRequest;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_set_readonly_gracefully() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let (set_readonly_request, recv) = WorkerRequest::new_set_readonly_gracefully(region_id);
    engine
        .inner
        .workers
        .submit_to_worker(region_id, set_readonly_request)
        .await
        .unwrap();
    let result = recv.await.unwrap();
    assert!(result.exist);
    assert_eq!(result.last_entry_id.unwrap(), 0);

    // For fast-path.
    let result = engine.set_readonly_gracefully(region_id).await.unwrap();
    assert!(result.exist);
    assert_eq!(result.last_entry_id.unwrap(), 0);

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };

    let error = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest { rows: rows.clone() }),
        )
        .await
        .unwrap_err();

    let error = error.as_any().downcast_ref::<Error>().unwrap();

    assert_matches!(error, Error::RegionReadonly { .. });

    engine.set_writable(region_id, true).unwrap();

    put_rows(&engine, region_id, rows).await;

    let result = engine.set_readonly_gracefully(region_id).await.unwrap();
    assert!(result.exist);
    assert_eq!(result.last_entry_id.unwrap(), 1);
}

#[tokio::test]
async fn test_set_readonly_gracefully_not_exist() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let non_exist_region_id = RegionId::new(1, 1);

    // For inner `handle_inter_command`.
    let (set_readonly_request, recv) =
        WorkerRequest::new_set_readonly_gracefully(non_exist_region_id);
    engine
        .inner
        .workers
        .submit_to_worker(non_exist_region_id, set_readonly_request)
        .await
        .unwrap();
    let result = recv.await.unwrap();
    assert!(!result.exist);
    assert!(result.last_entry_id.is_none());

    // For fast-path.
    let result = engine
        .set_readonly_gracefully(non_exist_region_id)
        .await
        .unwrap();
    assert!(!result.exist);
    assert!(result.last_entry_id.is_none());
}
