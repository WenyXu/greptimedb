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

use ::table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
use datatypes::type_id::LogicalTypeId;
use store_api::storage::SequenceNumber;

use crate::manifest::action::*;
use crate::metadata::RegionMetadata;
use crate::sst::{FileId, FileMeta};
use crate::test_util::descriptor_util::RegionDescBuilder;

pub const DEFAULT_TEST_FILE_SIZE: u64 = 1024;
pub const TABLE_NAME: &str = "demo";
pub const MITO_ENGINE: &str = "mito";

pub fn schema_for_test() -> Schema {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        // Nullable value column: cpu
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        // Non-null value column: memory
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_datatype(common_time::timestamp::TimeUnit::Millisecond),
            true,
        )
        .with_time_index(true),
    ];

    SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .build()
        .expect("ts must be timestamp column")
}

pub fn build_test_table_info() -> TableInfo {
    let table_meta = TableMetaBuilder::default()
        .schema(Arc::new(schema_for_test()))
        .engine(MITO_ENGINE)
        .next_column_id(1)
        // host is primary key column.
        .primary_key_indices(vec![0])
        .build()
        .unwrap();

    TableInfoBuilder::new(TABLE_NAME.to_string(), table_meta)
        .ident(0)
        .table_version(0u64)
        .table_type(TableType::Base)
        .catalog_name("greptime".to_string())
        .schema_name("public".to_string())
        .build()
        .unwrap()
}

pub fn build_region_meta() -> RegionMetadata {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .id(0)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    desc.try_into().unwrap()
}

pub fn build_altered_region_meta() -> RegionMetadata {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .id(0)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .push_value_column(("v2", LogicalTypeId::Float32, true))
        .build();
    desc.try_into().unwrap()
}

pub fn build_region_edit(
    sequence: SequenceNumber,
    files_to_add: &[FileId],
    files_to_remove: &[FileId],
) -> RegionEdit {
    RegionEdit {
        region_version: 0,
        flushed_sequence: Some(sequence),
        files_to_add: files_to_add
            .iter()
            .map(|f| FileMeta {
                region_id: 0,
                file_id: *f,
                time_range: None,
                level: 0,
                file_size: DEFAULT_TEST_FILE_SIZE,
            })
            .collect(),
        files_to_remove: files_to_remove
            .iter()
            .map(|f| FileMeta {
                region_id: 0,
                file_id: *f,
                time_range: None,
                level: 0,
                file_size: DEFAULT_TEST_FILE_SIZE,
            })
            .collect(),
    }
}
