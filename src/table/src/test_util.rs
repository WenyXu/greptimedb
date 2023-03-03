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

mod empty_table;
mod memtable;
mod mock_engine;

pub use empty_table::EmptyTable;
pub use memtable::MemTable;
pub use mock_engine::MockTableEngine;

#[inline]
fn region_name(table_id: u32, n: u32) -> String {
    format!("{table_id}_{n:010}")
}

#[inline]
fn table_dir(catalog_name: &str, schema_name: &str, table_id: u32) -> String {
    format!("{catalog_name}/{schema_name}/{table_id}")
}

pub fn test_region_dir(
    dir: &str,
    catalog_name: &str,
    schema_name: &str,
    table_id: u32,
    region_id: u32,
) -> String {
    let table_dir = table_dir(catalog_name, schema_name, table_id);
    let region_name = region_name(table_id, region_id);

    format!("{}/{}/{}", dir, table_dir, region_name)
}

pub fn has_parquet_file(sst_dir: &str) -> bool {
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            return true;
        }
    }

    false
}
