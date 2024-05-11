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
use std::time::Duration;

use catalog::kvbackend::new_table_cache;
use common_meta::cache::{
    new_composite_table_route_cache, new_table_flownode_set_cache, new_table_info_cache,
    new_table_name_cache, new_table_route_cache, CacheRegistryBuilder,
};
use common_meta::kv_backend::KvBackendRef;
use moka::future::CacheBuilder;

const DEFAULT_CACHE_MAX_CAPACITY: u64 = 65536;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

pub const TABLE_INFO_CACHE_NAME: &str = "table_info_cache";
pub const TABLE_NAME_CACHE_NAME: &str = "table_name_cache";
pub const TABLE_CACHE_NAME: &str = "table_cache";
pub const TABLE_FLOWNODE_SET_CACHE_NAME: &str = "table_flownode_set_cache";
pub const TABLE_ROUTE_CACHE: &str = "table_route_cache";
pub const COMPOSITE_TABLE_ROUTE_CACHE: &str = "composite_table_route_cache";

// TODO(weny): Make the cache configurable.
pub fn default_cache_registry_builder(kv_backend: KvBackendRef) -> CacheRegistryBuilder {
    // Builds table info cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_info_cache = Arc::new(new_table_info_cache(
        TABLE_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table name cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_name_cache = Arc::new(new_table_name_cache(
        TABLE_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_cache = Arc::new(new_table_cache(
        TABLE_CACHE_NAME.to_string(),
        cache,
        table_info_cache.clone(),
        table_name_cache.clone(),
    ));

    // Builds table flownode set cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_flownode_set_cache = Arc::new(new_table_flownode_set_cache(
        TABLE_FLOWNODE_SET_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table route cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_route_cache = Arc::new(new_table_route_cache(
        TABLE_ROUTE_CACHE.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds composite table route cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let composite_table_route_cache = Arc::new(new_composite_table_route_cache(
        COMPOSITE_TABLE_ROUTE_CACHE.to_string(),
        cache,
        table_route_cache.clone(),
    ));

    CacheRegistryBuilder::default()
        .add_cache(table_info_cache)
        .add_cache(table_name_cache)
        .add_cache(table_cache)
        .add_cache(table_flownode_set_cache)
        .add_cache(table_route_cache)
        .add_cache(composite_table_route_cache)
}
