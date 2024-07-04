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

use futures::future::BoxFuture;
use futures::TryStreamExt;
use moka::future::Cache;

use crate::cache::{CacheContainer, Initializer};
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::flow::flow_route::{
    FlowRouteKey, FlowRouteManager, FlowRouteManagerRef, FlowRouteValue,
};
use crate::key::FlowId;
use crate::kv_backend::KvBackendRef;

type FlowRoutes = Arc<Vec<(FlowRouteKey, FlowRouteValue)>>;

pub type FlowRoutesCacheRef = Arc<FlowRoutesCache>;

/// [`FlowRoutesCache`] caches the [`FlowId`] to [`FlowRoutes`] mapping.
pub type FlowRoutesCache = CacheContainer<FlowId, FlowRoutes, CacheIdent>;

/// Constructs a [FlowRoutesCache].
pub fn new_flow_routes_cache(
    name: String,
    cache: Cache<FlowId, FlowRoutes>,
    kv_backend: KvBackendRef,
) -> FlowRoutesCache {
    let table_flow_manager = Arc::new(FlowRouteManager::new(kv_backend));
    let init = init_factory(table_flow_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(table_flow_manager: FlowRouteManagerRef) -> Initializer<FlowId, FlowRoutes> {
    Arc::new(move |&flow_id| {
        let table_flow_manager = table_flow_manager.clone();
        Box::pin(async move {
            let routes = table_flow_manager
                .routes(flow_id)
                .try_collect::<Vec<_>>()
                .await?;
            if routes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Arc::new(routes)))
            }
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<FlowId, FlowRoutes>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let CacheIdent::FlowId(flow_id) = ident {
            cache.invalidate(flow_id).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::FlowId(_))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use moka::future::CacheBuilder;
    use table::table_name::TableName;

    use crate::cache::new_flow_routes_cache;
    use crate::instruction::CacheIdent;
    use crate::key::flow::flow_info::FlowInfoValue;
    use crate::key::flow::flow_route::FlowRouteValue;
    use crate::key::flow::FlowMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;

    #[tokio::test]
    async fn test_cache_get_non_exist_routes() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_flow_routes_cache("test".to_string(), cache, mem_kv);
        assert!(cache.get(1024).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cache_get() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_flow_routes_cache("test".to_string(), cache, mem_kv.clone());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        flow_metadata_manager
            .create_flow_metadata(
                1024,
                FlowInfoValue {
                    source_table_ids: vec![1024, 1025],
                    sink_table_name: TableName {
                        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                        table_name: "sink_table".to_string(),
                    },
                    flownode_ids: BTreeMap::from([(0, 1), (1, 2), (2, 3)]),
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    flow_name: "my_flow".to_string(),
                    raw_sql: "sql".to_string(),
                    expire_after: Some(300),
                    comment: "comment".to_string(),
                    options: Default::default(),
                },
                vec![
                    (
                        0,
                        FlowRouteValue {
                            peer: Peer::empty(0),
                        },
                    ),
                    (
                        1,
                        FlowRouteValue {
                            peer: Peer::empty(1),
                        },
                    ),
                    (
                        2,
                        FlowRouteValue {
                            peer: Peer::empty(2),
                        },
                    ),
                ],
            )
            .await
            .unwrap();
        let routes = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(routes.len(), 3);
        assert!(cache.contains_key(&1024));
        cache.invalidate(&[CacheIdent::FlowId(1024)]).await.unwrap();
        assert!(!cache.contains_key(&1024));
    }
}
