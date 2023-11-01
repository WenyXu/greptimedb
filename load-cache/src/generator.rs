use std::collections::BTreeMap;
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManager;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Partition, Region, RegionRoute};
use common_telemetry::info;
use futures::future::try_join_all;
use meta_srv::service::store::etcd::EtcdStore;
use meta_srv::service::store::kv::KvBackendAdapter;
use store_api::storage::RegionId;
use tokio::time::Instant;

use crate::{GeneratorArgs, Runnable};

pub struct Generator {
    args: GeneratorArgs,
}

impl Generator {
    pub fn new(args: GeneratorArgs) -> Box<dyn Runnable> {
        Box::new(Self { args })
    }
}

pub fn generate_region_route(region_id: RegionId) -> RegionRoute {
    RegionRoute {
        region: Region {
            id: region_id,
            name: "r1".to_string(),
            partition: Some(Partition {
                column_list: vec![b"cpu_util".to_vec(), b"mem_util".to_vec(), b"host".to_vec()],
                value_list: vec![b"1024".to_vec(), b"2048".to_vec(), b"100.0.0".to_vec()],
            }),
            attrs: BTreeMap::new(),
        },
        leader_peer: Some(Peer::new(1, "192.0.0.1")),
        follower_peers: vec![
            Peer::new(2, "192.0.0.2"),
            Peer::new(2, "192.0.0.2"),
            Peer::new(3, "192.0.0.3"),
        ],
    }
}

#[async_trait::async_trait]
impl Runnable for Generator {
    async fn run(&self) {
        let endpoint = &self.args.etcd_addr;

        let kvstore = EtcdStore::with_endpoints(endpoint.split(',').collect::<Vec<_>>())
            .await
            .expect("failed to connect to etcd");

        let table_metadata = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(kvstore)));

        let amount = self.args.amount;

        let region_num = self.args.region_num;

        let timer = Instant::now();

        let region_routes = (0..region_num)
            .map(|region_num| generate_region_route(RegionId::new(0, region_num as u32)))
            .collect::<Vec<_>>();

        let value = TableRouteValue::new(region_routes.clone());
        info!("Generated TableRouteValue: {value:?}");
        let encoded_len = value.try_as_raw_value().unwrap().len();
        info!(
            "Encoded TableRouteValue size: {}",
            ReadableSize(encoded_len as u64)
        );

        let tables = (0..amount).collect::<Vec<_>>();

        let mut tasks = Vec::with_capacity(amount / 1000);

        for chunk in tables.array_chunks::<1000>() {
            let table_metadata_moved = table_metadata.clone();
            let chunk = *chunk;
            tasks.push(tokio::spawn(async move {
                for table_id in chunk {
                    let region_routes = (0..region_num)
                        .map(|region_num| {
                            generate_region_route(RegionId::new(table_id as u32, region_num as u32))
                        })
                        .collect::<Vec<_>>();

                    table_metadata_moved
                        .create_table_route(table_id as u32, region_routes)
                        .await
                        .unwrap();
                }
            }));
        }

        let _ = try_join_all(tasks).await.unwrap();

        let elapsed = timer.elapsed().as_secs();

        info!(
            "Loaded {amount} into Ectd, elapsed: {}s, total size: {}",
            elapsed,
            ReadableSize(encoded_len as u64 * amount as u64),
        );
    }
}
