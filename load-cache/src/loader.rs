use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_meta::range_stream::PaginationStream;
use common_meta::rpc::store::RangeRequest;
use common_telemetry::info;
use futures::TryStreamExt;
use meta_srv::service::store::etcd::EtcdStore;
use meta_srv::service::store::kv::KvBackendAdapter;
use tokio::time::Instant;

use crate::{LoadArgs, Runnable};

pub struct Loader {
    args: LoadArgs,
}

impl Loader {
    pub fn new(args: LoadArgs) -> Box<dyn Runnable> {
        Box::new(Self { args })
    }
}

#[async_trait::async_trait]
impl Runnable for Loader {
    async fn run(&self) {
        let endpoint = &self.args.etcd_addr;

        let kvstore = EtcdStore::with_endpoints(endpoint.split(',').collect::<Vec<_>>())
            .await
            .expect("failed to connect to etcd");
        let kvstore = KvBackendAdapter::wrap(kvstore);

        let timer = Instant::now();

        let req = RangeRequest::new().with_prefix(self.args.prefix.to_string());

        let stream = PaginationStream::new(
            kvstore,
            req,
            self.args.page_size,
            Arc::new(|kv| Ok((kv.key, kv.value))),
        );
        let resp = stream.try_collect::<Vec<_>>().await.unwrap();

        let elapsed = timer.elapsed().as_millis();

        let total: usize = resp.iter().map(|(k, v)| k.len() + v.len()).sum();

        info!(
            "Loads data: {}, elapsed: {} ms",
            ReadableSize(total as u64),
            elapsed
        );
    }
}
