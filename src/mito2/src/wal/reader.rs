use std::fmt::Debug;
use std::sync::Arc;

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_wal::options::{KafkaWalOptions, WalOptions};
use store_api::logstore::reader::{EntryDecoder, EntryStream, Reader, ReaderCtx};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use super::EntryId;

/// The Reader of [LogStore].
pub struct LogStoreReader<S, DecodedEntry> {
    store: Arc<S>,
    decoder: EntryDecoder<DecodedEntry>,
}

impl<S, D> Debug for LogStoreReader<S, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStoreReader").finish()
    }
}

impl<S: LogStore, DecodedEntry> LogStoreReader<S, DecodedEntry>
where
    DecodedEntry: Send + Sync + 'static,
{
    pub fn new(store: Arc<S>, decoder: EntryDecoder<DecodedEntry>) -> Self {
        Self { store, decoder }
    }

    fn read_region(
        &self,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, DecodedEntry>, BoxedError> {
        let store = self.store.clone();
        let decoder = self.decoder.clone();
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = store.namespace(region_id.into(), &Default::default());
            let mut stream = store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)?;

            while let Some(entries) = stream.next().await {
                let entries = entries.map_err(BoxedError::new)?;

                for entry in entries {
                    yield (decoder)(&entry)?
                }
            }
        });

        Ok(Box::pin(stream))
    }

    fn read_topic(
        &self,
        topic: &str,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, DecodedEntry>, BoxedError> {
        let topic = topic.to_string();
        let store = self.store.clone();
        let decoder = self.decoder.clone();
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = store.namespace(
                RegionId::from_u64(0).into(),
                &WalOptions::Kafka(KafkaWalOptions { topic }),
            );

            let mut stream = store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)
                .unwrap();

            while let Some(entries) = stream.next().await {
                let entries = entries.map_err(BoxedError::new).unwrap();

                for entry in entries {
                    yield (decoder)(&entry)?
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

impl<S: LogStore, DecodedEntry> Reader for LogStoreReader<S, DecodedEntry>
where
    DecodedEntry: Send + Sync + 'static,
{
    type DecodedEntry = DecodedEntry;

    fn read(
        &self,
        ctx: ReaderCtx,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, Self::DecodedEntry>, BoxedError> {
        let stream = match ctx {
            ReaderCtx::RaftEngine(region_id) => self.read_region(region_id, start_id)?,
            ReaderCtx::Kafka(topic) => self.read_topic(&topic, start_id)?,
        };

        Ok(Box::pin(stream))
    }
}
