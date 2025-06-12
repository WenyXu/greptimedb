use std::sync::Arc;
use std::u64;

use async_trait::async_trait;
use clap::Parser;
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::ddl::utils::region_storage_path;
use common_meta::kv_backend::KvBackendRef;
use common_meta::DatanodeId;
use common_telemetry::tracing::warn;
use futures::TryStreamExt;
use mito2::region::opener::RegionMetadataLoader;
use mito2::region::options::RegionOptions;
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use snafu::ResultExt;
use store_api::path_utils::region_dir;
use store_api::storage::RegionId;

use crate::common::object_store::ObjectStoreConfig;
use crate::error::{InvalidArgumentsSnafu, MitoSnafu, ObjectStoreNotSetSnafu, Result};
use crate::metadata::common::StoreConfig;
use crate::metadata::doctor::utils::{
    DatanodeTableIterator, FullTableMetadata, IteratorInput, RegionNumbers, TableMetadataIterator,
};
use crate::Tool;

#[derive(Debug, Default, Parser)]
pub struct DiagnoseCommand {
    /// The table id to diagnose.
    #[clap(long)]
    table_id: Option<u32>,

    /// The table name to diagnose.
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    table_names: Vec<String>,

    /// The schema name of the table.
    #[clap(long)]
    schema_name: Option<String>,

    /// The region id to diagnose.
    #[clap(long)]
    region_id: Option<u64>,

    /// If specified, diagnose all regions on the specified datanode.
    #[clap(long)]
    datanode_id: Option<u64>,

    /// If specified, diagnose will not fail fast.
    #[clap(long)]
    no_fail_fast: bool,

    /// The store config.
    #[clap(flatten)]
    store: StoreConfig,

    /// The object store config.
    #[clap(flatten)]
    object_store: ObjectStoreConfig,

    /// The manifest checkpoint distance.
    #[clap(long, default_value = "10")]
    manifest_checkpoint_distance: u64,

    /// The compress manifest.
    #[clap(long)]
    compress_manifest: bool,
}

impl DiagnoseCommand {
    fn validate(&self) -> std::result::Result<(), BoxedError> {
        if self.table_id.is_none()
            && self.table_names.is_empty()
            && self.region_id.is_none()
            && self.datanode_id.is_none()
        {
            return Err(BoxedError::new(
                InvalidArgumentsSnafu {
                    msg: "You must specify either --table-id or --table-name or --region-id or --datanode-id.",
                }
                .build(),
            ));
        }
        Ok(())
    }
}

impl DiagnoseCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        self.validate()?;
        let kvbackend = self.store.build().await?;
        let object_store = self
            .object_store
            .build()?
            .ok_or_else(|| BoxedError::new(ObjectStoreNotSetSnafu.build()))?;
        let object_store_manager = Arc::new(ObjectStoreManager::new("", object_store));
        Ok(Box::new(DiagnoseTool {
            table_id: self.table_id,
            table_names: self.table_names.clone(),
            schema_name: self.schema_name.clone(),
            region_id: self.region_id,
            datanode_id: self.datanode_id,
            no_fail_fast: self.no_fail_fast,
            kvbackend,
            object_store_manager,
            compress_manifest: self.compress_manifest,
            manifest_checkpoint_distance: self.manifest_checkpoint_distance,
        }))
    }
}

struct DiagnoseTool {
    table_id: Option<u32>,
    table_names: Vec<String>,
    schema_name: Option<String>,
    region_id: Option<u64>,
    datanode_id: Option<u64>,
    no_fail_fast: bool,
    kvbackend: KvBackendRef,
    object_store_manager: ObjectStoreManagerRef,
    compress_manifest: bool,
    manifest_checkpoint_distance: u64,
}

#[async_trait]
impl Tool for DiagnoseTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        let iterator_input = self
            .generate_iterator_input()
            .await
            .map_err(BoxedError::new)?;
        let mut table_metadata_iterator = Box::pin(
            TableMetadataIterator::new(self.kvbackend.clone(), iterator_input).into_stream(),
        );

        while let Some((full_table_metadata, region_numbers)) = table_metadata_iterator
            .try_next()
            .await
            .map_err(BoxedError::new)?
        {
            if !full_table_metadata.is_physical_table() {
                let catalog_name = &full_table_metadata.table_info.catalog_name;
                let schema_name = &full_table_metadata.table_info.schema_name;
                let table_name = &full_table_metadata.table_info.name;
                warn!(
                    "Table({}) is not a physical table, skipped",
                    format_full_table_name(catalog_name, schema_name, table_name)
                );
                continue;
            }

            let region_numbers = match region_numbers {
                RegionNumbers::All => full_table_metadata.table_route.region_numbers(),
                RegionNumbers::Partial(region_numbers) => region_numbers,
            };

            for region_number in region_numbers {
                let region_id = RegionId::new(full_table_metadata.table_id, region_number);
                let region_metadata_exists = self
                    .diagnose_region(region_id, &full_table_metadata)
                    .await
                    .map_err(BoxedError::new)?;
                if !region_metadata_exists {
                    warn!("Region metadata not found, region_id: {}", region_id);

                    if !self.no_fail_fast {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}

impl DiagnoseTool {
    /// Returns true if the region metadata exists.
    async fn diagnose_region(
        &self,
        region_id: RegionId,
        full_table_metadata: &FullTableMetadata,
    ) -> Result<bool> {
        let loader = RegionMetadataLoader::new(
            self.compress_manifest,
            self.manifest_checkpoint_distance,
            self.object_store_manager.clone(),
        );

        let catalog_name = &full_table_metadata.table_info.catalog_name;
        let schema_name = &full_table_metadata.table_info.schema_name;
        let region_dir = region_dir(&region_storage_path(&catalog_name, &schema_name), region_id);
        let opts = full_table_metadata.table_info.to_region_options();
        // TODO(weny): We ignore WAL options now. We should `prepare_wal_options()` in the future.
        let region_options = RegionOptions::try_from(&opts).context(MitoSnafu)?;
        // TODO(weny): handle the case that the metadata is not found.
        let region_metadata_exists = loader
            .load(&region_dir, &region_options)
            .await
            .context(MitoSnafu)?
            .is_some();

        Ok(region_metadata_exists)
    }

    /// Generates the iterator input based on the command line arguments.
    async fn generate_iterator_input(&self) -> Result<IteratorInput> {
        if let Some(datanode_id) = self.datanode_id {
            let datanode_id = DatanodeId::from(datanode_id);
            let table_ids = DatanodeTableIterator::new(self.kvbackend.clone(), vec![datanode_id])
                .into_stream()
                .try_collect::<Vec<_>>()
                .await?;

            return Ok(IteratorInput::new_table_ids(table_ids));
        } else if let Some(table_id) = self.table_id {
            return Ok(IteratorInput::new_table_ids(vec![(
                table_id,
                RegionNumbers::All,
            )]));
        } else if !self.table_names.is_empty() {
            let table_names = self.table_names.clone();
            let catalog = DEFAULT_CATALOG_NAME.to_string();
            let schema_name = self
                .schema_name
                .clone()
                .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

            let table_names = table_names
                .iter()
                .map(|table_name| (catalog.clone(), schema_name.clone(), table_name.clone()))
                .collect::<Vec<_>>();
            return Ok(IteratorInput::new_table_names(table_names));
        }

        InvalidArgumentsSnafu {
            msg: "You must specify either --table-id or --table-name or --region-id or --datanode-id.",
        }
        .fail()
    }
}
