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

use std::collections::VecDeque;

use async_stream::try_stream;
use common_catalog::format_full_table_name;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::DatanodeId;
use futures::{Stream, TryStreamExt};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionNumber, TableId};
use table::metadata::RawTableInfo;

use crate::error::{Result, TableMetadataSnafu, UnexpectedSnafu};

/// The input for the iterator.
pub enum IteratorInput {
    TableIds(VecDeque<(TableId, RegionNumbers)>),
    TableNames(VecDeque<(String, String, String)>),
}

impl IteratorInput {
    pub fn new_table_ids(table_ids: Vec<(TableId, RegionNumbers)>) -> Self {
        Self::TableIds(table_ids.into())
    }

    pub fn new_table_names(table_names: Vec<(String, String, String)>) -> Self {
        Self::TableNames(table_names.into())
    }
}

/// An iterator for retrieving table metadata from the metadata store.
///
/// This struct provides functionality to iterate over table metadata based on
/// either [`TableId`] and their associated regions or fully qualified table names.
pub struct TableMetadataIterator {
    input: IteratorInput,
    table_metadata_manager: TableMetadataManager,
}

pub struct FullTableMetadata {
    pub table_info: RawTableInfo,
    pub table_route: TableRouteValue,
}

impl FullTableMetadata {
    /// Returns true if it's [TableRouteValue::Physical].
    pub fn is_physical_table(&self) -> bool {
        self.table_route.is_physical()
    }
}

pub enum RegionNumbers {
    All,
    Partial(Vec<RegionNumber>),
}

impl TableMetadataIterator {
    pub fn new(kvbackend: KvBackendRef, input: IteratorInput) -> Self {
        let table_metadata_manager = TableMetadataManager::new(kvbackend);
        Self {
            input,
            table_metadata_manager,
        }
    }

    /// Returns the next table metadata and its associated regions.
    ///
    /// This method handles two types of inputs:
    /// - TableIds: Returns metadata for a specific table ID along with its specified regions
    /// - TableNames: Returns metadata for a table identified by its full name (catalog.schema.table)
    ///
    /// Returns `None` when there are no more tables to process.
    pub async fn next(&mut self) -> Result<Option<(FullTableMetadata, RegionNumbers)>> {
        match &mut self.input {
            IteratorInput::TableIds(table_ids) => {
                while let Some((table_id, regions)) = table_ids.pop_front() {
                    let full_table_metadata = self.get_table_metadata(table_id).await?;
                    return Ok(Some((full_table_metadata, regions)));
                }
            }

            IteratorInput::TableNames(table_names) => {
                while let Some(full_table_name) = table_names.pop_front() {
                    let table_id = self.get_table_id_by_name(full_table_name).await?;
                    let full_table_metadata = self.get_table_metadata(table_id).await?;
                    return Ok(Some((full_table_metadata, RegionNumbers::All)));
                }
            }
        }

        Ok(None)
    }

    /// Converts the iterator into a stream of table metadata and region numbers.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<(FullTableMetadata, RegionNumbers)>> {
        try_stream!({
            while let Some((full_table_metadata, region_numbers)) = self.next().await? {
                yield (full_table_metadata, region_numbers);
            }
        })
    }

    async fn get_table_id_by_name(
        &mut self,
        (catalog_name, schema_name, table_name): (String, String, String),
    ) -> Result<TableId> {
        let key = TableNameKey::new(&catalog_name, &schema_name, &table_name);
        let table_id = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataSnafu)?
            .with_context(|| UnexpectedSnafu {
                msg: format!(
                    "Table not found: {}",
                    format_full_table_name(&catalog_name, &schema_name, &table_name)
                ),
            })?
            .table_id();
        Ok(table_id)
    }

    async fn get_table_metadata(&mut self, table_id: TableId) -> Result<FullTableMetadata> {
        let (table_info, table_route) = self
            .table_metadata_manager
            .get_full_table_info(table_id)
            .await
            .context(TableMetadataSnafu)?;

        let table_info = table_info
            .context(UnexpectedSnafu {
                msg: format!("Table info not found for table id: {table_id}"),
            })?
            .into_inner()
            .table_info;
        let table_route = table_route
            .context(UnexpectedSnafu {
                msg: format!("Table route not found for table id: {table_id}"),
            })?
            .into_inner();

        Ok(FullTableMetadata {
            table_info,
            table_route,
        })
    }
}

/// An iterator that retrieves tables and associated regions belong to datanodes.
pub struct DatanodeTableIterator {
    input: VecDeque<DatanodeId>,
    table_metadata_manager: TableMetadataManager,
}

impl DatanodeTableIterator {
    pub fn new(kvbackend: KvBackendRef, input: Vec<DatanodeId>) -> Self {
        let table_metadata_manager = TableMetadataManager::new(kvbackend);
        Self {
            input: input.into(),
            table_metadata_manager,
        }
    }

    /// Returns the next batch of table id and region numbers belong to a datanode.
    ///
    /// If there are no more datanodes to iterate over, it will return `None`.
    pub async fn next(&mut self) -> Result<Option<VecDeque<(TableId, RegionNumbers)>>> {
        while let Some(datanode_id) = self.input.pop_front() {
            let table_ids = self.get_datanode_table_ids(datanode_id).await?;
            return Ok(Some(table_ids));
        }

        Ok(None)
    }

    /// Converts the iterator into a stream of table ids and region numbers.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<(TableId, RegionNumbers)>> {
        try_stream!({
            while let Some(table_ids) = self.next().await? {
                let table_ids = table_ids.into_iter().collect::<Vec<_>>();
                for (table_id, region_numbers) in table_ids {
                    yield (table_id, region_numbers);
                }
            }
        })
    }

    async fn get_datanode_table_ids(
        &mut self,
        datanode_id: DatanodeId,
    ) -> Result<VecDeque<(TableId, RegionNumbers)>> {
        let table_ids = self
            .table_metadata_manager
            .datanode_table_manager()
            .tables(datanode_id);

        let table_ids = table_ids
            .map_ok(|val| (val.table_id, RegionNumbers::Partial(val.regions)))
            .try_collect::<VecDeque<_>>()
            .await
            .context(TableMetadataSnafu)?;

        Ok(table_ids)
    }
}
