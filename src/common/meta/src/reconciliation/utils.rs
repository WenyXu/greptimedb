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

use snafu::{ensure, OptionExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::{self, Result};
use crate::key::table_name::{TableNameKey, TableNameManager};
use crate::key::TableMetadataManagerRef;
use crate::node_manager::NodeManagerRef;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartialRegionMetadata<'a> {
    pub(crate) column_metadatas: &'a [ColumnMetadata],
    pub(crate) primary_key: &'a [u32],
    pub(crate) table_id: TableId,
}

impl<'a> From<&'a RegionMetadata> for PartialRegionMetadata<'a> {
    fn from(region_metadata: &'a RegionMetadata) -> Self {
        Self {
            column_metadatas: &region_metadata.column_metadatas,
            primary_key: &region_metadata.primary_key,
            table_id: region_metadata.region_id.table_id(),
        }
    }
}

/// Validates the table id and name consistency.
///
/// It will check the table id and table name consistency.
/// If the table id and table name are not consistent, it will return an error.
pub(crate) async fn validate_table_id_and_name(
    table_name_manager: &TableNameManager,
    table_id: TableId,
    table_name: &TableName,
) -> Result<()> {
    let table_name_key = TableNameKey::new(
        &table_name.catalog_name,
        &table_name.schema_name,
        &table_name.table_name,
    );
    let table_name_value = table_name_manager
        .get(table_name_key)
        .await?
        .with_context(|| error::TableNotFoundSnafu {
            table_name: table_name.to_string(),
        })?;

    ensure!(
        table_name_value.table_id() == table_id,
        error::UnexpectedSnafu {
            err_msg: format!(
                "The table id mismatch for table: {}, expected {}, actual {}",
                table_name,
                table_id,
                table_name_value.table_id()
            ),
        }
    );

    Ok(())
}

#[derive(Clone)]
pub struct Context {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
}

/// Checks if the column metadatas are consistent.
///
/// The column metadatas are consistent if:
/// - The column metadatas are the same.
/// - The primary key are the same.
/// - The table id of the region metadatas are the same.
///
/// ## Panic
/// Panic if region_metadatas is empty.
pub(crate) fn check_column_metadatas_consistent(
    region_metadatas: &[RegionMetadata],
) -> Option<Vec<ColumnMetadata>> {
    let is_column_metadata_consistent = region_metadatas
        .windows(2)
        .all(|w| PartialRegionMetadata::from(&w[0]) == PartialRegionMetadata::from(&w[1]));

    if !is_column_metadata_consistent {
        return None;
    }

    Some(region_metadatas[0].column_metadatas.clone())
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::storage::RegionId;

    use crate::ddl::test_util::region_metadata::build_region_metadata;
    use crate::reconciliation::utils::check_column_metadatas_consistent;

    fn new_test_column_metadatas() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            },
        ]
    }

    #[test]
    fn test_check_column_metadatas_consistent() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result =
            check_column_metadatas_consistent(&[region_metadata1, region_metadata2]).unwrap();
        assert_eq!(result, column_metadatas);

        let region_metadata1 = build_region_metadata(RegionId::new(1025, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result = check_column_metadatas_consistent(&[region_metadata1, region_metadata2]);
        assert!(result.is_none());
    }
}
