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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::ValueRef;
use mito2::row_converter::{RowCodec, SortField, SparseRowCodec};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_ENCODED_PRIMARY_KEY_COLUMN_NAME, DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
    DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::ColumnId;

use crate::error::Result;

pub type CodecRef = Arc<Codec>;

pub struct Codec {
    pub(crate) codec: SparseRowCodec,
    pub(crate) region_metadata: RegionMetadataRef,
}

impl Codec {
    pub fn new(region_metadata: &RegionMetadataRef) -> Self {
        let mut fields = Vec::with_capacity(region_metadata.primary_key.len() + 2);
        fields.push((
            ReservedColumnId::table_id(),
            SortField::new(ConcreteDataType::uint32_datatype()),
        ));
        fields.push((
            ReservedColumnId::tsid(),
            SortField::new(ConcreteDataType::uint64_datatype()),
        ));
        for column in region_metadata.column_metadatas.iter() {
            if column.semantic_type == SemanticType::Tag {
                fields.push((
                    column.column_id,
                    SortField::new(column.column_schema.data_type.clone()),
                ));
            }
        }
        Self {
            codec: SparseRowCodec::new_partial_fields(fields),
            region_metadata: region_metadata.clone(),
        }
    }

    pub fn encode_rows(&self, mut iter: RowsIter) -> Result<Rows> {
        let num_column = iter.rows.schema.len();
        let num_primary_key_column = iter.index.num_primary_key_column;
        // num_output_column = remaining columns(fields columns + timestamp column) + 1 (encoded primary key column)
        let num_output_column = num_column - num_primary_key_column + 1;

        // Build output rows
        let mut output = Vec::with_capacity(iter.len());
        for mut row in iter.iter() {
            let mut values = Vec::with_capacity(num_output_column);
            let encoded = self.codec.encode(row.primary_keys()).unwrap();
            values.push(ValueData::BinaryValue(encoded).into());
            values.extend(row.remaining());
            output.push(Row { values });
        }

        // Builds output schema
        let mut schema = Vec::with_capacity(num_output_column);
        schema.push(ColumnSchema {
            column_name: DATA_SCHEMA_ENCODED_PRIMARY_KEY_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        });
        schema.extend(iter.remaining_columns());

        Ok(Rows {
            rows: output,
            schema,
        })
    }
}

pub struct RowsIter {
    rows: Rows,
    index: IterIndex,
}

impl RowsIter {
    pub fn new(rows: Rows, metadata: &RegionMetadataRef) -> Self {
        let index: IterIndex = IterIndex::new(&rows.schema, metadata);
        Self { rows, index }
    }

    fn iter(&mut self) -> impl Iterator<Item = RowIter> {
        self.rows.rows.drain(..).map(|row| RowIter {
            row,
            index: &self.index,
            schema: &self.rows.schema,
        })
    }

    fn len(&self) -> usize {
        self.rows.rows.len()
    }

    fn remaining_columns(&mut self) -> impl Iterator<Item = ColumnSchema> + '_ {
        self.index.indices[self.index.num_primary_key_column..]
            .iter()
            .map(|idx| std::mem::take(&mut self.rows.schema[idx.index]))
    }
}

struct RowIter<'a> {
    row: Row,
    index: &'a IterIndex,
    schema: &'a Vec<ColumnSchema>,
}

impl RowIter<'_> {
    fn primary_keys(&self) -> impl Iterator<Item = (ColumnId, ValueRef)> {
        self.index.indices[..self.index.num_primary_key_column]
            .iter()
            .map(|idx| {
                common_telemetry::debug!("primary key: {:?}", idx);
                (
                    idx.column_id,
                    api::helper::pb_value_to_value_ref(
                        &self.row.values[idx.index],
                        &self.schema[idx.index].datatype_extension,
                    ),
                )
            })
    }

    fn remaining(&mut self) -> impl Iterator<Item = Value> + '_ {
        self.index.indices[self.index.num_primary_key_column..]
            .iter()
            .map(|idx| std::mem::take(&mut self.row.values[idx.index]))
    }
}

#[derive(Debug)]
struct ValueIndex {
    column_id: ColumnId,
    index: usize,
}

struct IterIndex {
    indices: Vec<ValueIndex>,
    num_primary_key_column: usize,
}

impl IterIndex {
    fn new(row_schema: &[ColumnSchema], metadata: &RegionMetadataRef) -> Self {
        let name_to_index = row_schema
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.column_name.as_str(), idx))
            .collect::<HashMap<_, _>>();

        let mut indices = Vec::with_capacity(metadata.column_metadatas.len());
        let mut num_primary_key_column = 0;
        // TODO: internal column should be added

        for (pk_column_id, name) in [
            (
                ReservedColumnId::table_id(),
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            ),
            (ReservedColumnId::tsid(), DATA_SCHEMA_TSID_COLUMN_NAME),
        ] {
            if let Some(index) = name_to_index.get(name) {
                indices.push(ValueIndex {
                    column_id: pk_column_id,
                    index: *index,
                });
                num_primary_key_column += 1;
            }
        }

        for pk_column_id in &metadata.primary_key {
            // Safety: Id comes from primary key.
            let column = metadata.column_by_id(*pk_column_id).unwrap();
            if let Some(index) = name_to_index.get(column.column_schema.name.as_str()) {
                indices.push(ValueIndex {
                    column_id: *pk_column_id,
                    index: *index,
                });
                num_primary_key_column += 1;
            }
        }

        // Get timestamp index.
        // Safety: time index must exist
        let ts_index = name_to_index
            .get(metadata.time_index_column().column_schema.name.as_str())
            .unwrap();
        indices.push(ValueIndex {
            column_id: metadata.time_index_column().column_id,
            index: *ts_index,
        });

        // Iterate columns and find field columns.
        for column in metadata.field_columns() {
            // Get index in request for each field column.
            if let Some(index) = name_to_index.get(column.column_schema.name.as_str()) {
                indices.push(ValueIndex {
                    column_id: column.column_id,
                    index: *index,
                });
            }
        }

        common_telemetry::debug!(
            "indices: {:?}, num_primary_key_column: {:?}",
            indices,
            num_primary_key_column
        );
        IterIndex {
            indices,
            num_primary_key_column,
        }
    }
}
