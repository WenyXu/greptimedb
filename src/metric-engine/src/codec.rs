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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use datatypes::value::ValueRef;
use mito2::row_converter::sparse::MetricRowCodec;
use mito2::row_converter::RowCodec;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_ENCODED_PRIMARY_KEY_COLUMN_NAME, DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
    DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::ColumnId;

use crate::error::Result;

pub struct Codec {
    pub(crate) codec: MetricRowCodec,
}

impl Codec {
    pub fn new() -> Self {
        Self {
            codec: MetricRowCodec::new(),
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
    pub fn new(rows: Rows, name_to_column_id: &HashMap<String, ColumnId>) -> Self {
        let index: IterIndex = IterIndex::new(&rows.schema, name_to_column_id);
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
    fn new(row_schema: &[ColumnSchema], name_to_column_id: &HashMap<String, ColumnId>) -> Self {
        let mut reserved_indices = Vec::with_capacity(2);
        let mut primary_key_indices = Vec::with_capacity(row_schema.len());
        let mut field_indices = Vec::with_capacity(row_schema.len());
        let mut ts_index = None;
        for (idx, col) in row_schema.iter().enumerate() {
            if col.semantic_type == SemanticType::Tag as i32 {
                if col.column_name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME {
                    reserved_indices.push(ValueIndex {
                        column_id: ReservedColumnId::table_id(),
                        index: idx,
                    });
                } else if col.column_name == DATA_SCHEMA_TSID_COLUMN_NAME {
                    reserved_indices.push(ValueIndex {
                        column_id: ReservedColumnId::tsid(),
                        index: idx,
                    });
                } else {
                    primary_key_indices.push(ValueIndex {
                        column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                        index: idx,
                    });
                }
            } else if col.semantic_type == SemanticType::Field as i32 {
                field_indices.push(ValueIndex {
                    column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                    index: idx,
                });
            } else if col.semantic_type == SemanticType::Timestamp as i32 {
                ts_index = Some(ValueIndex {
                    column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                    index: idx,
                });
            }
        }
        let num_primary_key_column = primary_key_indices.len() + reserved_indices.len();
        let indices = reserved_indices
            .into_iter()
            .chain(primary_key_indices)
            .chain(ts_index)
            .chain(field_indices)
            .collect();
        IterIndex {
            indices,
            num_primary_key_column,
        }
    }
}
