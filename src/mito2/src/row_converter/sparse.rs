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

use datatypes::data_type::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use memcomparable::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{DeserializeFieldSnafu, Result, SerializeFieldSnafu};
use crate::row_converter::{
    CompositeValues, LeftMostValueDecoder, RowCodec, SortField, SparseValue,
};

pub struct MetricRowCodec {
    table_id_field: SortField,
    tsid_field: SortField,
    label_field: SortField,
}

impl MetricRowCodec {
    pub fn new() -> Self {
        Self {
            table_id_field: SortField::new(ConcreteDataType::uint32_datatype()),
            tsid_field: SortField::new(ConcreteDataType::uint64_datatype()),
            label_field: SortField::new(ConcreteDataType::string_datatype()),
        }
    }
}

impl RowCodec for MetricRowCodec {
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        let mut serializer = Serializer::new(buffer);
        for (idx, (column_id, value)) in row.enumerate() {
            match idx {
                0 => {
                    column_id
                        .serialize(&mut serializer)
                        .context(SerializeFieldSnafu)?;
                    self.table_id_field.serialize(&mut serializer, &value)?;
                }
                1 => {
                    column_id
                        .serialize(&mut serializer)
                        .context(SerializeFieldSnafu)?;
                    self.tsid_field.serialize(&mut serializer, &value)?;
                }
                _ => {
                    column_id
                        .serialize(&mut serializer)
                        .context(SerializeFieldSnafu)?;
                    self.label_field.serialize(&mut serializer, &value)?;
                }
            }
        }

        Ok(())
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        unimplemented!()
    }

    fn estimated_size(&self) -> usize {
        16
    }
}

#[derive(Debug)]
pub struct SparseRowCodec {
    table_id_decoder: SortField,
    tsid_decoder: SortField,
    label_decoder: SortField,
    pub fields: HashMap<ColumnId, SortField>,
    pub ordered_fields: Vec<(ColumnId, SortField)>,
}

impl SparseRowCodec {
    pub fn new(region_metadata: &RegionMetadataRef) -> Self {
        Self {
            table_id_decoder: SortField::new(ConcreteDataType::uint32_datatype()),
            tsid_decoder: SortField::new(ConcreteDataType::uint64_datatype()),
            label_decoder: SortField::new(ConcreteDataType::string_datatype()),
            fields: region_metadata
                .primary_key_columns()
                .map(|c| {
                    (
                        c.column_id,
                        SortField::new(c.column_schema.data_type.clone()),
                    )
                })
                .collect(),
            ordered_fields: region_metadata
                .primary_key_columns()
                .map(|c| {
                    (
                        c.column_id,
                        SortField::new(c.column_schema.data_type.clone()),
                    )
                })
                .collect(),
        }
    }

    pub fn new_partial_fields(fields: Vec<(ColumnId, SortField)>) -> Self {
        Self {
            table_id_decoder: SortField::new(ConcreteDataType::uint32_datatype()),
            tsid_decoder: SortField::new(ConcreteDataType::uint64_datatype()),
            label_decoder: SortField::new(ConcreteDataType::string_datatype()),
            fields: fields.clone().into_iter().collect(),
            ordered_fields: fields,
        }
    }

    pub(crate) fn has_column(
        &self,
        pk: &[u8],
        offsets_map: &mut HashMap<u32, usize>,
        column_id: ColumnId,
    ) -> Option<usize> {
        if offsets_map.is_empty() {
            let mut deserializer = Deserializer::new(pk);
            let mut offset = 0;
            while deserializer.has_remaining() {
                let column_id = u32::deserialize(&mut deserializer).unwrap();
                offset += 4;
                offsets_map.insert(column_id, offset);
                let Some(field) = self.fields.get(&column_id) else {
                    break;
                };

                let skip = field.skip_deserialize(pk, &mut deserializer).unwrap();
                offset += skip;
            }

            offsets_map.get(&column_id).copied()
        } else {
            offsets_map.get(&column_id).copied()
        }
    }

    /// Decode value at `offset` in `pk`.
    pub(crate) fn decode_value_at(
        &self,
        pk: &[u8],
        offset: usize,
        column_id: ColumnId,
    ) -> Result<Value> {
        let mut deserializer = Deserializer::new(pk);
        deserializer.advance(offset);
        let field = self.fields.get(&column_id).unwrap();
        field
            .deserialize(&mut deserializer)
            .inspect_err(|e| common_telemetry::error!(e; "Failed to decode primary key, column_id: {:?}, pk: {:?}", column_id, pk))
    }

    pub fn decode_dense(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::with_capacity(self.ordered_fields.len());
        let mut offset_map = HashMap::new();
        for (col_id, _) in self.ordered_fields.iter() {
            if let Some(offset) = self.has_column(bytes, &mut offset_map, *col_id) {
                let value = self.decode_value_at(bytes, offset, *col_id)?;
                values.push(value);
            } else {
                values.push(Value::Null);
            }
        }
        Ok(values)
    }
}

impl RowCodec for SparseRowCodec {
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        let mut serializer = Serializer::new(buffer);
        for (column_id, value) in row {
            if !value.is_null() {
                if let Some(field) = &self.fields.get(&column_id) {
                    column_id
                        .serialize(&mut serializer)
                        .context(SerializeFieldSnafu)?;
                    field.serialize(&mut serializer, &value)?;
                } else {
                    common_telemetry::debug!(
                        "field not found encode column: {} field: {:?}",
                        column_id,
                        value
                    );
                }
            }
        }

        Ok(())
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = SparseValue::new(HashMap::new());

        let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
        let value = self.table_id_decoder.deserialize(&mut deserializer)?;
        values.insert(column_id, value);

        let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
        let value = self.tsid_decoder.deserialize(&mut deserializer)?;
        values.insert(column_id, value);
        while deserializer.has_remaining() {
            let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
            let value = self.label_decoder.deserialize(&mut deserializer)?;
            values.insert(column_id, value);
        }

        Ok(CompositeValues::Sparse(values))
    }

    fn estimated_size(&self) -> usize {
        self.fields.len() * 4 + 16
    }
}

pub struct LeftMostSparseRowCodec {
    leftmost_field: SortField,
}

impl LeftMostSparseRowCodec {
    pub fn new(data_type: ConcreteDataType) -> Self {
        Self {
            leftmost_field: SortField::new(data_type),
        }
    }
}

impl LeftMostValueDecoder for LeftMostSparseRowCodec {
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        let mut deserializer = Deserializer::new(bytes);
        let _ = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
        let value = self.leftmost_field.deserialize(&mut deserializer)?;
        Ok(Some(value))
    }
}
