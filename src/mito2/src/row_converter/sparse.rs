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

#[derive(Debug)]
pub struct SparseRowCodec {
    table_id_decoder: SortField,
    tsid_decoder: SortField,
    label_decoder: SortField,
    pub fields: HashMap<ColumnId, SortField>,
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
        }
    }

    pub fn new_partial_fields(fields: Vec<(ColumnId, SortField)>) -> Self {
        Self {
            table_id_decoder: SortField::new(ConcreteDataType::uint32_datatype()),
            tsid_decoder: SortField::new(ConcreteDataType::uint64_datatype()),
            label_decoder: SortField::new(ConcreteDataType::string_datatype()),
            fields: fields.into_iter().collect(),
        }
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
