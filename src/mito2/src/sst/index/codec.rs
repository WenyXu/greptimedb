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
use memcomparable::Serializer;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{FieldTypeMismatchSnafu, IndexEncodeNullSnafu, Result};
use crate::row_converter::{CompositeRowCodec, CompositeValues, RowCodec, SortField};

/// Encodes index values according to their data types for sorting and storage use.
pub struct IndexValueCodec;

impl IndexValueCodec {
    /// Serializes a non-null `ValueRef` using the data type defined in `SortField` and writes
    /// the result into a buffer.
    ///
    /// For `String` data types, we don't serialize it via memcomparable, but directly write the
    /// bytes into the buffer, since we have to keep the original string for searching with regex.
    ///
    /// # Arguments
    /// * `value` - The value to be encoded.
    /// * `field` - Contains data type to guide serialization.
    /// * `buffer` - Destination buffer for the serialized value.
    pub fn encode_nonnull_value(
        value: ValueRef,
        field: &SortField,
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        ensure!(!value.is_null(), IndexEncodeNullSnafu);

        if matches!(field.data_type, ConcreteDataType::String(_)) {
            let value = value
                .as_string()
                .context(FieldTypeMismatchSnafu)?
                .context(IndexEncodeNullSnafu)?;
            buffer.extend_from_slice(value.as_bytes());
            Ok(())
        } else {
            buffer.reserve(field.estimated_size());
            let mut serializer = Serializer::new(buffer);
            field.serialize(&mut serializer, &value)
        }
    }
}

/// Decodes primary key values into their corresponding column ids, data types and values.
pub struct IndexValuesCodec {
    /// The decoder for the primary key.
    decoder: CompositeRowCodec,
    /// The data types of tag columns.
    fields: HashMap<ColumnId, (String, SortField)>,
    /// The ordered list of primary keys.
    ordered_fields: Vec<ColumnId>,
}

impl IndexValuesCodec {
    /// Creates a new `IndexValuesCodec` from a list of `ColumnMetadata` of tag columns.
    pub fn from_tag_columns(region_metadata: &RegionMetadataRef) -> Self {
        let decoder = CompositeRowCodec::new(region_metadata);
        let fields = region_metadata
            .primary_key_columns()
            .map(|column| {
                (
                    column.column_id,
                    (
                        column.column_id.to_string(),
                        SortField::new(column.column_schema.data_type.clone()),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        Self {
            decoder,
            fields,
            ordered_fields: region_metadata.primary_key.clone(),
        }
    }

    /// Returns the encoder for the given column id.
    pub fn field_encoder(&self, column_id: &ColumnId) -> Option<&(String, SortField)> {
        self.fields.get(column_id)
    }

    /// Decodes a primary key into its corresponding column ids, data types and values.
    pub fn decode(
        &self,
        primary_key: &[u8],
    ) -> Result<Vec<(ColumnId, Option<Value>)>> {
        let values = self.decoder.decode(primary_key)?;
        match values {
            CompositeValues::Dense(values) => {
                let iter =
                    values
                        .into_iter()
                        .zip(&self.ordered_fields)
                        .map(|(value, column_id)| {
                            if value.is_null() {
                                (*column_id, None)
                            } else {
                                (*column_id, Some(value))
                            }
                        });

                Ok(iter.collect())
            }
            CompositeValues::Sparse(values) => {
                let iter = values
                    .into_iter()
                    .map(|(column_id, value)| (column_id, Some(value)));
                Ok(iter.collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::row_converter::McmpRowCodec;

    #[test]
    fn test_encode_value_basic() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::string_datatype());

        let mut buffer = Vec::new();
        IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer).unwrap();
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_encode_value_type_mismatch() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::int64_datatype());

        let mut buffer = Vec::new();
        let res = IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer);
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_encode_null_value() {
        let value = ValueRef::Null;
        let field = SortField::new(ConcreteDataType::string_datatype());

        let mut buffer = Vec::new();
        let res = IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer);
        assert!(matches!(res, Err(Error::IndexEncodeNull { .. })));
    }

    #[test]
    fn test_decode_primary_key_basic() {
        let primary_key = McmpRowCodec::new(vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::int64_datatype()),
        ])
        .encode([(1, ValueRef::Null), (2, ValueRef::Int64(10))].into_iter())
        .unwrap();

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag0", ConcreteDataType::string_datatype(), true),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag1", ConcreteDataType::int64_datatype(), false),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1, 2]);
        let region_metadata = Arc::new(builder.build().unwrap());

        let codec = IndexValuesCodec::from_tag_columns(&region_metadata);
        let mut iter = codec.decode(&primary_key).unwrap();

        let (column_id, value) = iter.next().unwrap();
        assert_eq!(column_id, 1);
        let (col_id_str, field) = codec.field_encoder(&1).unwrap();
        assert_eq!(col_id_str, "1");
        assert_eq!(field, &SortField::new(ConcreteDataType::string_datatype()));
        assert_eq!(value, None);

        let (column_id, value) = iter.next().unwrap();
        assert_eq!(column_id, 2);
        let (col_id_str, field) = codec.field_encoder(&2).unwrap();
        assert_eq!(col_id_str, "2");
        assert_eq!(field, &SortField::new(ConcreteDataType::int64_datatype()));
        assert_eq!(value, Some(Value::Int64(10)));

        assert!(iter.next().is_none());
    }
}
