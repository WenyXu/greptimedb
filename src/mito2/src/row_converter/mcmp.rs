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

use datatypes::prelude::Value;
use datatypes::value::ValueRef;
use memcomparable::{Deserializer, Serializer};
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::row_converter::{CompositeValues, LeftMostValueDecoder, RowCodec, SortField};

/// A memory-comparable row [Value] encoder/decoder.
#[derive(Debug)]
pub struct McmpRowCodec {
    fields: Vec<SortField>,
}

impl McmpRowCodec {
    /// Creates [McmpRowCodec] instance with all primary keys in given `metadata`.
    pub fn new_with_primary_keys(metadata: &RegionMetadata) -> Self {
        Self::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        )
    }

    pub fn new(fields: Vec<SortField>) -> Self {
        Self { fields }
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Estimated length for encoded bytes.
    pub fn estimated_size(&self) -> usize {
        self.fields.iter().map(|f| f.estimated_size()).sum()
    }

    /// Decode value at `pos` in `bytes`.
    ///
    /// The i-th element in offsets buffer is how many bytes to skip in order to read value at `pos`.
    pub fn decode_value_at(
        &self,
        bytes: &[u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<Value> {
        let mut deserializer = Deserializer::new(bytes);
        if pos < offsets_buf.len() {
            // We computed the offset before.
            let to_skip = offsets_buf[pos];
            deserializer.advance(to_skip);
            return self.fields[pos].deserialize(&mut deserializer);
        }

        if offsets_buf.is_empty() {
            let mut offset = 0;
            // Skip values before `pos`.
            for i in 0..pos {
                // Offset to skip before reading value i.
                offsets_buf.push(offset);
                let skip = self.fields[i].skip_deserialize(bytes, &mut deserializer)?;
                offset += skip;
            }
            // Offset to skip before reading this value.
            offsets_buf.push(offset);
        } else {
            // Offsets are not enough.
            let value_start = offsets_buf.len() - 1;
            // Advances to decode value at `value_start`.
            let mut offset = offsets_buf[value_start];
            deserializer.advance(offset);
            for i in value_start..pos {
                // Skip value i.
                let skip = self.fields[i].skip_deserialize(bytes, &mut deserializer)?;
                // Offset for the value at i + 1.
                offset += skip;
                offsets_buf.push(offset);
            }
        }

        self.fields[pos].deserialize(&mut deserializer)
    }

    pub fn decode_values(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = Vec::with_capacity(self.fields.len());
        for f in &self.fields {
            let value = f.deserialize(&mut deserializer)?;
            values.push(value);
        }

        Ok(values)
    }
}

impl RowCodec for McmpRowCodec {
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        buffer.reserve(self.estimated_size());
        let mut serializer = Serializer::new(buffer);
        for ((_, value), field) in row.zip(self.fields.iter()) {
            field.serialize(&mut serializer, &value)?;
        }
        Ok(())
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        let values = self.decode_values(bytes)?;
        Ok(CompositeValues::Dense(values))
    }

    fn estimated_size(&self) -> usize {
        self.fields.iter().map(|f| f.estimated_size()).sum()
    }
}

impl LeftMostValueDecoder for McmpRowCodec {
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        let mut values = self.decode_values(bytes)?;
        Ok(values.pop())
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::{Bytes, StringBytes};
    use common_decimal::Decimal128;
    use common_time::time::Time;
    use common_time::{
        Date, DateTime, Duration, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth,
        Timestamp,
    };
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;

    use super::*;

    fn check_encode_and_decode(data_types: &[ConcreteDataType], row: Vec<Value>) {
        let encoder = McmpRowCodec::new(
            data_types
                .iter()
                .map(|t| SortField::new(t.clone()))
                .collect::<Vec<_>>(),
        );

        let value_ref = row
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, v.as_value_ref()))
            .collect::<Vec<_>>();

        let result = encoder.encode(value_ref.iter().cloned()).unwrap();
        let decoded = encoder.decode(&result).unwrap().into_dense();
        assert_eq!(decoded, row);
        let mut decoded = Vec::new();
        let mut offsets = Vec::new();
        // Iter two times to test offsets buffer.
        for _ in 0..2 {
            decoded.clear();
            for i in 0..data_types.len() {
                let value = encoder.decode_value_at(&result, i, &mut offsets).unwrap();
                decoded.push(value);
            }
            assert_eq!(data_types.len(), offsets.len(), "offsets: {:?}", offsets);
            assert_eq!(decoded, row);
        }
    }

    #[test]
    fn test_memcmp() {
        let encoder = McmpRowCodec::new(vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::int64_datatype()),
        ]);
        let values = [Value::String("abcdefgh".into()), Value::Int64(128)];
        let value_ref = values
            .iter()
            .map(|v| (0, v.as_value_ref()))
            .collect::<Vec<_>>();
        let result = encoder.encode(value_ref.iter().cloned()).unwrap();

        let decoded = encoder.decode(&result).unwrap().into_dense();
        assert_eq!(&values, &decoded as &[Value]);
    }

    #[test]
    fn test_memcmp_timestamp() {
        check_encode_and_decode(
            &[
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Timestamp(Timestamp::new_millisecond(42)),
                Value::Int64(43),
            ],
        );
    }

    #[test]
    fn test_memcmp_duration() {
        check_encode_and_decode(
            &[
                ConcreteDataType::duration_millisecond_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Duration(Duration::new_millisecond(44)),
                Value::Int64(45),
            ],
        )
    }

    #[test]
    fn test_memcmp_binary() {
        check_encode_and_decode(
            &[
                ConcreteDataType::binary_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Binary(Bytes::from("hello".as_bytes())),
                Value::Int64(43),
            ],
        );
    }

    #[test]
    fn test_memcmp_string() {
        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String(StringBytes::from("hello"))],
        );

        check_encode_and_decode(&[ConcreteDataType::string_datatype()], vec![Value::Null]);

        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String("".into())],
        );
        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String("world".into())],
        );
    }

    #[test]
    fn test_encode_null() {
        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int32_datatype(),
            ],
            vec![Value::String(StringBytes::from("abcd")), Value::Null],
        )
    }

    #[test]
    fn test_encode_multiple_rows() {
        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![
                Value::String("hello".into()),
                Value::Int64(42),
                Value::Boolean(false),
            ],
        );

        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![
                Value::String("world".into()),
                Value::Int64(43),
                Value::Boolean(true),
            ],
        );

        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![Value::Null, Value::Int64(43), Value::Boolean(true)],
        );

        // All types.
        check_encode_and_decode(
            &[
                ConcreteDataType::boolean_datatype(),
                ConcreteDataType::int8_datatype(),
                ConcreteDataType::uint8_datatype(),
                ConcreteDataType::int16_datatype(),
                ConcreteDataType::uint16_datatype(),
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::uint64_datatype(),
                ConcreteDataType::float32_datatype(),
                ConcreteDataType::float64_datatype(),
                ConcreteDataType::binary_datatype(),
                ConcreteDataType::string_datatype(),
                ConcreteDataType::date_datatype(),
                ConcreteDataType::datetime_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::time_millisecond_datatype(),
                ConcreteDataType::duration_millisecond_datatype(),
                ConcreteDataType::interval_year_month_datatype(),
                ConcreteDataType::interval_day_time_datatype(),
                ConcreteDataType::interval_month_day_nano_datatype(),
                ConcreteDataType::decimal128_default_datatype(),
                ConcreteDataType::vector_datatype(3),
            ],
            vec![
                Value::Boolean(true),
                Value::Int8(8),
                Value::UInt8(8),
                Value::Int16(16),
                Value::UInt16(16),
                Value::Int32(32),
                Value::UInt32(32),
                Value::Int64(64),
                Value::UInt64(64),
                Value::Float32(1.0.into()),
                Value::Float64(1.0.into()),
                Value::Binary(b"hello"[..].into()),
                Value::String("world".into()),
                Value::Date(Date::new(10)),
                Value::DateTime(DateTime::new(11)),
                Value::Timestamp(Timestamp::new_millisecond(12)),
                Value::Time(Time::new_millisecond(13)),
                Value::Duration(Duration::new_millisecond(14)),
                Value::IntervalYearMonth(IntervalYearMonth::new(1)),
                Value::IntervalDayTime(IntervalDayTime::new(1, 15)),
                Value::IntervalMonthDayNano(IntervalMonthDayNano::new(1, 1, 15)),
                Value::Decimal128(Decimal128::from(16)),
                Value::Binary(Bytes::from(vec![0; 12])),
            ],
        );
    }
}
