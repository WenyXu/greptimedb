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

use bytes::Buf;
use common_base::bytes::Bytes;
use common_decimal::Decimal128;
use common_time::time::Time;
use common_time::{Date, Duration, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::types::IntervalType;
use datatypes::value::ValueRef;
use memcomparable::{Deserializer, Serializer};
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error;
use crate::error::{FieldTypeMismatchSnafu, NotSupportedFieldSnafu, Result, SerializeFieldSnafu};

pub trait LeftMostValueDecoder {
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>>;
}

/// Row value encoder/decoder.
pub trait RowCodec: Send + Sync {
    /// Encodes rows to bytes.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode<'a, I>(&self, row: I) -> Result<Vec<u8>>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        let mut buffer = Vec::new();
        self.encode_to_vec(row, &mut buffer)?;
        Ok(buffer)
    }

    /// Encodes rows to specific vec.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>;

    /// Decode row values from bytes.
    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues>;

    fn estimated_size(&self) -> usize;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    pub(crate) data_type: ConcreteDataType,
}

impl SortField {
    pub fn new(data_type: ConcreteDataType) -> Self {
        Self { data_type }
    }

    pub fn estimated_size(&self) -> usize {
        match &self.data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_)
            | ConcreteDataType::Json(_)
            | ConcreteDataType::Vector(_) => 11,
            ConcreteDataType::String(_) => 11, // a non-empty string takes at least 11 bytes.
            ConcreteDataType::Date(_) => 5,
            ConcreteDataType::DateTime(_) => 9,
            ConcreteDataType::Timestamp(_) => 10,
            ConcreteDataType::Time(_) => 10,
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(_) => 18,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => 0,
        }
    }
}

impl SortField {
    pub(crate) fn serialize(
        &self,
        serializer: &mut Serializer<&mut Vec<u8>>,
        value: &ValueRef,
    ) -> Result<()> {
        macro_rules! cast_value_and_serialize {
            (
                $self: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {
                match &$self.data_type {
                $(
                    ConcreteDataType::$ty(_) => {
                        paste!{
                            value
                            .[<as_ $f>]()
                            .context(FieldTypeMismatchSnafu)?
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                        }
                    }
                )*
                    ConcreteDataType::Timestamp(_) => {
                        let timestamp = value.as_timestamp().context(FieldTypeMismatchSnafu)?;
                        timestamp
                            .map(|t|t.value())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                        let interval = value.as_interval_year_month().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i32())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                        let interval = value.as_interval_day_time().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i64())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                        let interval = value.as_interval_month_day_nano().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i128())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::List(_) |
                    ConcreteDataType::Dictionary(_) |
                    ConcreteDataType::Null(_) => {
                        return error::NotSupportedFieldSnafu {
                            data_type: $self.data_type.clone()
                        }.fail()
                    }
                }
            };
        }
        cast_value_and_serialize!(self; serializer;
            Boolean, boolean,
            Binary, binary,
            Int8, i8,
            UInt8, u8,
            Int16, i16,
            UInt16, u16,
            Int32, i32,
            UInt32, u32,
            Int64, i64,
            UInt64, u64,
            Float32, f32,
            Float64, f64,
            String, string,
            Date, date,
            DateTime, datetime,
            Time, time,
            Duration, duration,
            Decimal128, decimal128,
            Json, binary,
            Vector, binary
        );

        Ok(())
    }

    fn deserialize<B: Buf>(&self, deserializer: &mut Deserializer<B>) -> Result<Value> {
        use common_time::DateTime;
        macro_rules! deserialize_and_build_value {
            (
                $self: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {

                match &$self.data_type {
                    $(
                        ConcreteDataType::$ty(_) => {
                            Ok(Value::from(Option::<$f>::deserialize(deserializer).context(error::DeserializeFieldSnafu)?))
                        }
                    )*
                    ConcreteDataType::Binary(_) | ConcreteDataType::Json(_) | ConcreteDataType::Vector(_) => Ok(Value::from(
                        Option::<Vec<u8>>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(Bytes::from),
                    )),
                    ConcreteDataType::Timestamp(ty) => {
                        let timestamp = Option::<i64>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(|t|ty.create_timestamp(t));
                        Ok(Value::from(timestamp))
                    }
                    ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                        let interval = Option::<i32>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalYearMonth::from_i32);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                        let interval = Option::<i64>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalDayTime::from_i64);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                        let interval = Option::<i128>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalMonthDayNano::from_i128);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::List(l) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::List(l.clone()),
                    }
                    .fail(),
                    ConcreteDataType::Dictionary(d) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::Dictionary(d.clone()),
                    }
                    .fail(),
                    ConcreteDataType::Null(n) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::Null(n.clone()),
                    }
                    .fail(),
                }
            };
        }
        deserialize_and_build_value!(self; deserializer;
            Boolean, bool,
            Int8, i8,
            Int16, i16,
            Int32, i32,
            Int64, i64,
            UInt8, u8,
            UInt16, u16,
            UInt32, u32,
            UInt64, u64,
            Float32, f32,
            Float64, f64,
            String, String,
            Date, Date,
            Time, Time,
            DateTime, DateTime,
            Duration, Duration,
            Decimal128, Decimal128
        )
    }

    /// Skip deserializing this field, returns the length of it.
    fn skip_deserialize(
        &self,
        bytes: &[u8],
        deserializer: &mut Deserializer<&[u8]>,
    ) -> Result<usize> {
        let pos = deserializer.position();
        if bytes[pos] == 0 {
            deserializer.advance(1);
            return Ok(1);
        }

        let to_skip = match &self.data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_)
            | ConcreteDataType::Json(_)
            | ConcreteDataType::Vector(_) => {
                // Now the encoder encode binary as a list of bytes so we can't use
                // skip bytes.
                let pos_before = deserializer.position();
                let mut current = pos_before + 1;
                while bytes[current] == 1 {
                    current += 2;
                }
                let to_skip = current - pos_before + 1;
                deserializer.advance(to_skip);
                return Ok(to_skip);
            }
            ConcreteDataType::String(_) => {
                let pos_before = deserializer.position();
                deserializer.advance(1);
                deserializer
                    .skip_bytes()
                    .context(error::DeserializeFieldSnafu)?;
                return Ok(deserializer.position() - pos_before);
            }
            ConcreteDataType::Date(_) => 5,
            ConcreteDataType::DateTime(_) => 9,
            ConcreteDataType::Timestamp(_) => 9, // We treat timestamp as Option<i64>
            ConcreteDataType::Time(_) => 10,     // i64 and 1 byte time unit
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(IntervalType::YearMonth(_)) => 5,
            ConcreteDataType::Interval(IntervalType::DayTime(_)) => 9,
            ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => 17,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => 0,
        };
        deserializer.advance(to_skip);
        Ok(to_skip)
    }
}

pub enum CompositeLeftMostValueDecoder {
    Full(McmpRowCodec),
    Sparse(LeftMostSparseRowCodec),
}

impl CompositeLeftMostValueDecoder {
    pub fn new(primary_key_encoding: PrimaryKeyEncoding, data_type: ConcreteDataType) -> Self {
        match primary_key_encoding {
            PrimaryKeyEncoding::Full => {
                Self::Full(McmpRowCodec::new(vec![SortField::new(data_type)]))
            }
            PrimaryKeyEncoding::Sparse => Self::Sparse(LeftMostSparseRowCodec::new(data_type)),
        }
    }
}

impl LeftMostValueDecoder for CompositeLeftMostValueDecoder {
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        match self {
            CompositeLeftMostValueDecoder::Full(decoder) => decoder.decode_leftmost(bytes),
            CompositeLeftMostValueDecoder::Sparse(decoder) => decoder.decode_leftmost(bytes),
        }
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
        let _ = u32::deserialize(&mut deserializer).unwrap();
        let value = self.leftmost_field.deserialize(&mut deserializer)?;
        Ok(Some(value))
    }
}

impl LeftMostValueDecoder for McmpRowCodec {
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        let mut values = self.decode_values(bytes)?;
        Ok(values.pop())
    }
}

/// A composite row codec that can be either full or sparse.
#[derive(Debug)]
pub enum CompositeRowCodec {
    Full(McmpRowCodec),
    Sparse(SparseRowCodec),
}

impl CompositeRowCodec {
    pub fn new(region_metadata: &RegionMetadataRef) -> Self {
        match region_metadata.primary_key_encoding {
            PrimaryKeyEncoding::Full => {
                Self::Full(McmpRowCodec::new_with_primary_keys(region_metadata))
            }
            PrimaryKeyEncoding::Sparse => Self::Sparse(SparseRowCodec::new(region_metadata)),
        }
    }

    pub fn new_partial_fields(
        primary_key_encoding: PrimaryKeyEncoding,
        fields: Vec<(ColumnId, SortField)>,
    ) -> Self {
        match primary_key_encoding {
            PrimaryKeyEncoding::Full => Self::Full(McmpRowCodec::new(
                fields.into_iter().map(|(_, f)| f).collect(),
            )),
            PrimaryKeyEncoding::Sparse => Self::Sparse(SparseRowCodec::new_partial_fields(fields)),
        }
    }

    pub fn empty_composite_values(&self) -> CompositeValues {
        match self {
            CompositeRowCodec::Full(_) => CompositeValues::Dense(Vec::new()),
            CompositeRowCodec::Sparse(_) => {
                CompositeValues::Sparse(SparseValue::new(HashMap::new()))
            }
        }
    }

    pub fn is_sparse(&self) -> bool {
        matches!(self, CompositeRowCodec::Sparse(_))
    }
}

impl RowCodec for CompositeRowCodec {
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        match self {
            CompositeRowCodec::Full(codec) => codec.encode_to_vec(row, buffer),
            CompositeRowCodec::Sparse(codec) => codec.encode_to_vec(row, buffer),
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        match self {
            CompositeRowCodec::Full(codec) => codec.decode(bytes),
            CompositeRowCodec::Sparse(codec) => codec.decode(bytes),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            CompositeRowCodec::Full(codec) => codec.estimated_size(),
            CompositeRowCodec::Sparse(codec) => codec.estimated_size(),
        }
    }
}

#[derive(Debug)]
pub struct SparseRowCodec {
    table_id_decoder: SortField,
    tsid_decoder: SortField,
    label_decoder: SortField,
    fields: HashMap<ColumnId, SortField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseValue {
    values: HashMap<ColumnId, Value>,
}

impl IntoIterator for SparseValue {
    type Item = (ColumnId, Value);
    type IntoIter = std::collections::hash_map::IntoIter<ColumnId, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl SparseValue {
    /// Creates a new [`SpraseValue`] instance.
    pub fn new(values: HashMap<ColumnId, Value>) -> Self {
        Self { values }
    }

    /// Returns the value of the given column, or [`Value::Null`] if the column is not present.
    pub fn get_or_null(&self, column_id: ColumnId) -> &Value {
        self.values.get(&column_id).unwrap_or(&Value::Null)
    }

    /// Inserts a new value into the [`SpraseValue`].
    pub fn insert(&mut self, column_id: ColumnId, value: Value) {
        self.values.insert(column_id, value);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeValues {
    Dense(Vec<Value>),
    Sparse(SparseValue),
}

#[cfg(test)]
impl CompositeValues {
    pub fn into_sparse(self) -> SparseValue {
        match self {
            CompositeValues::Sparse(v) => v,
            _ => panic!("CompositeValues is not sparse"),
        }
    }

    pub fn into_dense(self) -> Vec<Value> {
        match self {
            CompositeValues::Dense(v) => v,
            _ => panic!("CompositeValues is not dense"),
        }
    }
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
                }
            }
        }

        Ok(())
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = SparseValue::new(HashMap::new());

        let column_id = u32::deserialize(&mut deserializer).unwrap();
        let value = self.table_id_decoder.deserialize(&mut deserializer)?;
        values.insert(column_id, value);

        let column_id = u32::deserialize(&mut deserializer).unwrap();
        let value = self.tsid_decoder.deserialize(&mut deserializer)?;
        values.insert(column_id, value);
        while deserializer.has_remaining() {
            let column_id = u32::deserialize(&mut deserializer).unwrap();
            let value = self.label_decoder.deserialize(&mut deserializer)?;
            values.insert(column_id, value);
        }

        Ok(CompositeValues::Sparse(values))
    }

    fn estimated_size(&self) -> usize {
        self.fields.len() * 4 + 16
    }
}

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

#[cfg(test)]
mod tests {
    use common_base::bytes::StringBytes;
    use common_time::{
        DateTime, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp,
    };
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
