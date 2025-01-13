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

pub mod mcmp;
pub mod sparse;

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
pub use mcmp::McmpRowCodec;
use memcomparable::{Deserializer, Serializer};
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
pub use sparse::{LeftMostSparseRowCodec, SparseRowCodec};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
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

    fn decode_dense(&self, _bytes: &[u8]) -> Result<Vec<Value>> {
        unimplemented!()
    }

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

    pub fn decode_value_at(
        &self,
        bytes: &[u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<Value> {
        match self {
            CompositeRowCodec::Full(codec) => codec.decode_value_at(bytes, pos, offsets_buf),
            CompositeRowCodec::Sparse(_) => unreachable!(),
        }
    }

    pub fn has_column(
        &self,
        pk: &[u8],
        offsets_map: &mut HashMap<u32, usize>,
        column_id: ColumnId,
    ) -> Option<usize> {
        match self {
            CompositeRowCodec::Full(_) => None,
            CompositeRowCodec::Sparse(codec) => {
                if offsets_map.is_empty() {
                    let mut deserializer = Deserializer::new(pk);
                    let mut offset = 0;
                    while deserializer.has_remaining() {
                        let column_id = u32::deserialize(&mut deserializer).unwrap();
                        offset += 4;
                        offsets_map.insert(column_id, offset);
                        let Some(field) = codec.fields.get(&column_id) else {
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
        }
    }

    /// Decode value at `offset` in `pk`.
    pub(crate) fn sparse_decode_value_at(
        &self,
        pk: &[u8],
        offset: usize,
        column_id: ColumnId,
    ) -> Result<Value> {
        match self {
            CompositeRowCodec::Full(_) => unreachable!(),
            CompositeRowCodec::Sparse(codec) => {
                let mut deserializer = Deserializer::new(pk);
                deserializer.advance(offset);
                let field = codec.fields.get(&column_id).unwrap();
                field
                    .deserialize(&mut deserializer)
                    .inspect_err(|e| common_telemetry::error!(e; "Failed to decode primary key, column_id: {:?}, pk: {:?}", column_id, pk))
            }
        }
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

    fn decode_dense(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        match self {
            CompositeRowCodec::Full(codec) => codec.decode_values(bytes),
            CompositeRowCodec::Sparse(codec) => codec.decode_dense(bytes),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            CompositeRowCodec::Full(codec) => codec.estimated_size(),
            CompositeRowCodec::Sparse(codec) => codec.estimated_size(),
        }
    }
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
