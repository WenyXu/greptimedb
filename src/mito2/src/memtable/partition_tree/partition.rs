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

//! Partition of a partition tree.
//!
//! We only support partitioning the tree by pre-defined internal columns.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt};
use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::value::Value;
use memcomparable::Deserializer;
use serde::Deserialize;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use super::tree::SparseEncoder;
use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::data::{DataBatch, DataParts, DATA_INIT_CAP};
use crate::memtable::partition_tree::dedup::DedupReader;
use crate::memtable::partition_tree::shard::{
    BoxedDataBatchSource, Shard, ShardMerger, ShardNode, ShardSource,
};
use crate::memtable::partition_tree::shard_builder::ShardBuilder;
use crate::memtable::partition_tree::{PartitionTreeConfig, PkId};
use crate::memtable::stats::WriteMetrics;
use crate::metrics::PARTITION_TREE_READ_STAGE_ELAPSED;
use crate::read::{Batch, BatchBuilder};
use crate::row_converter::{McmpRowCodec, RowCodec};

/// Key of a partition.
pub type PartitionKey = u32;

/// A tree partition.
pub struct Partition {
    inner: RwLock<Inner>,
    /// Whether to dedup batches.
    dedup: bool,
}

pub type PartitionRef = Arc<Partition>;

impl Partition {
    /// Creates a new partition.
    pub fn new(metadata: RegionMetadataRef, config: &PartitionTreeConfig) -> Self {
        Partition {
            inner: RwLock::new(Inner::new(metadata, config)),
            dedup: config.dedup,
        }
    }

    /// Writes to the partition with a primary key.
    pub fn write_with_key(
        &self,
        primary_key: &mut Vec<u8>,
        row_codec: &McmpRowCodec,
        key_value: KeyValue,
        re_encode: bool,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        // Freeze the shard builder if needed.
        if inner.shard_builder.should_freeze() {
            inner.freeze_active_shard()?;
        }

        // Finds key in shards, now we ensure one key only exists in one shard.
        if let Some(pk_id) = inner.find_key_in_shards(primary_key) {
            inner.write_to_shard(pk_id, &key_value)?;
            inner.num_rows += 1;
            return Ok(());
        }

        // Key does not yet exist in shard or builder, encode and insert the full primary key.
        if re_encode {
            // `primary_key` is sparse, re-encode the full primary key.
            let sparse_key = primary_key.clone();
            primary_key.clear();
            row_codec.encode_to_vec(key_value.primary_keys(), primary_key)?;
            let pk_id = inner.shard_builder.write_with_key(
                primary_key,
                Some(&sparse_key),
                &key_value,
                metrics,
            );
            inner.pk_to_pk_id.insert(sparse_key, pk_id);
        } else {
            // `primary_key` is already the full primary key.
            let pk_id = inner.shard_builder.write_with_key(
                primary_key,
                Some(&primary_key),
                &key_value,
                metrics,
            );
            inner.pk_to_pk_id.insert(std::mem::take(primary_key), pk_id);
        };

        inner.num_rows += 1;
        Ok(())
    }

    /// Writes to the partition without a primary key.
    pub fn write_no_key(&self, key_value: KeyValue) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        // If no primary key, always write to the first shard.
        debug_assert!(!inner.shards.is_empty());
        debug_assert_eq!(1, inner.shard_builder.current_shard_id());

        // A dummy pk id.
        let pk_id = PkId {
            shard_id: 0,
            pk_index: 0,
        };
        inner.shards[0].write_with_pk_id(pk_id, &key_value)?;
        inner.num_rows += 1;

        Ok(())
    }

    /// Scans data in the partition.
    pub fn read(&self, mut context: ReadPartitionContext) -> Result<PartitionReader> {
        let start = Instant::now();

        let key_filter = if context.need_prune_key {
            Some(SparsePrimaryKeyFilter::new(
                context.metadata.clone(),
                context.filters.clone(),
                context.sparse_encoder.clone(),
            ))
        } else {
            None
        };
        let (builder_source, shard_reader_builders) = {
            let inner = self.inner.read().unwrap();
            let mut shard_source = Vec::with_capacity(inner.shards.len() + 1);
            let builder_reader = if !inner.shard_builder.is_empty() {
                let builder_reader = inner.shard_builder.read(&mut context.pk_weights)?;
                Some(builder_reader)
            } else {
                None
            };
            for shard in &inner.shards {
                if !shard.is_empty() {
                    let shard_reader_builder = shard.read()?;
                    shard_source.push(shard_reader_builder);
                }
            }
            (builder_reader, shard_source)
        };

        context.metrics.num_shards += shard_reader_builders.len();
        let mut nodes = shard_reader_builders
            .into_iter()
            .map(|builder| {
                Ok(ShardNode::new(ShardSource::Shard(
                    builder.build(key_filter.clone())?,
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(builder) = builder_source {
            context.metrics.num_builder += 1;
            // Move the initialization of ShardBuilderReader out of read lock.
            let shard_builder_reader =
                builder.build(Some(&context.pk_weights), key_filter.clone())?;
            nodes.push(ShardNode::new(ShardSource::Builder(shard_builder_reader)));
        }

        // Creating a shard merger will invoke next so we do it outside the lock.
        let merger = ShardMerger::try_new(nodes)?;
        if self.dedup {
            let source = DedupReader::try_new(merger)?;
            context.metrics.build_partition_reader += start.elapsed();
            PartitionReader::new(context, Box::new(source))
        } else {
            context.metrics.build_partition_reader += start.elapsed();
            PartitionReader::new(context, Box::new(merger))
        }
    }

    /// Freezes the partition.
    pub fn freeze(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.freeze_active_shard()?;
        Ok(())
    }

    /// Forks the partition.
    ///
    /// Must freeze the partition before fork.
    pub fn fork(&self, metadata: &RegionMetadataRef, config: &PartitionTreeConfig) -> Partition {
        let (shards, shard_builder) = {
            let inner = self.inner.read().unwrap();
            debug_assert!(inner.shard_builder.is_empty());
            let shard_builder = ShardBuilder::new(
                metadata.clone(),
                config,
                inner.shard_builder.current_shard_id(),
            );
            let shards = inner
                .shards
                .iter()
                .map(|shard| shard.fork(metadata.clone()))
                .collect();

            (shards, shard_builder)
        };
        let pk_to_pk_id = {
            let mut inner = self.inner.write().unwrap();
            std::mem::take(&mut inner.pk_to_pk_id)
        };

        Partition {
            inner: RwLock::new(Inner {
                metadata: metadata.clone(),
                shard_builder,
                shards,
                num_rows: 0,
                pk_to_pk_id,
                frozen: false,
            }),
            dedup: self.dedup,
        }
    }

    /// Returns true if the partition has data.
    pub fn has_data(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.num_rows > 0
    }

    /// Gets the stats of the partition.
    pub(crate) fn stats(&self) -> PartitionStats {
        let inner = self.inner.read().unwrap();
        let num_rows = inner.num_rows;
        let shard_num = inner.shards.len();
        let shared_memory_size = inner
            .shards
            .iter()
            .map(|shard| shard.shared_memory_size())
            .sum();
        PartitionStats {
            num_rows,
            shard_num,
            shared_memory_size,
        }
    }

    /// Get partition key from the key value.
    pub(crate) fn get_partition_key(key_value: &KeyValue, is_partitioned: bool) -> PartitionKey {
        if !is_partitioned {
            return PartitionKey::default();
        }

        let Some(value) = key_value.primary_keys().next() else {
            return PartitionKey::default();
        };

        value.as_u32().unwrap().unwrap()
    }

    /// Returns true if the region can be partitioned.
    pub(crate) fn has_multi_partitions(metadata: &RegionMetadataRef) -> bool {
        metadata
            .primary_key_columns()
            .next()
            .map(|meta| meta.column_schema.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .unwrap_or(false)
    }

    /// Returns true if this is a partition column.
    pub(crate) fn is_partition_column(name: &str) -> bool {
        name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
    }
}

pub(crate) struct PartitionStats {
    pub(crate) num_rows: usize,
    pub(crate) shard_num: usize,
    pub(crate) shared_memory_size: usize,
}

#[derive(Default)]
struct PartitionReaderMetrics {
    build_partition_reader: Duration,
    read_source: Duration,
    data_batch_to_batch: Duration,
    num_builder: usize,
    num_shards: usize,
}

/// Reader to scan rows in a partition.
///
/// It can merge rows from multiple shards.
pub struct PartitionReader {
    context: ReadPartitionContext,
    source: BoxedDataBatchSource,
}

impl PartitionReader {
    fn new(context: ReadPartitionContext, source: BoxedDataBatchSource) -> Result<Self> {
        let reader = Self { context, source };

        Ok(reader)
    }

    /// Returns true if the reader is valid.
    pub fn is_valid(&self) -> bool {
        self.source.is_valid()
    }

    /// Advances the reader.
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    pub fn next(&mut self) -> Result<()> {
        self.advance_source()
    }

    /// Converts current data batch into a [Batch].
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    pub fn convert_current_batch(&mut self) -> Result<Batch> {
        let start = Instant::now();
        let data_batch = self.source.current_data_batch();
        let batch = data_batch_to_batch(
            &self.context.metadata,
            &self.context.projection,
            self.source.current_key(),
            data_batch,
        )?;
        self.context.metrics.data_batch_to_batch += start.elapsed();
        Ok(batch)
    }

    pub(crate) fn into_context(self) -> ReadPartitionContext {
        self.context
    }

    fn advance_source(&mut self) -> Result<()> {
        let read_source = Instant::now();
        self.source.next()?;
        self.context.metrics.read_source += read_source.elapsed();
        Ok(())
    }
}

#[derive(Debug)]
pub struct SparsePrimaryKeyDecoder {
    codec: Arc<SparseEncoder>,
}

impl SparsePrimaryKeyDecoder {
    pub(crate) fn new(codec: Arc<SparseEncoder>) -> Self {
        Self { codec }
    }

    /// Returns the index of the column in the primary key if the column exists.
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
                let Some(field) = self.codec.fields.get(&column_id) else {
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
        let field = self.codec.fields.get(&column_id).unwrap();
        field
            .deserialize(&mut deserializer)
            .inspect_err(|e| common_telemetry::error!(e; "Failed to decode primary key, column_id: {:?}, pk: {:?}", column_id, pk))
    }
}

#[derive(Clone)]
pub(crate) struct SparsePrimaryKeyFilter {
    metadata: RegionMetadataRef,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    codec: Arc<SparsePrimaryKeyDecoder>,
    offsets_map: HashMap<u32, usize>,
}

impl SparsePrimaryKeyFilter {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: Arc<SparseEncoder>,
    ) -> Self {
        Self {
            metadata,
            filters,
            codec: Arc::new(SparsePrimaryKeyDecoder::new(codec)),
            offsets_map: HashMap::new(),
        }
    }

    pub(crate) fn prune_primary_key(&mut self, pk: &[u8]) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // no primary key, we simply return true.
        if self.metadata.primary_key.is_empty() {
            return true;
        }

        // evaluate filters against primary key values
        let mut result = true;
        self.offsets_map.clear();
        for filter in &*self.filters {
            if Partition::is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = self.metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            // ignore filters that are not referencing primary key columns
            if column.semantic_type != SemanticType::Tag {
                continue;
            }

            if let Some(offset) = self
                .codec
                .has_column(pk, &mut self.offsets_map, column.column_id)
            {
                let value = match self.codec.decode_value_at(pk, offset, column.column_id) {
                    Ok(v) => v,
                    Err(e) => {
                        common_telemetry::error!(e; "Failed to decode primary key");
                        return true;
                    }
                };

                let scalar_value = value
                    .try_to_scalar_value(&column.column_schema.data_type)
                    .unwrap();
                result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
            } else {
                let scalar_value = Value::Null
                    .try_to_scalar_value(&column.column_schema.data_type)
                    .unwrap();
                result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
            }
        }

        result
    }
}

#[derive(Clone)]
pub(crate) struct PrimaryKeyFilter {
    metadata: RegionMetadataRef,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    codec: Arc<McmpRowCodec>,
    offsets_buf: Vec<usize>,
}

impl PrimaryKeyFilter {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: Arc<McmpRowCodec>,
    ) -> Self {
        Self {
            metadata,
            filters,
            codec,
            offsets_buf: Vec::new(),
        }
    }

    pub(crate) fn prune_primary_key(&mut self, pk: &[u8]) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // no primary key, we simply return true.
        if self.metadata.primary_key.is_empty() {
            return true;
        }

        // evaluate filters against primary key values
        let mut result = true;
        self.offsets_buf.clear();
        for filter in &*self.filters {
            if Partition::is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = self.metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            // ignore filters that are not referencing primary key columns
            if column.semantic_type != SemanticType::Tag {
                continue;
            }
            // index of the column in primary keys.
            // Safety: A tag column is always in primary key.
            let index = self.metadata.primary_key_index(column.column_id).unwrap();
            let value = match self.codec.decode_value_at(pk, index, &mut self.offsets_buf) {
                Ok(v) => v,
                Err(e) => {
                    common_telemetry::error!(e; "Failed to decode primary key");
                    return true;
                }
            };

            // TODO(yingwen): `evaluate_scalar()` creates temporary arrays to compare scalars. We
            // can compare the bytes directly without allocation and matching types as we use
            // comparable encoding.
            // Safety: arrow schema and datatypes are constructed from the same source.
            let scalar_value = value
                .try_to_scalar_value(&column.column_schema.data_type)
                .unwrap();
            result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
        }

        result
    }
}

/// Structs to reuse across readers to avoid allocating for each reader.
pub(crate) struct ReadPartitionContext {
    metadata: RegionMetadataRef,
    row_codec: Arc<McmpRowCodec>,
    sparse_encoder: Arc<SparseEncoder>,
    projection: HashSet<ColumnId>,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    /// Buffer to store pk weights.
    pk_weights: Vec<u16>,
    need_prune_key: bool,
    metrics: PartitionReaderMetrics,
}

impl Drop for ReadPartitionContext {
    fn drop(&mut self) {
        let partition_read_source = self.metrics.read_source.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["partition_read_source"])
            .observe(partition_read_source);
        let partition_data_batch_to_batch = self.metrics.data_batch_to_batch.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["partition_data_batch_to_batch"])
            .observe(partition_data_batch_to_batch);

        common_telemetry::debug!(
            "TreeIter partitions metrics, \
            num_builder: {}, \
            num_shards: {}, \
            build_partition_reader: {}s, \
            partition_read_source: {}s, \
            partition_data_batch_to_batch: {}s",
            self.metrics.num_builder,
            self.metrics.num_shards,
            self.metrics.build_partition_reader.as_secs_f64(),
            partition_read_source,
            partition_data_batch_to_batch,
        );
    }
}

impl ReadPartitionContext {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        row_codec: Arc<McmpRowCodec>,
        sparse_encoder: Arc<SparseEncoder>,
        projection: HashSet<ColumnId>,
        filters: Vec<SimpleFilterEvaluator>,
    ) -> ReadPartitionContext {
        let need_prune_key = Self::need_prune_key(&metadata, &filters);
        ReadPartitionContext {
            metadata,
            row_codec,
            sparse_encoder,
            projection,
            filters: Arc::new(filters),
            pk_weights: Vec::new(),
            need_prune_key,
            metrics: Default::default(),
        }
    }

    /// Does filter contain predicate on primary key columns after pruning the
    /// partition column.
    fn need_prune_key(metadata: &RegionMetadataRef, filters: &[SimpleFilterEvaluator]) -> bool {
        for filter in filters {
            // We already pruned partitions before so we skip the partition column.
            if Partition::is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            if column.semantic_type != SemanticType::Tag {
                continue;
            }

            return true;
        }

        false
    }
}

// TODO(yingwen): Pushdown projection to shard readers.
/// Converts a [DataBatch] to a [Batch].
fn data_batch_to_batch(
    metadata: &RegionMetadataRef,
    projection: &HashSet<ColumnId>,
    key: Option<&[u8]>,
    data_batch: DataBatch,
) -> Result<Batch> {
    let record_batch = data_batch.slice_record_batch();
    let primary_key = key.map(|k| k.to_vec()).unwrap_or_default();
    let mut builder = BatchBuilder::new(primary_key);
    builder
        .timestamps_array(record_batch.column(1).clone())?
        .sequences_array(record_batch.column(2).clone())?
        .op_types_array(record_batch.column(3).clone())?;

    if record_batch.num_columns() <= 4 {
        // No fields.
        return builder.build();
    }

    // Iterate all field columns.
    for (array, field) in record_batch
        .columns()
        .iter()
        .zip(record_batch.schema().fields().iter())
        .skip(4)
    {
        // TODO(yingwen): Avoid finding column by name. We know the schema of a DataBatch.
        // Safety: metadata should contain all fields.
        let column_id = metadata.column_by_name(field.name()).unwrap().column_id;
        if !projection.contains(&column_id) {
            continue;
        }
        builder.push_field_array(column_id, array.clone())?;
    }

    builder.build()
}

/// Inner struct of the partition.
///
/// A key only exists in one shard.
struct Inner {
    metadata: RegionMetadataRef,
    /// Map to index pk to pk id.
    pk_to_pk_id: HashMap<Vec<u8>, PkId>,
    /// Shard whose dictionary is active.
    shard_builder: ShardBuilder,
    /// Shards with frozen dictionary.
    shards: Vec<Shard>,
    num_rows: usize,
    frozen: bool,
}

impl Inner {
    fn new(metadata: RegionMetadataRef, config: &PartitionTreeConfig) -> Self {
        let (shards, current_shard_id) = if metadata.primary_key.is_empty() {
            let data_parts = DataParts::new(metadata.clone(), DATA_INIT_CAP, config.dedup);
            (
                vec![Shard::new(
                    0,
                    None,
                    data_parts,
                    config.dedup,
                    config.data_freeze_threshold,
                )],
                1,
            )
        } else {
            (Vec::new(), 0)
        };
        let shard_builder = ShardBuilder::new(metadata.clone(), config, current_shard_id);
        Self {
            metadata,
            pk_to_pk_id: HashMap::new(),
            shard_builder,
            shards,
            num_rows: 0,
            frozen: false,
        }
    }

    fn find_key_in_shards(&self, primary_key: &[u8]) -> Option<PkId> {
        assert!(!self.frozen);
        self.pk_to_pk_id.get(primary_key).copied()
    }

    fn write_to_shard(&mut self, pk_id: PkId, key_value: &KeyValue) -> Result<()> {
        if pk_id.shard_id == self.shard_builder.current_shard_id() {
            self.shard_builder.write_with_pk_id(pk_id, key_value);
            return Ok(());
        }

        // Safety: We find the shard by shard id.
        let shard = self
            .shards
            .iter_mut()
            .find(|shard| shard.shard_id == pk_id.shard_id)
            .unwrap();
        shard.write_with_pk_id(pk_id, key_value)?;
        self.num_rows += 1;

        Ok(())
    }

    fn freeze_active_shard(&mut self) -> Result<()> {
        if let Some(shard) = self
            .shard_builder
            .finish(self.metadata.clone(), &mut self.pk_to_pk_id)?
        {
            self.shards.push(shard);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ahash::{HashMap, HashMapExt};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::ValueRef;
    use memcomparable::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    use crate::memtable::partition_tree::partition::SparsePrimaryKeyDecoder;
    use crate::memtable::partition_tree::tree::SparseEncoder;
    use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

    #[test]
    fn test_sparse_primary_key_decoder_0() {
        common_telemetry::init_default_ut_logging();
        let mut pk = vec![
            1u8, 0, 0, 37, 233, 1, 4, 158, 44, 2, 247, 159, 150, 172, 1, 1, 101, 107, 115, 45, 97,
            112, 45, 115, 9, 111, 117, 116, 104, 101, 97, 115, 116, 9, 45, 49, 45, 113, 97, 49, 0,
            0, 6, 1, 1, 113, 97, 0, 0, 0, 0, 0, 0, 2, 1, 1, 102, 97, 108, 115, 101, 0, 0, 0, 5, 1,
            1, 49, 48, 46, 48, 46, 51, 55, 46, 9, 50, 51, 56, 58, 50, 51, 50, 50, 9, 55, 0, 0, 0,
            0, 0, 0, 0, 1, 1, 1, 107, 117, 98, 101, 114, 110, 101, 116, 9, 101, 115, 45, 112, 111,
            100, 115, 0, 7, 1, 1, 106, 97, 114, 118, 105, 115, 0, 0, 6, 1, 1, 106, 97, 114, 118,
            105, 115, 45, 112, 9, 114, 111, 100, 117, 99, 101, 114, 45, 9, 99, 100, 55, 56, 54, 52,
            98, 99, 9, 102, 45, 106, 57, 52, 57, 100, 0, 7, 1, 1, 109, 111, 110, 105, 116, 111,
            114, 105, 9, 110, 103, 47, 112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45,
            107, 117, 9, 98, 101, 45, 112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45, 112,
            114, 9, 111, 109, 101, 116, 104, 101, 117, 115, 8, 1, 1, 112, 114, 111, 109, 101, 116,
            104, 101, 9, 117, 115, 45, 112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45,
            107, 117, 9, 98, 101, 45, 112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45, 112,
            114, 9, 111, 109, 101, 116, 104, 101, 117, 115, 9, 45, 48, 0, 0, 0, 0, 0, 0, 2, 1, 1,
            116, 97, 115, 107, 115, 0, 0, 0, 5, 1, 1, 100, 111, 105, 110, 103, 0, 0, 0, 5, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut deserializer = Deserializer::new(pk.as_slice());
        let table_id = Option::<u32>::deserialize(&mut deserializer).unwrap();
        println!("table_id: {:?}", table_id);
        let ts_id = Option::<u64>::deserialize(&mut deserializer).unwrap();
        println!("ts_id: {:?}", ts_id);
        while deserializer.has_remaining() {
            let label = Option::<String>::deserialize(&mut deserializer).unwrap();
            println!("label: {:?}", label);
        }
    }

    #[test]
    fn test_sparse_primary_key_decoder_1() {
        common_telemetry::init_default_ut_logging();
        let mut pk = vec![
            0, 0, 0, 11, 1, 1, 109, 111, 110, 105, 116, 111, 114, 105, 9, 110, 103, 47, 112, 114,
            111, 109, 101, 9, 116, 104, 101, 117, 115, 45, 107, 117, 9, 98, 101, 45, 112, 114, 111,
            109, 101, 9, 116, 104, 101, 117, 115, 45, 112, 114, 9, 111, 109, 101, 116, 104, 101,
            117, 115, 8, 0, 0, 0, 12, 1, 1, 112, 114, 111, 109, 101, 116, 104, 101, 9, 117, 115,
            45, 112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45, 107, 117, 9, 98, 101, 45,
            112, 114, 111, 109, 101, 9, 116, 104, 101, 117, 115, 45, 112, 114, 9, 111, 109, 101,
            116, 104, 101, 117, 115, 9, 45, 48, 0, 0, 0, 0, 0, 0, 2, 128, 0, 0, 4, 1, 0, 0, 49, 41,
            128, 0, 0, 3, 1, 22, 41, 133, 163, 194, 103, 179, 0, 0, 0, 0, 2, 1, 1, 101, 107, 115,
            45, 97, 112, 45, 115, 9, 111, 117, 116, 104, 101, 97, 115, 116, 9, 45, 49, 45, 113, 97,
            49, 0, 0, 6, 0, 0, 0, 3, 1, 1, 101, 116, 99, 100, 0, 0, 0, 0, 4, 0, 0, 0, 4, 1, 1, 99,
            108, 105, 101, 110, 116, 0, 0, 6, 0, 0, 0, 5, 1, 1, 113, 97, 0, 0, 0, 0, 0, 0, 2, 0, 0,
            0, 6, 1, 1, 102, 97, 108, 115, 101, 0, 0, 0, 5, 0, 0, 0, 7, 1, 1, 49, 48, 46, 48, 46,
            49, 48, 46, 9, 50, 52, 57, 58, 50, 51, 55, 57, 8, 0, 0, 0, 8, 1, 1, 101, 116, 99, 100,
            47, 101, 116, 99, 9, 100, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 9, 1, 1, 101, 116, 99, 100,
            0, 0, 0, 0, 4, 0, 0, 0, 10, 1, 1, 101, 116, 99, 100, 45, 49, 0, 0, 6,
        ];

        let codec = McmpRowCodec::new(vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::uint32_datatype()),
            SortField::new(ConcreteDataType::uint64_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::string_datatype()),
        ])
        .with_primary_keys(vec![
            11, 12, 2147483652, 2147483651, 2, 3, 4, 5, 6, 8, 9, 10, 7,
        ]);

        let values = codec.decode(&pk).unwrap();

        println!("{:?}", values);
    }

    #[test]
    fn test_sparse_primary_key_decoder() {
        common_telemetry::init_default_ut_logging();
        let mut pk = vec![];

        let mut fields = HashMap::with_capacity(2);
        fields.insert(
            2147483652,
            SortField::new(ConcreteDataType::uint32_datatype()),
        );
        fields.insert(
            2147483651,
            SortField::new(ConcreteDataType::uint64_datatype()),
        );
        let encoder = SparseEncoder {
            fields: fields.clone(),
        };

        let mut serializer = Serializer::new(&mut pk);
        2147483652_u32.serialize(&mut serializer).unwrap();
        let value = ValueRef::UInt32(2);
        fields
            .get(&2147483652)
            .unwrap()
            .serialize(&mut serializer, &value)
            .unwrap();

        2147483651_u32.serialize(&mut serializer).unwrap();
        let value = ValueRef::UInt64(3);
        fields
            .get(&2147483651)
            .unwrap()
            .serialize(&mut serializer, &value)
            .unwrap();

        println!("{:?}", pk);

        let decoder = SparsePrimaryKeyDecoder::new(Arc::new(encoder));
        let mut offsets_map = HashMap::new();
        let offset = decoder.has_column(&pk, &mut offsets_map, 2147483652);
        let value = decoder
            .decode_value_at(&pk, offset.unwrap(), 2147483652)
            .unwrap();
        println!("offset: {:?}", offset);
        println!("value: {:?}", value);
        let offset = decoder.has_column(&pk, &mut offsets_map, 2147483651);
        let value = decoder
            .decode_value_at(&pk, offset.unwrap(), 2147483651)
            .unwrap();
        println!("offset: {:?}", offset);
        println!("value: {:?}", value);
    }
}
