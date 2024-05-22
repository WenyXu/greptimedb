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

use std::fmt::Debug;
use std::sync::Arc;

use api::v1::WalEntry;
use common_error::ext::BoxedError;
use futures::stream::BoxStream;

use super::entry::Entry;
use crate::logstore::EntryId;
use crate::storage::RegionId;

/// A stream that yields triple of [EntryId] and [RegionId] and corresponding entry.
pub type EntryStream<'a, T> = BoxStream<'a, Result<(EntryId, RegionId, T), BoxedError>>;

// pub type EntryDecoder<DecodedEntry> = Arc<
//     dyn for<'a> Fn(
//             EntryId,
//             RegionId,
//             &'a [u8],
//         ) -> Result<(EntryId, RegionId, DecodedEntry), BoxedError>
//         + Send
//         + Sync,
// >;

pub type EntryDecoder<DecodedEntry> = Arc<
    dyn for<'a> Fn(&'a dyn Entry) -> Result<(EntryId, RegionId, DecodedEntry), BoxedError>
        + Send
        + Sync,
>;

/// The default Reader.
pub type ReaderRef = Arc<dyn Reader<DecodedEntry = WalEntry>>;

/// The context of reader.
pub enum ReaderCtx {
    RaftEngine(RegionId),
    Kafka(String),
}

pub trait Reader: Send + Sync + Debug {
    type DecodedEntry;

    fn read(
        &self,
        ctx: ReaderCtx,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, Self::DecodedEntry>, BoxedError>;
}
