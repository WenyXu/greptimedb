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
use common_wal::options::WalOptions;
use futures::stream::BoxStream;

use crate::logstore::reader::ReaderRef;
use crate::logstore::EntryId;
use crate::storage::RegionId;

/// A stream that yields tuple of [EntryId] and corresponding entry.
pub type EntryStream<'a, T> = BoxStream<'a, Result<(EntryId, T), BoxedError>>;

pub type EntryDistributorRef = Arc<dyn EntryDistributor<DecodedEntry = WalEntry>>;

pub trait EntryDistributor: Send + Sync + Debug {
    type DecodedEntry;

    fn subscribe(
        &self,
        reader: ReaderRef,
        region_id: RegionId,
        options: &WalOptions,
        start_id: EntryId,
    ) -> Result<EntryStream<'static, Self::DecodedEntry>, BoxedError>;
}
