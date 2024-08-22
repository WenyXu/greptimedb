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

use api::v1::{Mutation, OpType, WalEntry};
use store_api::logstore::provider::Provider;
use store_api::logstore::LogStore;
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::Result;
use crate::manifest::action::RegionMetaActionList;
use crate::region::version::{VersionControlData, VersionControlRef};
use crate::wal::{EntryId, WalWriter};

pub(crate) struct ManifestActionRecorder {
    /// Id of region to write.
    region_id: RegionId,
    /// VersionControl of the region.
    version_control: VersionControlRef,
    /// Next sequence number to write.
    ///
    /// The context assigns a unique sequence number for each row.
    next_sequence: SequenceNumber,
    /// Next entry id of WAL to write.
    next_entry_id: EntryId,
    /// Valid WAL entry to write.
    ///
    /// We keep [WalEntry] instead of mutations to avoid taking mutations
    /// out of the context to construct the wal entry when we write to the wal.
    wal_entry: WalEntry,
    manifest_action_list: Vec<(ManifestVersion, RegionMetaActionList)>,

    /// Wal options of the region being written to.
    provider: Provider,
}

impl ManifestActionRecorder {
    pub(crate) fn new(
        region_id: RegionId,
        version_control: &VersionControlRef,
        provider: Provider,
    ) -> ManifestActionRecorder {
        let VersionControlData {
            committed_sequence,
            last_entry_id,
            ..
        } = version_control.current();

        ManifestActionRecorder {
            region_id,
            version_control: version_control.clone(),
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            manifest_action_list: vec![],
            provider,
        }
    }

    pub(crate) fn push_action_list(
        &mut self,
        next_version: ManifestVersion,
        action_list: RegionMetaActionList,
    ) -> Result<()> {
        self.wal_entry.mutations.push(Mutation {
            op_type: OpType::Manifest.into(),
            sequence: self.next_entry_id,
            rows: None,
            action_list: Some(api::v1::ManifestActionList {
                manifest_version: next_version,
                data: Some(api::v1::manifest_action_list::Data::Json(
                    action_list.encode_str()?,
                )),
            }),
        });
        self.manifest_action_list.push((next_version, action_list));
        self.next_sequence += 1;

        Ok(())
    }

    pub(crate) fn add_wal_entry<S: LogStore>(
        &mut self,
        wal_writer: &mut WalWriter<S>,
    ) -> Result<()> {
        wal_writer.add_entry(
            self.region_id,
            self.next_entry_id,
            &self.wal_entry,
            &self.provider,
        )?;
        self.next_entry_id += 1;
        Ok(())
    }

    pub(crate) fn finish(&mut self) {
        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_sequence - 1);
    }
}
