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

use crate::region_write_ctx::RegionWriteCtx;
use crate::request::{OptionOutputTx, SenderReplicationRequest};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn handle_replication(
        &mut self,
        SenderReplicationRequest {
            region_id,
            mut sender,
            entry_id,
            wal_entry,
        }: SenderReplicationRequest,
    ) {
        let Some(region) = self.regions.readonly_region_or(region_id, &mut sender) else {
            return;
        };

        let mut region_write_ctx = RegionWriteCtx::new(
            region.region_id,
            &region.version_control,
            region.provider.clone(),
        );
        let mut rows_replayed = 0;

        for mutation in wal_entry.mutations {
            rows_replayed += mutation
                .rows
                .as_ref()
                .map(|rows| rows.rows.len())
                .unwrap_or(0);
            region_write_ctx.push_mutation(mutation.op_type, mutation.rows, OptionOutputTx::none());
        }

        // set next_entry_id and write to memtable.
        region_write_ctx.set_next_entry_id(entry_id + 1);
        region_write_ctx.write_memtable();
        sender.send(Ok(rows_replayed))
    }
}