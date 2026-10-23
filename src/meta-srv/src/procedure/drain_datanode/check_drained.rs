// Copyright 2026 Greptime Team
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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::{
    Context, MAX_RESCAN_ROUNDS, PersistentContext, State, utils,
};

/// Rescans the datanode and decides whether the drain is finished.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckDrained;

#[async_trait::async_trait]
#[typetag::serde]
impl State for CheckDrained {
    async fn next(
        &mut self,
        ctx: &mut Context,
        persistent_ctx: &mut PersistentContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = persistent_ctx.node_id;
        let plan = utils::build_datanode_plan(&ctx.table_metadata_manager, node_id).await?;
        persistent_ctx.follower_region_count = plan.follower_region_count;

        // Regions given up by `DrainDatanodeStrategy::TryAll` are still led by the datanode;
        // they must not trigger another rescan round.
        let remaining_leader_regions: Vec<_> = plan
            .leader_regions
            .iter()
            .copied()
            .filter(|region_id| !persistent_ctx.failed_regions.contains(region_id))
            .collect();

        if remaining_leader_regions.is_empty() {
            info!(
                "Drained datanode {}, migrated regions: {}, follower regions: {}",
                node_id, persistent_ctx.migrated, persistent_ctx.follower_region_count
            );
            return Ok((Box::new(DrainDatanodeEnd), Status::executing(true)));
        }

        if persistent_ctx.rescan_round >= MAX_RESCAN_ROUNDS {
            return error::DrainDatanodeRescanExceededSnafu {
                node_id,
                max_rounds: MAX_RESCAN_ROUNDS,
            }
            .fail();
        }

        info!(
            "Datanode {} still leads {} regions, rescan round: {}",
            node_id,
            remaining_leader_regions.len(),
            persistent_ctx.rescan_round + 1
        );
        persistent_ctx.leader_regions = remaining_leader_regions;
        persistent_ctx.rescan_round += 1;

        Ok((Box::new(Dispatch), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
